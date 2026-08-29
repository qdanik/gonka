# Rules

Three kinds of rule govern this gateway, and they answer three different questions:

- **Invariants** — what must never stop being true. Break one and the gateway still runs; it just loses money or lies.
- **Non-goals** — what it deliberately does not do, so "is this missing or on purpose?" is answerable without reading code.
- **Verification limits** — what a green test suite does *not* prove.

Most of the invariants were learned the hard way, several were violated during the rewrite itself, and the best ones are enforced by a type or a lock rather than by discipline.

## Part 1 — Invariants

### 1. A committed nonce is always settled

Advancing the nonce sequence sends a `MsgStart`. From that moment the inference is owed a resolution: either the host finishes it and the session records the finish, or the gateway posts a timeout vote. A committed nonce with neither is an orphaned chain message settlement can never resolve — money the escrow can never reclaim.

Every committed nonce ends in exactly one of three states:

| State | Who resolves it |
| --- | --- |
| Served | an attempt ran; the response was applied, or a timeout vote was posted |
| Ghosted | the scheduler burned it locally with a recorded reason; nothing was sent, so nothing is owed |
| Stranded but voted | the assignment could not be dispatched; a synthetic outcome carries it into the timeout plan |

What enforces it:

- **The decision type is closed.** `scheduler.Decision` is a sum of `serve`, `burn`, `hold` and `decline` (`scheduler/decision.go`). There is no way to express "committed and nobody's" without changing the type.
- **Terminal events block.** `AttemptDone` uses a blocking send while progress events are dropped when the coordinator is busy (`engine/attempt.go`, `emit` vs `offer`). Making the terminal non-blocking would compile, pass most tests, and quietly break this invariant.
- **An undispatchable assignment is stranded, not dropped.** `strand` releases the host slot, keeps the escrow hold, and appends a synthetic attempt with `TerminalNoReceipt` (`engine/race.go`).
- **A race that fails before its first attempt still reports** (`engine/race.go`, `fail`).
- **`Engine.Stop` is a real barrier.** A race is registered under the engine mutex *before* it starts and released only after its vote goroutine finishes (`engine/engine.go`, `admit` and `raceRegistration.release`). Registering later would leave the just-admitted race — precisely the one still owing a vote — outside the barrier.
- **`Run` recovers a panic solely to release that registration, then re-panics with the same value.** Without it a panic between admission and settlement leaves `Stop` waiting forever.

**This shape broke seven times**, and every one was a variation of *state moved between the commit and the lookup meant to resolve it*: admission peeked instead of acquiring atomically; a missing dispatch target returned early after the commit; a dead declined-admission path; an escrow retired between race end and settle became unresolvable; an in-flight counter with no production caller left every escrow reading idle; a reply applied only on the success path stranded nonces that failed *beside* a reply; and a re-added escrow id stole the settlement lookup from the entry still draining.

### 2. Exactly one outcome and exactly one winner per race, on every path

A race runs several attempts, may be abandoned mid-flight, and continues after its caller returns. Every exit path still produces exactly one `RaceOutcome` and crowns at most one winner.

**One winner.** `c.winner` is assigned in `answer` and `settleClaims`, both guarded by `c.winner == nil`, both on the coordinator goroutine. Attempts do not decide — they *claim*: a writer with content sends a crown request and blocks on the reply (`engine/stream.go`, `winnerWriter.claim`), so no byte reaches the client before the coordinator has answered. A loser's `Abandon` clears its client field, so a suppressed attempt has no reachable sink at all rather than a sink behind a branch someone must remember to write.

**One outcome.** `report()` closes the race's `done` channel before building the outcome. A second call panics on the double close — a duplicated outcome takes the process down at the point of the defect instead of passing silently.

**One recording point.** The exemption ladder deciding whether a host is judged is applied in `Engine.record` and nowhere else. The legacy gateway had six divergent entry points for the same decision.

### 3. No field crosses a goroutine except through the event channel

The legacy engine shared a 57-field struct across three goroutines on the money path: the drain returned while attempts were still writing the fields the outcome then read. That is a data race deciding what gets paid.

Here each attempt owns its state and publishes facts as events. `attemptState` is never read by another goroutine; `liveAttempt` is written only in response to events, on the coordinator; the byte path runs only on the attempt's goroutine. There are exactly two cross-goroutine channels — the event channel and the crown handshake — plus a close-only `done`.

**`select` chooses at random among ready cases, and that forced a specific shape.** When a buffered event and a fired timer are both ready, the coordinator may take the timer and act on state the queued event has already invalidated: spending a nonce on an escalation newer state has cleared, mislabelling a healthy host, reporting a completed winner as cancelled. So every select arm that *reads* race state first drains the event queue (`engine/race.go`, `catchUp`). This appeared three times before it was fixed structurally, and once as a live flake that passed at `-count=3` then failed with the wrong terminal state.

Precedence is therefore explicit, never inherited from arm order: `nextDeadline` takes the earliest deadline outright, and the declaration order of the trigger constants breaks exact ties (`engine/deadline.go`).

### 4. Routing and settlement read the escrow set asymmetrically, on purpose

```
Routing      → published entries only
Settlement   → published, then draining
Status reads → published or draining, without counting
```

Routing must refuse a retired escrow. The nonces it already committed still owe votes, and the draining entry holds the storage those votes settle through. Unifying the two lookups — the obvious tidy-up — either dispatches into an escrow that is going away or strands the votes of one that already did.

The narrow interfaces make the mistake hard to express: an attempt to reproduce the settlement bug at the API layer by swapping one lookup for the other did not compile, because that layer declares only the settlement lookup. The read-only status handle is a third case precisely because it must **not** take a hold — an operator inspecting an escrow must not keep it from retiring.

### 5. The slot and the escrow hold are taken with the nonce, and given back after the vote

Three resources move together: the nonce, the participant's concurrency slot, and the escrow's in-flight hold.

**Acquisition is one atomic step**, inside the callback of `session.Advance`, under the session's own lock with the nonce-to-host binding already fixed (`scheduler/dispatcher.go`). A refused slot becomes `burn{ghostThrottled}`.

**Release is by whoever spends it.** The engine's view of the limiter exposes only `Release` — an interface that cannot acquire cannot get the ownership wrong. Every scheduler path that cannot deliver an assignment gives the slot and the hold back *together*.

**The hold outlives the race**, released only after the settlement vote is posted, from inside the goroutine that posts it. The exception is the stranded case, where the assignment's own hold is kept, because the escrow being retired is exactly why there was no target and the vote still has to reach it.

**Why the peek still exists.** `limits.Available` remains a cheap pre-filter for routing, where a stale answer costs nothing. What broke was using it as the *authority*. Filter versus authority is the whole lesson.

### 6. Shutdown order is a contract

Nine steps (`lifecycle.go`, `shutdownOrder` and `stopAll`), listed in [operations.md](./operations.md). The order encodes three dependencies: races drain to the vote that settles their nonces, and that vote needs the escrow sessions, the observer and the chain client alive — so races stop second and everything they depend on stops below them. The store is second to last because closing it drains the accounting ledger. Chain connections close last because every step above can still reach the chain.

Three runner properties are as load-bearing as the order:

- **Every step runs even after a failure**, because the store must be reached whatever happened above it.
- **The drain is bounded but not cancelled.** Cancelling aborts the vote the drain exists to protect; waiting forever forfeits every later step to the SIGKILL that follows. An overrunning drain is left running and reported (`lifecycle.go`, `waitFor`).
- **One step is guarded by quiescence.** A step marked `needsQuiesced` is skipped, and the skip reported, whenever anything above it failed — today only the escrow sessions. Closing a session takes no lock, so closing one under an overrun drain races a nonce commit, trading a vote already going to be dropped for on-disk state nothing can repair.

Got wrong twice: an earlier order omitted the dispatchers and the registry entirely, closing the gateway store while every escrow session still held a handle; and an earlier `waitFor` discarded its context, so the grace period reached only the listener — one unanswered request plus SIGTERM blocked the drain for minutes and the orchestrator killed the process mid-vote.

### 7. Money-path orderings

Each is an ordering whose reversal produces a plausible-looking program that loses funds.

| Rule | Enforced at | Cost of reversal |
| --- | --- | --- |
| Durable intent is written before the broadcast; a failed write aborts before broadcasting | `chain/txclient.go`; `escrow/commitments.go`, `createEscrow` | a crash between broadcast and record leaves a funded escrow nobody knows about |
| The precomputed tx hash must equal the one the node returns | `chain/txclient.go` | the recovery record points at a transaction that is not the one that landed |
| The commitment row is deleted only after the escrow is registered | `escrow/commitments.go`, `persistEscrow` | a crash in between loses the only pointer to a funded escrow |
| A transaction that may still land is never dropped from durable intent | `escrow/commitments.go`, `commitmentReconcileGrace` | an escrow created but not yet indexed is forgotten |
| An escrow is deactivated only on confirmed absence; any endpoint error keeps it active | `escrow/checker.go`, `TriggerEscrowCheck` | a network blip deactivates a live, funded escrow |
| Park before the settlement reconciliation, not after | `escrow/settlement.go`, `settle` | the row is deleted while the escrow is still routable, and nothing can un-publish it |
| `settlement_pending` is cleared only after the broadcast is confirmed | `escrow/settlement.go`, `settle` | a settlement that failed looks completed |
| The row is deleted only after settlement succeeds; with settlement off the escrow is parked, never deleted | `escrow/settlement.go`, `retire` | the row names the env variable of the settling key — delete it and the funds are unrecoverable |
| A replacement escrow is created before the depleted one is retired | `escrow/depletion.go`, `replaceDepleted` | coverage drops to zero between the two |
| A zero-row UPDATE or DELETE is an error, never a silent success | `store/devshards.go`, `requireOneRow` | state diverges from what the caller believes it wrote |
| Private keys are never persisted — only the name of the variable holding one | `store/devshards.go`; `api/errors.go`, `ErrPrivateKeyEnvRequired` | a key reaches the row, the logs, and every operator in between |

### 8. Fail-closed and fail-open are chosen per signal

Neither default is right everywhere. What matters is that each is decided rather than inherited.

**Fail closed** — refuse rather than guess:

- A model absent from a *populated* per-model weight view gets zero capacity rather than inheriting the all-model view (`limits/capacity.go`). Model strings are client input; without this an unknown model inherited full network capacity.
- A NaN or out-of-range scale clamps to **zero**, never to one (`limits/gateway.go`, `clampUnit`). The natural-looking alternative — returning an empty weight map — would have hit the "no baseline means unlimited" branch and granted *full* capacity.
- An empty escrow registry means not ready, not "everything routes": 503.
- A host's node-version capability is unknown until proven, and a stale entry counts as unknown.
- Admin routes do not exist without a configured key — 404, not 401 — and a key must be at least 16 characters.

**Fail open** — carry on rather than break a working system:

- A failed chain poll republishes the previous snapshot with `LastError` set rather than an empty one.
- `MaxNonce == 0` means "not fetched", not "no limit": the scheduler falls back to `fallbackNonceCeiling` (19 800) rather than disabling the cap.
- A nil preserved set means "not loaded yet", so everyone counts as preserved. Reading absent data as "nobody is preserved" would ghost every nonce the escrow owns.
- With no weights reported at all, escrow scoring falls back to availability-filtered membership share instead of zero, which would reject every request during the boot window. Because that fallback serves requests correctly and *silently*, it publishes a gauge derived from the same two predicates the branch reads, so the signal cannot drift from the behaviour it reports.

### 9. Bounded by construction

Loops that spend money or memory per iteration carry an explicit bound, not a hope:

| Loop | Bound |
| --- | --- |
| the scheduler's drain | predicates frozen for the drain, a burn budget of `groupSize × (waiters + 1)` fixed at entry |
| SSE reassembly | three budgets — per attempt, per participant, process-wide; the participant level stops one host that never terminates an event from starving every other host of classification |
| the accounting ledger | rejects a zero on either retention axis, and sheds rows rather than blocking the response path |
| settlement of parked escrows | `pendingSettleBudget` = 4 per tick |
| version polling | 16 ways, 2 s per fetch — a sequential pass over hundreds of miners exceeds the freshness window, which silently makes every node report as not validation-capable |
| boot | escrow runtimes built under a semaphore whose depth is also the idle-connection pool size |

Three maps deliberately never evict, and each has a reason eviction would break:

- **Crown strikes** — the entry *is* the running count of contentless answers, and only an answer that carried content clears it; a dial failure or a stranded nonce says nothing about the host, and evicting on one hands it a clean record it did not earn.
- **Reassembly charges** — held by pointer for the life of the attempt, so dropping the entry mints a second counter while the first still carries live charges, and the participant's cap then bounds nothing.
- **The per-escrow opening lock** — removing a lock while a caller waits on it lets the next caller take a fresh one and open the same SQLite file concurrently, which is the failure the lock exists to prevent.

Both of the first two are keyed by participant, and the participant set is the bounded validator set.

### 10. Labels, ordering and determinism

- Metric label **values** are exported by the package that emits them and referenced by the metrics layer, never restated. Renaming the constant breaks the build rather than the dashboard — but editing its *value* still compiles and moves the wire string under every panel and alert, so those constants are a contract with Grafana, not an internal vocabulary.
- A route label is always a route pattern, never a raw path. Three of the legacy gateway's eight branches emitted unbounded strings, including a default that returned the path itself on unauthenticated traffic — one series per probe.
- **Escrow id is safe on gauges and expensive on counters.** Escrow ids are monotonic and never reused, so a counter keyed by one grows with uptime and no scrape retires it. Exactly three counter families carry it — ghost burns, nonce holds, burn budget — because a spent nonce must be attributable to the escrow that paid for it, and the scheduler deletes all three when it reaps that escrow's dispatcher.
- Iteration with an observable effect is over **sorted** keys: membership publication, the escrow-missing drain, the registry's snapshot and close paths.
- Request capture admits a deterministic **stride**, not a random sample, so a configured rate is honoured exactly and reproducibly.

## Part 2 — What it deliberately does not do

### Dropped from the legacy gateway

| Dropped | Why, and what replaces it |
| --- | --- |
| **Single-escrow mode** and the ten top-level routes that existed only for it | one escrow is now a pool of one; everything is reachable at `/devshard/{id}/…`. Operator scripts using bare paths need the escrow id added |
| **The hand-written OpenAPI document and Swagger UI** | both had drifted, neither was tested, and the UI pulled assets from a CDN. Shipping a stale hand-written document is worse than shipping none |
| **`/debug/pprof/*`** | legacy mounted it on the same mux as public traffic, where one unauthenticated request can stall the process. Adding it back belongs with a decision about where it is exposed |
| **Short-content response capture** | capture has two triggers only: a request the filters rejected, and one every attempt failed. What answered the same question is now counted rather than kept — crown strikes, `no_winner_attempts_total`, the per-request record |
| **A configurable host route prefix** | derived from the binary's own version and not overridable (`main.go`, `ResolveRoutePrefix`). The prefix names the protocol version sessions are created with and settlements carry; a knob that sets them apart is a way to build a settlement the chain will not take |
| **The quarantine state machine** (`probe`/`shadow`/`probation`, 30–60 minute sentences) | replaced by the AIMD window plus a circuit breaker whose worst case is minutes: adaptation instead of punishment. The operator escape hatch survives as `POST /v1/admin/participants/unquarantine` |
| **The pairwise speed comparator and its 500 ms winner hold** | pairwise routing is gone, so there is no preference signal to hold for — and inline in the writer the hold stalled the eventual winner's own socket while buying nothing |
| **Host-index-keyed performance metrics** | `host_idx` is not an identity any more; the four families duplicated participant-keyed twins from the same call site |
| **`devshard_runtime_reserved_tokens`** | token reservation moved into the limiter; the legacy load formula that consumed it was already documented as misleading |
| **Probe attempts inside the race engine** | unreachable: a nonce that cannot be served is burned inside the scheduler and never becomes an attempt. Every probe field and its seven guards were deleted rather than left unset; the concept survives as ghost burns |
| **One of the three escalation rules** | rule 3 (switch to a measurably faster secondary) needed a latency model this gateway does not keep, and a faster host is not a reason to abandon an answer already streaming |
| **Persisted participant health** | AIMD windows, breaker state and decayed counts all start empty. Minute-scale backoff self-heals faster than replaying stale penalties is worth. The cost is honest and small: a genuinely bad host gets one free window after every deploy |
| **The legacy `state.db` migration** | state starts fresh, bootstrapped from `GATEWAY_ESCROWS_JSON` and the admin import endpoint |
| **The `capacity_aware_limits` toggle** | it was read and never used. Making it real would mean *adding* a path that disables capacity scaling — new behaviour, not a restored one |

The metric families that described the deleted quarantine machinery went with it and have no successor, so a binary swap does not leave the in-repo Grafana dashboard working: it needs an edit either way.

### Refused mechanisms

**No panic-recovery middleware.** `net/http` already recovers a per-connection panic, and a recovery wrapper would be free to write a JSON error object into a half-written SSE stream. The one hazard recovery would genuinely fix — a panic between a race registering itself and releasing that registration — is fixed inside the engine by a deferred recover that releases through an idempotent token and re-panics with the same value.

**No per-request cost accounting.** The request ledger records tokens and topology, not money. Legacy had no cost column either, and computed its breakdown as a read-time join against live session state, so it printed zeros once an escrow settled. Money lives in chain settlement; the ledger answers "what did this request do", not "what did it cost".

**No KV-cache affinity routing.** `scheduler.AffinityHint` is an empty struct with no caller — the extension point exists so adding affinity later is a local change inside the scheduler rather than a fourth selection mechanism bolted beside the three legacy had.

**No data-driven first-token escalation.** The escalation curve is a hardcoded quadratic. An earlier design fed it a measured percentile; that reader is gone and no half-built seam dangles toward it. First-content timings are still observed, but only as a histogram with no in-process reader.

**No caching during proof-of-compute.** The cache is probed *after* the admission gate, so while requests are blocked the gateway serves nothing rather than serving hits. Legacy probed first. The admission gate's contract is that it rejects a request before it can take a cache lookup, a limiter slot or a token budget — probing first would make that claim false.

### Boundaries it declines to cross

- **The engine never acts on escrow lifecycle facts; it reports them.** An escrow a host reported missing is recorded on the outcome and handed to the escrow manager, which verifies against the chain and decides. That is what keeps `engine` from importing `escrow`.
- **An exhausted deposit is reported by the scheduler, not the engine.** A request whose pick failed may never have been assigned an escrow to name; the dispatcher is the one seam holding the failure and the escrow id at the same moment.
- **The gateway does not enforce the economics.** It caps `max_tokens`, so the reservation is bounded by what was asked for, and the chain clamps actual cost to that reservation. Within it a host can still over-claim; the defences are on-chain response-hash validation and the permanent state-root-divergence block.
- **The gateway does not restate chain policy locally.** The maximum active nonce count is a governance parameter. A failed read falls back to a conservative constant; it never invents a policy of its own.
- **No changes to the shared `devshard/` packages.** Where one has a defect the gateway must live with, the gateway normalises at the boundary. The standing example: the shared timeout handler returns a non-nil error on its own success path, so only its genuine failures wrap a cause — a reported reason beside an unwrapped error is a vote that reached the chain, and `SettleTimeout` translates exactly that pair into success (`api/timeouts.go`). Reading the raw error instead would record every posted vote as a failed one.

### Known gaps, stated rather than hidden

- Two per-escrow recovery tools legacy had are not restored: `signatures/collect` and `sync-hosts`. The read-only half of the recovery surface **is** served, and deliberately resolves through the settlement lookup so a draining escrow still answers — which is the point, since the escrow needing inspection is usually the one in trouble.
- **`/v1/debug/perf` is not served.** The request ledger at `/v1/requests/{id}` answers the same question per request, and the accounting findings now carry the thresholds a host's numbers are judged against. The pairwise summaries legacy's version carried describe a mechanism this gateway no longer has.
- The in-repo Grafana dashboard queries families this gateway does not emit, and the repository defines no alerting rules at all. Neither is a code gap; both mean a binary swap leaves panels blank.
- Three residuals recorded rather than papered over: a ghost burn commits its nonce without taking the escrow's in-flight hold, so it is not protected against a concurrent retire the way a served commit is; the per-participant limiter's state map never evicts, unlike the performance tracker's; and a per-participant decay half-life is captured when a host is first seen, so changing it at run time reaches only hosts seen afterwards.

## Part 3 — What the tests do not verify

The end-to-end suite (`e2e_test.go`) composes the real gateway — engine, scheduler, registry, limits, store — with real devshard sessions talking to real in-process hosts: real diffs, real state roots, real signatures, a real `MsgFinishInference`, no network and no chain.

This section exists because reading that suite as green and concluding "verified" would be wrong. These are not gaps to be closed by writing more of the same suite; they are outside what any in-process test can reach.

**That the real chain accepts the gateway's transactions.** Every chain response in the suite is written by the suite. Protobuf field numbers, unordered-transaction TTL semantics, gas, fee denomination, sequence handling under contention and the escrow-created event shape are checked against the suite's *model* of the chain. One wrong field number is a green suite and a rejected transaction. This is the largest irreducible risk, and nothing in the suite touches it.

**That real hosts behave like these.** The hosts are real state machines, but their inference engine is a stub. A vLLM version emitting a new SSE field, a valid receipt followed by a stream truncated at an unscripted byte boundary, TCP behaviour under packet loss — all unrepresented. The non-streaming reply is SSE-shaped because the in-process client writes SSE either way, so non-streaming *body* bytes are not evidence about a real host.

**Any tuning threshold.** Peak-EWMA, the hedging trigger, outlier ejection and the AIMD window all take a latency distribution as input. "Given these numbers, this decision" is asserted in the owning packages; "these thresholds are right for the fleet" is unanswerable without production traffic.

**Real concurrency at real scale.** The widest race in the suite is single digits. One scale hazard is known and needs load to observe: `ProcessResponse` serialising a wide race on one session mutex.

**Settlement money end to end.** The suite asserts the payload the gateway builds. Whether the chain then pays the right participants the right amounts is chain-side.

**Byte-for-byte compatibility with real clients.** The frames are pinned in the suite. Whether an OpenAI-SDK or aiohttp client in the field parses them as it parsed the legacy gateway's is an assumption, not a result.

**In one sentence:** the suite verifies every decision the gateway makes given a set of inputs, and the wiring that makes those decisions reachable — not that its model of the chain or of a real host is correct, and not any tuning.
