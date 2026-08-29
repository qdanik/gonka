# `scheduler` — which escrow, which host, which nonce

A chat request needs a nonce, and a nonce is bound to a host by `nonce % groupSize`. This package decides which escrow to spend from, and what to do when the nonce it is handed belongs to a host that cannot serve.

## What it owns

- **Escrow choice** (`scheduler.go`, `escrow_pick.go`) — a load score over the live escrows, scaled by the chain's own host weights.
- **One actor per escrow** (`dispatcher.go`, `dispatcher_queue.go`) — the only goroutine that advances that escrow's nonce stream, so two requests can never take the same nonce.
- **The match decision** (`match.go`, `decision.go`) — for the nonce just offered, either serve a waiting request, hold it briefly for a compatible one, burn it, or decline.
- **Burns** (`ghost.go`) — a nonce that will serve nobody, named by the reason it was burned, because an operator reads the reason to decide what to do.
- **Divergence blocks** (`divergence.go`) — a host whose post-state-root disagrees with the escrow gets one catch-up replay; after that it is blocked for that escrow.

## What it does not own

It does not dispatch. It hands out an assignment and the [`engine`](../engine/) sends the request. It does not decide whether a burn is charged to the host — that is [`burns`](../burns/).

## Boundaries worth knowing

- **Five host gates, all of which waiting can clear**: excluded, proof-of-compute-required, throttled, ejected, state-blocked. A capability refusal is counted, never routed on.
- **Predicates are frozen for the whole drain.** Reading them live lets a host look usable to the sweep that kept a waiter and unusable to the binding that would serve it — which burns a nonce every turn, forever.
- **The divergence block and the spent replay outlive the escrow's actor.** Reaping an idle dispatcher is idleness, not resolution.
- **A burn decided before the session could commit has no nonce to name**, and is reported without one.

## Picking an escrow

A request that names no escrow is scored across the candidates for its model; a request that pins one by id skips the score but is still checked against the same exhaustion gates, because the nonce ceiling is what reserves room for the finalize and settlement that follow.

- **Load score** is the ascending utilisation ratio `ActiveUsers / EscrowWeight`. A non-positive or NaN weight scores unusable rather than best.
- **The allowlist is read here as well as at dispatch.** An escrow whose whole group the allowlist refuses can never serve, and picking it by load alone spends the request on a group holding nobody. When every candidate is unreachable that way the caller gets `ErrAllowlistUnreachable`, because waiting cannot fix it. An empty allowlist skips the walk entirely: the narrowing exists only where an operator asked for it.
- **Ties hold indices, not candidates.** An index does not escape the way a returned `Escrow` does, so the common case picks without touching the heap. The tie-break is a single counter shared across every model and tie-set shape.
- **The nonce cap** uses `fallbackNonceCeiling` — the fixed ceiling the gateway ran on before the parameter existed — until governance `max_nonce` has been fetched. The fetched value is clamped to `MaxUint32`, never allowed to wrap: a cap wrapping to 0 makes `MaxActiveNonce` return `^uint64(0)` and disables the gate.
- **The balance floor** prices the reserve the way the chain does, `(input + max_tokens) × token_price`, and asks whether the escrow still covers everything in flight plus this arrival. Every multiplication is overflow-checked, because a price this escrow cannot afford must not read as an affordable small one.
- **Exhaustion is reported, not just acted on.** Routing only declines an exhausted escrow; replacing it belongs to the rotation lifecycle, which otherwise never learns and lets the escrow drain silently into `ErrNoEscrowCapacity`. An escrow the balance floor caught while it could still refuse cleanly is reported as the running dry it is, wrapping `ErrInsufficientBalance`, rather than as a model nobody serves.

See routing.md, "Picking an escrow".

## The drain

One pass of `drain` assigns nonces until the queue empties, a nonce is held, or the burn budget trips. The freeze and the budget are what bound its cost in nonces; the budget is `groupSize × (waiting + 1)`. It returns with a waiter still queued **only** when it reports a held nonce — the one exit the loop arms a timer for — so nothing is ever parked unwoken.

The host predicates are frozen once per drain by memoising them. `admit` couples admission to that freeze: a participant whose concurrency window refused a slot counts as throttled for the rest of the drain. A predicate nobody set stays nil rather than being wrapped, so a reader can tell "no allowlist" from "an allowlist that refuses everybody".

`sweepExhausted` runs before every binding and drops the waiters no available participant can serve, instantly and without touching the nonce — a busy or chain-blocked pool answers `ErrHostsBusy`, anything else `ErrNoAvailableHost`. An excluded host is not a dead end there: past the stale window `match` spends the nonce on it rather than burning, and dropping the waiter would fail the one request that rescue exists for. `servable` and `match` read the same `blocks` definition and must be exactly as strict as each other: a `servable` that is stricter fails a request `match` would have served, and one that is laxer keeps a waiter queued that every drain can only answer by burning a chain-costed nonce.

### Where the nonce, the slot and the hold are taken

The concurrency slot and the escrow's in-flight hold are taken inside the same `Advance` that commits the nonce, and are given back together or not at all. The hold is taken on the serve path only, so a ghost commits unprotected against a concurrent retire; the slot travels with the assignment, so the release path covers only what never reaches a dispatch. A failed `Advance` answers the whole queue, not just the waiter a serve decision chose — the session could not advance its nonce at all — and a spent deposit is terminal for the escrow rather than for the request, so only the exhaustion notice gets it replaced. An assignment the waiter can no longer accept is given back and burned as abandoned.

See routing.md, "Where the nonce, the slot and the hold are taken".

## The match decision

`match` is pure and total: it reads nothing, mutates nothing, and every path yields exactly one of serve, burn, hold, or decline (rules.md, "1. A committed nonce is always settled"). Filters are keyed by participant, never by slot, so a host a request excluded once can never be re-served to it through a sibling slot of the same validator.

- **hold** declines the bound nonce without committing it, giving a co-arriving compatible waiter a chance before the nonce is burned.
- **decline** gives the nonce back uncommitted: with no live waiter left, a burn would spend a chain-costed nonce on nobody, under a reason that never happened.
- **serve despite exclusion** is the rescue for a waiter whose only remaining host is one it excluded, past the stale window. It is logged, because a nonce was spent on a host the request asked to avoid. See routing.md, "Serving a host the request excluded".
- **burn** names the block that earned it. Participant-only blocks map to their ghost reason through one table rather than two switches.

## The waiter handoff

A waiter's reply channel is buffered so the dispatcher's handoff never blocks on the caller, and `deliver` never blocks anyway: a full or abandoned channel means the caller is gone. `abandon` takes whatever was already handed over under the same mutex that guards delivery — leaving and taking are one step, because an assignment delivered in that same instant holds a committed nonce and a concurrency slot nobody else will give back. A cancelled caller that finds one releases the slot and the escrow hold and records the nonce as an abandoned ghost.

`submitWaiter` never blocks either: a submit arriving before the actor starts, or faster than it drains, must not stall its caller or pin the lifecycle lock. A full queue and a stopped dispatcher are reported apart, because the first is rate-limit class and the second is retryable against a fresh actor.

## Dispatcher lifecycle

The dispatcher's loop goroutine is the sole owner of the waiting queue and of the session. See routing.md, "The per-escrow dispatcher".

- `stop` is idempotent and blocks until the loop has exited. It holds the write lock while closing the stop channel, which keeps `submitWaiter` from landing a waiter in a buffer nobody will ever read.
- `markStopped` shuts the actor down from inside its own goroutine and refuses once a waiter is already in the submit buffer.
- `retire` runs under the registry lock and only for an actor with no outstanding claim. The claim is taken in the same critical section that hands the actor out, so an actor deciding to retire cannot slip between the claim and the submit that follows. A stopped dispatcher is replaced by the next get-or-create, and the claim keeps the replacement alive, so a submit retries at most once more before the registry itself is closed.
- An escrow's actor is reaped after `idleDispatcherGrace` with an empty queue. Retirement is announced so an observer can forget the escrow: ids are monotonic chain identifiers and are never reused, so a per-escrow metric series that outlives its escrow grows with uptime and nothing else. See routing.md, "Idle dispatchers are reaped".
- **The divergence block and the spent replay are kept on purpose.** A dispatcher is recreated for the same escrow on the next request, and dropping either would hand a host that cannot follow this escrow's chain a fresh replay for having been quiet five minutes. What is kept is one entry per escrow that ever saw a divergent host, held for the life of the process.

## Divergence and the replay credit

A participant gets one catch-up replay per escrow before a state-root divergence blocks it there. A host rolls its diff back when its root disagrees, so its state survives the refusal intact and replaying the retained chain costs one request to try.

The credit is returned to a participant that served since it was taken — but only for a send that *started* after the credit was taken. Requests to one participant overlap, so a send that started before the replay proves nothing about the replayed state.

## Failure vocabulary

| Error | What it means | Does waiting help? |
| --- | --- | --- |
| `ErrNoAvailableHost` | no participant can take the request: excluded, in PoC, throttled, ejected, state-blocked, or the drain's burn budget tripped | yes |
| `ErrHostsBusy` | every host is at capacity right now — distinct from broken or excluded, and a client retries the two differently | yes |
| `ErrAllowlistUnreachable` | no escrow this gateway serves holds a participant the allowlist admits; the operator narrowed routing to participants none of these escrow groups contains | no |
| `ErrNoEscrowCapacity` | every candidate escrow is at zero spare weight; it deliberately does not name a host | — |
| `ErrEscrowBusy` | an escrow's dispatch queue is full: the escrow is sound, the caller arrived faster than it can serve | yes |
| `ErrDispatcherStopped` | the escrow's dispatcher shut down before the request was assigned a nonce; retryable | yes |
| `ErrEscrowGone` | a request's pinned escrow no longer accepts new inferences | no |

## The boundary types

The session adapter lives outside this package, so the two sides meet on a deliberately narrow contract.

- `session.Advance` is the one atomic nonce-peek → decide → commit unit: it computes the next candidate binding, calls the decision function, and commits only if the returned intent says to. A declined nonce is left untouched and yields a nil `Prepared`.
- `NonceIntent` exists because that adapter cannot branch on `Decision`, whose variants are unexported.
- `RequestProfile.Params` is forwarded to `Advance` unread and committed there as the escrow's inference params, so it must be exactly `devshard/user.InferenceParams` — not the request body it was built from, which the adapter cannot commit and will reject. `NonceIntent.Params` carries it verbatim under the same requirement, and is set only for a non-ghost commit.
- `Prepared` is satisfied verbatim by `*user.PreparedInference`, so the api adapter needs no conversion code.
- `Assignment.EscrowHold` gives back the escrow's in-flight count the commit took. It is idempotent, and nil when the escrow source counts nothing. A caller that has taken its own hold releases this one as soon as it has one; a caller that never dispatches releases it instead of dispatching.
- `Escrow.Hold` is taken with the nonce commit and refused once the escrow has been retired. `Candidates` returns escrows in a stable order, already filtered to accepts-new-inferences.
- On `hostLimiter`, `Acquire` is the admission authority and runs with the commit; `Available` is only a cheap pre-filter, so a match-wait answer from it costs nothing. A slot handed to a caller is released by the engine that spends it, never here.
- On `hostHealth`, `Ejected` is already capped to a fraction of the model's known hosts, so honouring it here can never empty the pool.
- The PoC-preserved set is read per drain, preferring the model's own; a nil set means it has not loaded yet, so every participant counts as preserved. See rules.md, "8. Fail-closed and fail-open are chosen per signal".
- `AffinityHint` is the extension point for KV-cache affinity: the per-request handle a later revision will rank hosts with. It carries nothing and changes no decision today.

## Read next

- [`docs/routing.md`](../docs/routing.md) — the drain, the gates, the hold, and every burn reason.
