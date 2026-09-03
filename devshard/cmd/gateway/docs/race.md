# Devshard gateway — the speculative race

One client request is a *race*: one to N attempts against distinct participants, of which at most one is crowned and streamed to the client. Speculation exists because a devshard host can fail in ways that a timeout alone does not catch — it can answer instantly with nothing, receipt and then never produce a token, or simply be much slower than a peer. Racing turns those into a latency cost rather than a failed request.

This document covers `engine/`. Where a nonce comes from is in [routing.md](./routing.md); the invariants the engine must not break are in [rules.md](./rules.md).

## Anatomy

```mermaid
sequenceDiagram
    participant API
    participant Coordinator
    participant A1 as Attempt (primary)
    participant A2 as Attempt (speculative)
    API->>Coordinator: Run(request, client writer)
    Coordinator->>Coordinator: pick, resolve target, take escrow hold
    Coordinator->>A1: launch
    A1-->>Coordinator: dispatched / receipt / first token / chunk
    Note over Coordinator: escalation timer fires,<br/>Confirm re-validates the trigger
    Coordinator->>A2: launch
    A2-->>Coordinator: receipt
    A2->>Coordinator: crown request (first content chunk)
    Coordinator-->>A2: you are the winner
    A2->>API: buffered prefix, then live bytes
    A1-->>Coordinator: done (lost)
    Coordinator->>Coordinator: one RaceOutcome, then timeout votes
```

Each attempt runs in its own goroutine and communicates with the coordinator only by events. The coordinator owns the race state and is the only thing that decides anything.

## Crowning

**A winner is crowned by its first chunk of actual content** — not by a receipt, not by the first token, and not by HTTP 200 (`engine/classify.go`, `chunkSignal.crownsWinner`; `engine/race.go`, `raceCoordinator.answer`).

A host that responds instantly with an empty stream wins on any earlier signal, and the client gets nothing while a slower, honest host is cancelled. Requiring content means the empty host loses to whoever produces tokens.

The mechanics are a handshake, not a flag. An attempt's writer buffers everything it receives while it has produced no content; on the first chunk that carries content it sends a crown request and **blocks** on the reply (`engine/stream.go`, `winnerWriter.Write` and `winnerWriter.claim`). So no byte reaches the client before the coordinator has settled on a single winner. The coordinator's answer is you are the winner or you are suppressed, and a suspicious host's claim is **held** until it can be answered honestly. A suppressed attempt's writer then discards its buffered prefix and clears its client field entirely — a loser has no reachable sink at all, rather than a sink reachable behind a branch. Suppression is permanent, which is why the claim is held rather than refused early: an attempt refused a crown can never be given one, so refusing it while a rival might still fail would throw away an answer the race has already paid for.

A suppressed attempt keeps reporting successful writes to its host, so the host keeps streaming to its own receipt. Its bytes go nowhere.

The buffered prefix matters for correctness of the visible stream: role announcements and comment chunks that arrive before the first content are flushed to the client in order, ahead of the chunk that won. Past the 32 MiB carry cap the prefix is dropped, never the attempt — a capped attempt still wins, and its client stream simply starts at the chunk that crowned it (`engine/stream.go`, `winnerWriter.buffer`).

### An SSE error event counts as a chunk but never crowns

An error event increments the attempt's chunk count — so the stream is *not* empty — while carrying no content, so it cannot crown (`engine/attempt.go`, `attemptState.record`; `engine/classify.go`, `chunkSignal.crownsWinner`). That combination is what distinguishes "the host said something went wrong" from "the host said nothing", and the two are charged differently.

A **capability refusal** is a third case and is kept out of the error class entirely (`engine/reassembly.go`, `sseClassifier.facts`): another host can still serve the request, so it must neither count as a chunk nor end the race, while its message still reaches the performance recorder. On a refusal the engine records the host's capability limit (`engine/capability.go` → `perf/tracker.go`, `RecordContextLimit`). Nothing routes on it: the count is reported so an operator knows what to fix.

### Crown denial

A host that repeatedly answers with no content stops being crownable, and keeps receiving attempts until the breaker cuts it off for leaving nonces open. Three content-free answers cost the crown; one content-bearing answer buys it back immediately (`engine/engine.go`). While denied, the host is treated as suspicious: a race that starts with it launches a speculative attempt immediately, and its claim on the client stream is held for as long as **any rival could still serve**. A rival is one already running that has not claimed, one whose pick is in flight, and one the race has committed to starting but not yet picked for — the replacement a suspicious primary earns is a rival from the moment the race decides to fetch it, not from the moment it launches. When none is left the held claim is crowned, because then its answer is the one the race committed a nonce for and refusing it would hand the client an error for a response that exists (`engine/race.go`).

The operator's manual suspicious-host pins fold into the same gate, so a pinned host escalates and is held back however well it has been answering.

An empty stream that burned completion tokens on a thinking-budget route is *not* a content-free answer: it is a model outcome, and the host is innocent. The check that separates them reads the host-reported usage — and it is allowed only on a thinking-budget route, because anywhere else any host could fake a usage object to escape the empty-stream penalty (`engine/classify.go`, `thinkingBudgetRoute`).

## Classification and reassembly

The classifier reads an attempt's SSE stream incrementally and yields, per chunk, whether it carried content, an error, a capability refusal, and how many tokens the host claims to have burned.

TCP does not deliver event-aligned chunks, so a carry buffer reassembles events split across reads and classifies each exactly once, when its final line arrives (`engine/reassembly.go`, `sseClassifier`). The unterminated final event is classified on flush, before emptiness is decided, so a host is not charged for an empty answer it did not give.

Reassembly is charged against three budgets — per attempt (1 MiB), per participant (10 MiB) and process-wide (100 MiB). The participant level is the one that matters: it stops a single host that never terminates an event from draining the shared pool and starving every other host of classification (`engine/carry.go`, `carryBudget`). A cap trip releases the fragment and classifies the raw chunk instead, so classification *degrades* rather than stopping, and the first trip is reported once as a metric. A trip on the global budget undoes the participant charge, because a charge left behind on a trip is quota nobody can return.

## Escalation

The escalation policy is **pure**: a function of its arguments and the configured thresholds, with no chain snapshot and no clock of its own. It reads no host performance data either — the race reads the outlier detector and hands `Decide` a boolean, so the policy's whole input is its argument list (`engine/escalation.go`).

Stages that can trigger another attempt. The reason column is the wire string, which is what `devshard_gateway_escalation_decisions_total{reason}` carries:

| Reason | When |
|---|---|
| `suspicious_host` | The first attempt's host is crown-denied or operator-pinned — escalate immediately. |
| `receipt_timeout` | No receipt within the receipt timeout (doubled above 100 000 input tokens, because admitting a very large prompt is itself work). |
| `first_token_timeout` | No first token within the first-token deadline. |
| `attempt_failed` | An attempt ended without producing anything usable. |

An attempt launched at race start beside a primary the race distrusts carries a start reason rather than an escalation reason on `devshard_gateway_attempts_started_total{reason}`, and the two vocabularies do not overlap: `primary_suspicious` for a crown-denied or operator-pinned host, `primary_degraded` for one the outlier detector wanted out of rotation. The second exists because the routing gate that withholds an ejected host is capped, and a fleet failing together stays routable by design — see [capacity.md](./capacity.md).

The first-token deadline is a fixed quadratic in prompt size, `1.7 + 3e-5·T + 5e-10·T²` seconds, with a floor from `engine_first_token_floor_ms` (`engine/escalation.go`, `EscalationPolicy.firstTokenTimeout`). At the default floor of twelve seconds the floor is what binds for ordinary traffic: the quadratic only overtakes it above roughly 117 000 input tokens.

**Arming is not permission.** `NextEscalation` yields an `ArmedEscalation`, which is a deadline and nothing more. The only producer of an actionable escalation is `Confirm`, which re-derives the trigger *at fire time* and rejects a trigger that has vanished, a stage that has advanced, or a timer that fired early (`engine/escalation.go`, `ArmedEscalation` and `EscalationPolicy.Confirm`). This exists because an attempt's stage moves while its timer runs — a receipt landing just under the receipt timeout is the common case — and escalating on the armed stage would start a needless extra attempt on *every* healthy request. Confirming re-reads state, so the type system enforces it: an unconfirmed `ArmedEscalation` carries a deadline and no permission, and cannot be acted on.

The trigger is consumed *before* the new attempt starts, so a failed start cannot retry the same trigger (`engine/race.go`).

The escalation pick runs on its own goroutine rather than inline in the coordinator loop (`engine/race.go`). The scheduler may hold the nonce briefly for a co-arriving request, and a crown claim is the client's first token: waiting for the pick inside the loop would spend that hold on exactly the latency escalation exists to avoid. A departing client cancels the pick, which returns the nonce and the slot.

**There is one ladder, because there is one reply shape.** The gateway asks every host to stream whatever the client asked for ([`filters/README.md`](../filters/README.md), "Streaming is forced upstream"), so every request has a first token to wait for and every attempt that ends without one is escalated on the spot. There is no buffered rung. A `response_timeout_retry` deadline scaled by an output-token rate configured at 50 against a measured 25 a second models half the real time, so its floor decides every fire, and a rung that never fires on its own terms reads as coverage it does not give. `engine_non_stream_response_floor_ms`, `engine_non_stream_response_ceiling_ms`, `engine_per_output_token_response_lag_ms`, the one-shot retry they gate and its exemption from the attempt budget do not exist.

**Scarcity overrides speculation.** When the chain is blocking requests and the relaxed bypass is not active, the attempt budget collapses to one: a speculative attempt spends a nonce the phase the gateway is serving through will not replace.

## Deadlines

One re-armed timer carries every deadline. `nextDeadline` takes the earliest of four families — the hard timeout, the escalation, the pick and the stall — and the declaration order of the trigger constants breaks *exact* ties only (`engine/race.go`, the `deadlineTrigger` constants and `nextDeadline`):

1. **Hard timeout** — the minimum of: the drain deadline once the client has left; 30 minutes of total wait and 20 minutes without content for a non-streaming race; the loser grace after a crowned attempt finishes; and 20 minutes per live attempt.
2. **Escalation** — the next armed trigger, suppressed while a pick is already running, and once the race is crowned, detached, or at its attempt budget.
3. **Pick** — when an unanswered escalation pick stops being worth waiting for; zero while no pick is running.
4. **Stall** — the earliest `last chunk + inter-chunk stall` over attempts that have produced content and gone quiet.

The tie-break order is itself the policy: a race that must stop gains nothing from spending a nonce, and a stall flag is telemetry either way.

**Every select arm that reads race state drains the event queue first.** A buffered event and a fired timer can both be ready, and `select` picks at random, so an arm that reads state without draining acts on state a queued event has already invalidated. Two arms read race state: the deadline timer and the client's departure (`engine/deadline.go`, `engine/race.go`).

## Client departure and the drain

A client disconnect does not kill the race. The receipt, the response the session applies to its own state, and the vote that settles a committed nonce all have to complete after the client is gone.

The race context is `context.WithoutCancel` of the client's context: cancellation is dropped, values are kept (`engine/drain.go`, `drain` and `newDrain`). Writes to a departed client are reported as successful rather than failing, because a write error would end the attempt carrying them and the host that earned the crown still owes its receipt (`engine/drain.go`, `clientStream`).

From departure onward the drain deadline is what bounds the race — forty minutes by default, and by construction longer than any deadline a race arms for itself, so the only host it ever ends is one that streams forever without tripping any other bound (`engine/drain.go`, `defaultDrainTimeout`).

Once the winner has finished, the client's request handler is released and any still-pending losers are handed to a second `await` on a background goroutine. Nothing they can do changes what the client received (`engine/race.go`, `runRace` and `raceCoordinator.release`).

## The outcome

Everything the race learned is folded into one `RaceOutcome`, and one field decides everything downstream: `Terminal`.

There are twenty-three terminal values, and every downstream vocabulary — limiter verdict, performance sample, metric label — is a *total function* of it (`engine/outcome.go`, `Terminal`). The HTTP-status recovery and the SSE inspection that decide the terminal therefore happen once, where the error and the bytes are, instead of being re-derived at each consumer.

`Rejected` is the one terminal whose scope is not obvious from its name: it covers every upstream 4xx that is neither throttling nor one of the named statuses, because those describe the request, not the host's ability to serve, so they move nothing (`engine/outcome.go`, `TerminalRejected`). A 5xx is the opposite case and gets its own terminal: the host answered but what it serves the request from did not, so `UpstreamServerError` halves the window on its own verdict, `UpstreamFault`. That verdict exists because the host demonstrably answered: unlike a throttle it must not clear the count of unanswered faults the breaker opens on, or a host alternating 5xx answers with connection resets would never be cut off.

The exception is a 5xx that names a fault in what the gateway sent. A host reports every diff it cannot apply as a 500 carrying the error's text — an escrow it cannot find, a nonce past the chain's cap, a balance, a payload hash — and none of those is the host's doing, so `transport.IsUpstreamRequestFault` keeps them a `Rejected` that blames nobody. The predicate matches on the shared error sentinels rather than a list of phrases, so the two sides cannot drift apart.

### The three translations

| Consumer | Rule |
|---|---|
| Limiter verdict | Won/Lost → success; throttled and unavailable → overload; an upstream 5xx → upstream fault, which halves the window and leaves the breaker's count alone; transport-class terminals and an empty stream that never finished its nonce → transport fault; burn-empty, error stream, capability refusal, a reply past the gateway's own buffer cap, and an empty stream that did finish its nonce → model outcome, which never moves a host's window. |
| Performance sample | One sample per attempt, unless the exemption ladder excuses it. The sample carries participant, model, whether the host was responsive, and the two timings the escalation ladder reads back as quantiles. |
| Metric labels | Bounded label vocabularies exported by the engine and referenced — not restated — by the metrics layer. |

"An empty stream is what the model produced, not what the host failed to carry, so the host's window must not contract for it" (`engine/outcome.go`, `Terminal.verdict`) — but only once the nonce is closed. A host that said nothing and left the nonce open did not produce an empty answer; it took the work and parked the reserve until the timeout vote, so it answers to the breaker as well as to crown denial (`engine/outcome.go`, `RaceOutcome.Verdict`).

### The exemption ladder

Whether an attempt contributes a performance sample at all is decided by one ordered ladder, applied in `Engine.record` and nowhere else (`engine/outcome.go`, `RaceOutcome.sampleExemption`). The legacy gateway made this decision at six divergent call sites.

1. Never dispatched — the attempt exists only because of the gateway's own bookkeeping.
2. Never reported — the attempt returned no terminal at all, so there is nothing to judge it on.
3. Ended by a proof-of-compute phase transition — blame the transition, not the host.
4. Error stream or capability refusal.
5. State-divergent.
6. Long response after content — the host produced output, its nonce is still open, and the attempt has run past the exemption window, so the delay is a slow answer rather than an unresponsive host. An attempt the twenty-minute backstop cut does not take this rung; it is judged on its own terminal.
7. Empty stream while the proof-of-compute bypass is active.
8. Empty stream in a race nobody won.
9. Cancelled by the race itself — a sample would say the host was unresponsive when it was told to stop.

Two parallel ladders use the same facts for different questions: the *verdict* ladder decides whether the AIMD window moves, and the *timeout-skip* ladder decides whether a vote is posted. The sample ladder disagrees with the verdict ladder in exactly one place. A loser the race cancelled needs no verdict rung, because no terminal maps `client_cancelled` to a verdict at all; it does need a sample rung, because a recorded sample would report the host as unresponsive when the race is what told it to stop. That is rung 9.

### Timeout votes

Every attempt whose nonce the host did not finish gets a vote posted, with one of four recorded skip reasons where it must not be (`engine/settle.go`, `RaceOutcome.timeoutSkipReason` and `RaceOutcome.TimeoutPlan`): the attempt was aborted by a phase transition, the stream was empty and the nonce already finished, the nonce is already finished, or the response ran long after producing content.

A host whose escrow state diverged still gets its vote posted. Divergence is a routing fact — the scheduler blocks the host permanently — while an unposted vote leaves an orphaned start message that settlement can never resolve (`engine/settle.go`, `RaceOutcome.timeoutSkipReason`).

A vote that fails is written down as a warning naming the nonce, the host and the reason, because a vote is the only thing that undoes a charge; the one exception is an escrow gone from the chain, which fails every vote it owed at once and has its own line already. Posting runs on its own goroutine beside the race, because the protocol wait is measured in minutes. The engine's registration for that race is released only inside that goroutine, after the vote (`engine/engine.go`, `Engine.settle`).

One external quirk is absorbed at the boundary: the shared session's timeout handler returns a **non-nil error on its success path**, so a posted vote is recognised by the handler's own `Applied` flag rather than by `err == nil` (`engine/session.go`, `SessionTimeouts.SettleTimeout`). The failure mode that used to be structurally identical — a diff the group carried without the timeout — is now marked at its source with `user.ErrTimeoutNotApplied`, so a caller reading the error alone no longer counts it as a posted vote (`user/timeout_effect.go`, `timeoutSettledError`). In the legacy gateway this quirk made the "completed" branch unreachable, so every posted vote was labelled failed.

### The vote nobody retried

The vote above is attempted **once**, at the end of the race that owned the nonce. A round that finds no verifiers, or a restart that outlives the round, leaves the nonce started, unvoted and still settleable — and `settleLiveRecordLocked` settles a started record at the **full reserved cost** in the executor's favour, without incrementing its `Missed` (`state/machine.go`).

The sweep is what claims it back. Every escrow tick scans its own live records for ones started, stamped and past their execution deadline by a grace, and re-votes them through the same `HandleTimeout` (`state/started_deadline.go`, `user/timeout_sweep.go`, `registry/timeout_sweep.go`). Three properties keep it off the hot path:

- **the scan is bounded by live records, not by traffic** — one pass under the read lock, no allocation when nothing is due (1.4 ms at 100 000 live records, 12 µs at 1 000);
- **the votes are bounded by one budget per tick across every escrow** (8 by default, so at most 32 a minute), and the walk rotates its starting escrow, so the RPC it adds never follows the request rate and no escrow starves;
- **the grace keeps it off a nonce whose own race is still due to wake and vote**, which is the ordinary case.

A nonce the verifiers decline stays started, and the next sweep finds it again; nothing is retried faster than the budget allows.

## Stop

`Engine.Stop` is a barrier over admitted races, not over finished ones. A race is registered under the engine's mutex *before* it starts and released only after its vote goroutine finishes, so a `Stop` that overlaps a running race cannot observe the engine as idle (`engine/engine.go`, `Engine.Stop`, `Engine.admit` and `raceRegistration.release`). The wait is bounded by the deadlines a race arms for itself — twenty minutes, or forty for a drain — never by a host.

A panicking race still releases its registration and then re-panics with the same value, because recovering would answer the client with an outcome no race produced, and *not* releasing would hang shutdown forever (`engine/engine.go`, the deferred recover in `Engine.Run`).

## Tunables and backstops

Carried in the configuration snapshot (`config.Engine`, `config.Stream`) and bounded by `Config.Validate`, at start-up and on every admin write. The five timings below take a `GATEWAY_ENGINE_*` variable and an admin override; a race reads the snapshot when it is admitted (`engine/engine.go`, `Engine.Run`), so a swap reaches the next race and never re-times one already running. `engine_max_attempts_per_request` is not among them: it multiplies nonces per request rather than moving a deadline, and stays a restart.

| Field | Default | Effect |
|---|---|---|
| `engine_receipt_timeout_ms` | 5 000 | Receipt deadline; doubled above 100 000 input tokens. |
| `engine_first_token_floor_ms` | 12 000 | Lower bound on the first-token curve. Binds below roughly 117 000 input tokens. |
| `engine_first_token_ceiling_ms` | 30 000 | Upper bound, whatever the host's own p75 asks for. |
| `engine_inter_chunk_stall_ms` | 30 000 | Silence after first content before an attempt is flagged stalled. |
| `engine_loser_grace_ms` | 600 000 | How long losers may keep streaming after the winner finishes. Must be at least the stall window, or losers merely between chunks are killed. |
| `engine_max_attempts_per_request` | 2 | Attempts one race may hold; 0 means bounded only by the host group. |
| `drain_timeout_seconds` | 2 400 | Bound on a race after its client leaves. |
| `classify_max_attempt_bytes`, `classify_max_participant_bytes`, `classify_max_global_bytes` | 1 / 10 / 100 MiB | Reassembly budgets: attempt, participant, global. |

Go constants rather than configuration — these bound a request that every value above already failed to bound (`engine/escalation.go`, the backstop constants):

| Constant | Value |
|---|---|
| Streaming hard timeout | 20 minutes per live attempt |
| Scheduler pick timeout | 2 minutes waiting for a host to be assigned |
| Receipt-doubling input-token threshold | 100 000 |
| Long-response exemption | 280 seconds |
| Crown-denial strikes | 3 |
| Event channel capacity | 32 |
| Crown prefix carry cap | 32 MiB |

One divergence follows from those numbers: because the streaming hard timeout (20 minutes) always exceeds the long-response exemption (280 seconds), a stalled winner is always past the exemption. `Stalled` therefore does not move a limiter window **while the host is still within its failure-rate budget** — `RaceOutcome.Verdict` exempts it as a model outcome whenever the attempt's `FailureRateExceeded` is unset, which the race fills from the outlier detector's ejection verdict. Once the host is already ejected the exemption stops applying and `Stalled` falls through to `Terminal.verdict`'s transport-fault class, which does move the window. The exemption is bounded by the failure-rate budget rather than absolute: `FailureRateExceeded` is the guard that bounds it.
