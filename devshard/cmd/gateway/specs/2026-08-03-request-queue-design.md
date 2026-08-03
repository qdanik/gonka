# Request queue with a single wait budget

**Goal.** A shard must not answer 429. An ordinary few-second hiccup should become invisible — the request simply answers a little later. A refusal survives only where waiting buys nothing.

**Origin.** Operator request: the shard should absorb this internally; let a request hang for up to three minutes rather than return 429.

## Why 429 is the wrong answer here

429 means "you exceeded a quota". The client exceeded nothing — the shard had no capacity at that moment. The honest answers to exhausted capacity are to wait, or to say 503 with a hint of when to come back.

## What the investigation found

Two independent places can refuse, and only the first has a queue.

| | question it answers | what it reads | queue |
|---|---|---|---|
| Gate 1 — `limits.GatewayLimiter` | "do we have budget for this?" | concurrent requests, input tokens, chain weights | **yes**, FIFO with promotion |
| Gate 2 — `scheduler` | "is there a live host to take it?" | participant windows, breakers, chain phase, capability | **no**, refuses at once |

Production confirms it: twenty concurrent requests produced zero gate-1 refusals and nine gate-2 refusals. Raising `acquire_wait_ms` treats the gate that was not refusing.

Both gates already accept a `context.Context` and already select between "the resource freed" and `ctx.Done()` (`limits/gateway.go`, `AcquireForModel`; `scheduler/scheduler.go`, `Pick`). Nobody gives them a deadline — that is the whole gap.

## Decisions

**Policy: a bounded queue.** Unbounded waiting is rejected: the client spends the whole budget and receives the same refusal, only later and more expensively, while connections pile up meanwhile.

**Budget: one deadline per request.** Each gate waits within what remains. The client is promised one number regardless of which gate it stalls at. Rejected alternatives: two independent windows (total wait doubles and was never promised), and "gate 1 decides everything" (host liveness changes faster than the decision, so the guarantee would be false).

**Waiting rule: wait for what passes on its own, and soon.**

| wait | do not wait |
|---|---|
| no budget in the limiter | proof-of-compute phase — minutes, with a known end |
| participant windows full | model not served |
| participant breaker open | escrow spent, being replaced |
| nonce briefly unavailable | request invalid |

## Changes

### 1. A pick deadline — blocking prerequisite for step 3

The budget must bound **waiting**, not the inference. An inference runs for minutes; a deadline derived from the request context would cut it off.

Gate 1 needs nothing: `AcquireForModel` already times its own wait out of `acquire_wait_ms`. Gate 2 has no deadline from anyone, so the moment it starts waiting instead of refusing, a request hangs until the client disconnects. This was reproduced: with step 3 applied and this step missing, a scheduler test hung rather than failing.

So the deadline must be a **separate context for the pick**, born where the pick is issued and never handed to the attempt. Two places issue one: `engine/race.go`, `raceCoordinator.pick` for the primary and `startPickWithinBudget` for the escalation — the second already bounds itself. Settle where the primary's bound is born before touching the scheduler.

**Order is not advisory: step 3 without this hangs requests.**

### 2. The scheduler learns to wait (after the pick deadline exists)

`cmd/gateway/scheduler/dispatcher.go`, `sweepExhausted`. Today a waiter that no participant can serve is failed immediately. It should stay queued until a participant window frees, its context expires, or the reason turns out to be one that does not pass.

Half the split already exists: `servable` reports "everyone is busy" separately from "everyone is broken" (`ErrHostsBusy` versus `ErrNoAvailableHost`). The first becomes a wait; the second stays an immediate refusal.

Care: `drain` already holds waiters between passes, and nonces have a burn budget. Waiting must not become a nonce walk — a waiter waits on a **release event**, it does not spin.

### 3. A depth cap

Before joining the queue: can we serve it within the remaining budget? The first version estimates from slot count and time per request, with no speed model. If it does not fit, answer 503 with `Retry-After` at once, without waiting.

Refining the cap from the measured rate (25.7 output tokens/s, 253-token average answer — about 10 s per slot) waits until queue-depth data exists.

### 4. What the client gets

`cmd/gateway/api/errors.go`. Every capacity refusal answers 503 and carries `Retry-After` — the wait we already spent when we know it, a default when we do not. `RateLimitError` no longer reaches a chat client as 429.

### 5. Visibility

Queue depth per model (partly present in `LimiterSnapshot`), a histogram of wait-before-service, refusals by reason. Without these the cap is tuned blind.

## Tests

- A request with no capacity now, and capacity in N seconds, **is served** rather than refused. Mutant: restore the immediate refusal in `sweepExhausted` — the test must fail.
- A request arriving at a provably full queue gets 503 **at once**, without spending the budget.
- A non-passing reason (chain phase, unknown model) answers immediately even with an empty queue.
- Total wait never exceeds the budget, whichever gate the request stalls at.
- No capacity refusal reaches a chat client as 429, and each carries `Retry-After`.

## Order

1. ~~Client-facing answers~~ — done. Every capacity refusal is 503 with `Retry-After`; the wait budget is 120 s.
2. ~~Depth cap~~ — done. `queue_depth_per_slot` = 12, from the budget over ~10 s per request.
3. **Pick deadline** — blocking prerequisite, see above.
4. The scheduler learns to wait — the real work, and only after 3.
5. Visibility — can run in parallel.

## Open questions

- ~~The budget number.~~ Settled at 120 s: three minutes of idle waiting was judged too long. Depth follows at 12 per slot.
- **Proof-of-compute phase.** The switch height is known, so an exact `Retry-After` is possible instead of a default. Worth the complexity?
- **Fairness across models.** The queue is one FIFO today. A single heavy stream can hold it. Leave as is until a second model runs under load.
