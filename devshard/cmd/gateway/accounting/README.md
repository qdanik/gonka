# `accounting` — the per-nonce ledger

Every nonce the gateway commits costs the escrow money whether or not anyone was served. This package answers, for each one, **where it went**.

## What it owns

- **`Book`** — the ledger. Facts go in (`RecordRace`, `RecordGhost`, `RecordTimeout`, `RecordAppliedTimeout`, `RecordInvalidVerdict`, `ObserveHostStats`, …), and each nonce lands in exactly one counter keyed by `CounterKey`: how it ended, why, and the timing flags that were true of it.
- **Findings** (`findings.go`) — the operator-facing verdicts derived from those counters: a ratio, a threshold, a severity. A finding is a claim about a host, so it fires only past `findingMinimumVolume` nonces.
- **A read API** (`http.go`) — epochs, participants, one participant, and the protocol event feed (`events.go`), which maps a chain-applied verdict back to the nonce and the client request that spent it.
- **Persistence** (`store.go`, `sqlstore.go`) — a periodic snapshot so a restart does not lose the epoch.

## What it does not own

It records; it does not decide. Nothing here withholds a host from routing, changes what is dispatched, or talks to the chain. The facts arrive from [`nonces`](../nonces/), which is the only writer.

## Boundaries worth knowing

- **The response schema is frozen.** `ParticipantRecord`, `SlotRecord` and `EpochSummary` are read by a tracker outside this repository. Adding a key is tolerable; renaming or removing one is a regression. Internal carriers between `slots()` and `absorb()` stay unexported rather than taking a JSON tag.
- **Aggregation happens on read, not on write.** Every total above the counters is derivable, so a stored total could only ever disagree with its own parts.
- **`in_flight` and `in_flight_requests` are not live-request gauges.** A race reaches the ledger only once it has ended, so these count nonces the chain has yet to settle. The live number is the limiter's `devshard_gateway_inflight_requests`.

## Read next

- [`docs/accounting-findings.md`](../docs/accounting-findings.md) — every finding, its threshold, and what an operator should do about it.
