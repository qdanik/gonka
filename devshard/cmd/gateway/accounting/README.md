# `accounting` — the per-nonce ledger

Every nonce the gateway commits costs the escrow money whether or not anyone was served. This package answers, for each one, **where it went**.

## What it owns

- **`Book`** — the ledger. Facts go in (`RecordRace`, `RecordGhost`, `RecordTimeout`, `RecordAppliedTimeout`, `RecordInvalidVerdict`, `ObserveHostStats`, …), and each nonce lands in exactly one counter keyed by `CounterKey`: how it ended, why, and the timing flags that were true of it.
- **Findings** (`findings.go`) — the operator-facing verdicts derived from those counters: a ratio, a threshold, a severity. A finding is a claim about a host, so it fires only past `findingMinimumVolume` nonces.
- **A read API** (`http.go`) — epochs, participants, one participant, and the protocol event feed (`events.go`), which maps a chain-applied verdict back to the nonce and the client request that spent it.
- **Persistence** (`store.go`, `sqlstore.go`) — a periodic snapshot so a restart does not lose the epoch.

## What it does not own

It records; it does not decide. Nothing here withholds a host from routing, changes what is dispatched, or talks to the chain. The facts arrive from [`nonces`](../nonces/), which is the only writer.

## Boundaries

- **The response schema is frozen.** `ParticipantRecord`, `SlotRecord` and `EpochSummary` are read by a tracker outside this repository. Adding a key is tolerable; renaming or removing one is a regression. Internal carriers between `slots()` and `absorb()` stay unexported rather than taking a JSON tag.
- **`SchemaVersion` is one number for two readers.** It is the report's `schema_version` and the value `sqlstore` checks on open; a mismatch drops the tables rather than migrating them, because the ledger is an epoch of observations, not a system of record. It is **7**, above the legacy `devshard/accounting`'s 6: both packages emit `schema_version` over different shapes, so the number is the only thing telling them apart.
- **Aggregation happens on read, not on write.** Every total above the counters is derivable, so a stored total could only ever disagree with its own parts.
- **`in_flight` and `in_flight_requests` are not live-request gauges.** A race reaches the ledger only once it has ended, so these count nonces the chain has yet to settle. The live number is the limiter's `devshard_gateway_inflight_requests`.

## How a nonce is classified

A nonce is filed under a **CounterKey** — one bucket per observed combination of slot, disposition and the dimensions around it. Only combinations that happened exist, and every dimension is bounded by its producer, so an escrow holds a few hundred buckets rather than a row per nonce.

Three shapes are the same struct summed at different levels: one slot of one escrow, one host across all of them, and one epoch across all hosts. Each is the sum of the rows below it, which is why aggregation happens on read rather than in storage — every total is derivable, so a stored total could only ever disagree with its own parts.

Open requests are **unioned rather than summed** when rows are folded: one request spends several nonces, so slots of the same escrow hold it at once and summing would count the client twice.

Two classifications are worth naming because they look like something else:

- **Committed and never dispatched** is not a ghost — nothing decided to burn it — and not in flight, because the race that held it has reported. It is an unfinished refusal: no host ever saw it.
- **An empty terminal is itself a fact**: the nonce reached classification without the race ever reporting on it. It is named rather than left blank, which reads as missing data and drops the bucket from any grouping by terminal.

Seeing a nonce raises the assigned watermark too. The chain's latest is polled on a sweep while nonces arrive on every race, so without this the counters outrun the range they are measured against and the gap reports as a disagreement with the chain.

Re-opening an escrow keeps its counters and **pins the epoch at first sighting**: one that re-stamped itself on every sweep would drag its whole history into the current epoch. Epoch is part of the query key for the same reason — a rotation leaves escrows of adjacent epochs live at once, and summing those would report one epoch's work under another's index.

## The ledger's check on itself

`namesNoReason` counts the nonces whose cause the ledger could not name, matching the fallbacks the producers actually emit: an engine terminal with no string of its own, an attempt the race never classified, and a burn kind with no reason. `unreported` is **not** one of them — that names a race that reported nothing, which is a fact rather than a gap.

`timeoutOutcomeOf` is where two vocabularies meet: the engine reports an action and a reason, the legacy ledger reports a single outcome, and one dashboard has to read both. `failed` alone says nothing a reader can act on; its reason is what tells a round that gathered too few votes from one that could not gather any.

## Storage

The pragmas ride the connection string rather than a first statement, because the pool recreates connections and a recreated one would come back without them. One connection, because the ledger is written whole inside a single transaction and has no concurrent writer to gain from more.

Nothing queries this database except the ledger's own load at start-up — every question an operator asks is answered from memory. The tables therefore mirror the in-memory shape one for one, and a write is one transaction that empties and refills them. That is what makes a half-written ledger impossible: a reader sees the previous contents until the commit, and a crash leaves them.

`SlotActivity` is the gateway's own side of the counts `HostStats` holds the chain's side of. Restoring one without the other reports every restart as a disagreement between the two.

Only nonces whose disposition can still move are written down. A burned or finished one cannot — no later fact lifts it — while an unfinished one can, because the protocol may finish it after the race gave up, and one still awaiting its timeout has no disposition yet.

## Read next

- [`docs/accounting.md`](../docs/accounting.md) — every finding, its threshold, and what an operator should do about it.
