# `journal` — one path from a lifecycle step to its log line and its ledger fact

Every lifecycle step of a request, attempt, nonce, timeout vote and escrow drain passes through here once; this package alone decides which steps become log lines and which the nonce ledger applies, in the order they happened.

## What it owns

- **`Journal`** — built once in `main.go`, closed by the `journal` shutdown step. Producers call its methods; one consumer goroutine hands each accepted event to the sinks.
- **Two consumer-side sinks.** `logSink` defaults to `devshard/logging`, so `internal/logcapture` sees every line. `ledgerSink` is `*nonces.Recorder`, or nil when nonce accounting is off.
- **Its counters**, returned by `Counts` and exported by `metrics.NewJournalCollector`.

## What it does not own

- **Nothing is persisted.** An event lives in memory between the producer's call and its delivery. At two to three million nonces a day an event row per step would cost about 10 GB a day, so the ledger keeps its per-escrow counters and the logs keep their handler.
- **Metrics stay synchronous.** A recorder in `metrics` is still called by the producer; only lines and ledger facts are queued.
- **It does not classify.** Which counter a nonce lands in is `accounting`'s decision.

## Boundaries

- **Producers never import this package.** `engine`, `scheduler`, `nonces` and `warmup` declare the interface they call; `api` imports it only for `RequestLine`. The composition root adapts the rest (`observers.go`).
- **A disabled ledger is a nil interface, never a typed nil.** `journalSettings` in `observers.go` leaves `Settings.Ledger` unset when the recorder is nil; a nil pointer inside the interface is non-nil to the consumer's check.
- **No sink writes what it is handed.** A race outcome's attempts are the slice `api` reads on the response path.
- **A renderer allocates the fields of every line.** A logger may keep the slice it is given; `logcapture.Recorder` does.

## Two lanes

| Lane | Kinds | Refused when | Counted in |
| --- | --- | --- | --- |
| money (the spec's `record`) | `KindRaceReported`, `KindTimeoutVote`, `KindNonceBurned`, `KindBurnBudgetExhausted`, `KindDiffComposed`, `KindWarmupProbe`, `KindNonceStranded`, `KindHostDiverged`, `KindReplyNotCached` | `MoneyCeiling` (100 000) money events are accepted and not yet delivered | `devshard_gateway_journal_money_refused_total` |
| progress (the spec's `offer`) | every other kind | `ProgressBacklog` (8 192) progress events are accepted and not yet delivered | `devshard_gateway_journal_progress_dropped_total` |

Both lanes share one queue, so a line and a ledger fact keep the order they happened in. A lane releases an event when the consumer has delivered it, not when the consumer takes it out of the queue, so the batch being delivered still counts against its lane. A producer never waits: admission is an append under the journal's mutex, and a refusal is a counter. The queued value stays a few words wide: every payload wider than a few words is copied once by the producer method that receives it, outside the mutex, and queued as a pointer, so neither the append nor a growing queue copies a payload under the mutex. The money ceiling is not back-pressure; it bounds memory when the consumer is stuck, and a refused money-lane event is a ledger fact never applied or a money-path line never written (a stranded nonce, a divergent host, an uncached reply), which is why `Close` returns an error when any were refused. After the batch that follows a progress drop, the consumer writes one Warn line, `journal skipped progress lines`, with `skipped_lines`. That batch may be empty: the consumer does not wait while a drop is still unwritten, so the line reaches the log before `Flush` or `Close` returns. A drop never needs to wake the consumer, because a lane is full only while an event it counts is queued or being delivered, and the consumer sleeps only when neither holds.

## Order and the locks it takes

- **The consumer never holds the journal's mutex while it calls a sink.** It swaps the pending slice out under the mutex and delivers the batch outside it.
- **The mutex guards a slice of small values.** A producer's payload is copied once, outside the mutex, when its method receives it; under the mutex only a pointer and a few scalars are appended, however long the queue grows.
- **A slow ledger delays every line.** Lines and ledger facts share one consumer, so while `Book.Snapshot` holds the book's read lock the consumer waits on its next ledger fact, and progress lines past the backlog are dropped.
- **A line's timestamp is when the consumer wrote it**, not when the step happened, and lines of different kinds can interleave differently than when each producer wrote its own.

## Flush and Close

- **`Flush`** returns once every event accepted before the call has reached the sinks and every drop counted before it has been written in a skipped-lines warning. Tests call it before asserting; a sink must never call it.
- **`Close`** refuses later events, drains what was accepted, writes the skipped-lines warning still owed, stops the consumer, and returns an error naming how many money-lane events were refused over the journal's life. A second call returns at once.
- **An event after `Close`** reaches no sink. It is counted in `devshard_gateway_journal_late_events_total`, and the first of each kind is written straight to the log sink as the Error line `journal received an event after it closed` with its `kind`. Only work a shutdown step abandoned can be late: a timeout vote still posting after the `races` drain ran out of budget, or a warmup goroutine.

## Shutdown

The `journal` step sits between `escrow sessions` and `nonce accounting` (`lifecycle.go`, `shutdownOrder`). Every producer stops above it and the ledger it feeds closes below it. It is not `needsQuiesced`: closing it destroys nothing a running step reads, and a late event is counted rather than lost unseen. The step closes through `closeWithin`: it waits for the drain up to the shutdown budget, and at least one second (`journalCloseFloor`) even when a drain step above spent the budget, then reports the step abandoned, so a sink that never returns cannot keep nonce accounting and the store from closing. A close that returns just as the budget runs out reports its own result, so a money-lane refusal is never replaced by the abandonment.

## Kinds

| Kind | Producer | Ledger | Line |
| --- | --- | --- | --- |
| `KindRaceReported` | `nonceAccountedRaces.RecordRace` (`observers.go`) | `RecordRace` | none |
| `KindTimeoutVote` | `nonceAccountedRaces.RecordTimeout` (`observers.go`) | `RecordTimeout` | written by `engine/settle.go` |
| `KindNonceBurned` | `tracedDispatches.GhostBurned` (`observers.go`) | `RecordGhost` | written by `observers.go` |

## Read next

- [`nonces/README.md`](../nonces/README.md) — the ledger sink.
- [`docs/operations.md`](../docs/operations.md), "Shutdown" and "Metrics".
