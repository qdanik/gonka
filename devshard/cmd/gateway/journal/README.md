# `journal` — one ordered path to a log line or a ledger fact

Race outcomes and race trace steps, request records, limiter refusals and uncached replies, burns and burn-budget trips, timeout votes, composed diffs' ledger facts, and warmup probes all pass through here, in the order they happened; this package decides which of those become log lines and which the nonce ledger applies.

## What it owns

- **`Journal`** — built once in `main.go`, closed by the `journal` shutdown step. Producers call its methods; one consumer goroutine hands each accepted event to the sinks.
- **Two consumer-side sinks.** `logSink` defaults to `devshard/logging`, so `internal/logcapture` sees every line. `ledgerSink` is `*nonces.Recorder`, or nil when nonce accounting is off.
- **Its counters**, returned by `Counts` and exported by `metrics.NewJournalCollector`.

## What it does not own

- **Nothing is persisted.** An event lives in memory between the producer's call and its delivery. At two to three million nonces a day an event row per step would cost about 10 GB a day, so the ledger keeps its per-escrow counters and the logs keep their handler.
- **Metrics stay synchronous.** A recorder in `metrics` is still called by the producer; only lines and ledger facts are queued.
- **It does not classify.** Which counter a nonce lands in is `accounting`'s decision.
- **The sweep writes the ledger directly.** `Recorder.sweep` (`nonces/recorder.go`) calls `Book.OpenEscrow`, `MarkFinished`, the `Observe*` methods and `RetireEscrow` on its own goroutine, never through this package.
- **The warmup's `OpenEscrow` is a direct write too.** `Prober.openLedger` (`warmup/warmup.go`) calls it itself, because the open must land before the probe reaches the ledger through the journal.
- **A few lifecycle lines are still written by their own packages.** The journal is not yet the only writer of lifecycle lines: the escrow drain lines in `registry/`, the dispatcher's excluded-host line (`scheduler/dispatcher_queue.go`), and the crown-strike lines (`engine/engine.go`) are examples.

## Boundaries

- **Producers never import this package.** `engine`, `scheduler`, `nonces` and `warmup` declare the interface they call; `api` imports it only for `RequestLine`. The composition root adapts the rest (`observers.go`).
- **`DiffFact` is declared in `accounting`** and aliased here, so `*nonces.Recorder` satisfies `ledgerSink` without importing this package.
- **A disabled ledger is a nil interface, never a typed nil.** `journalSettings` in `observers.go` leaves `Settings.Ledger` unset when the recorder is nil; a nil pointer inside the interface is non-nil to the consumer's check.
- **No sink writes what it is handed.** A race outcome's attempts are the slice `api` reads on the response path.
- **A renderer allocates the fields of every line.** A logger may keep the slice it is given; `logcapture.Recorder` does.
- **A race step is already a copy.** The coordinator fills `engine.RaceStep` on its own goroutine, the terminal it would log already raced and the phase mark already computed, so rendering reads no coordinator state. `RecordStep` queues a pointer to its own copy of the step, so the journal's mutex never copies an attempt outcome.
- **`api` reads the client stream before it hands the line over.** `RequestLine.Bytes` and `Terminated` are taken on the handler goroutine, because attempt goroutines can still write the stream after the handler returns.

## Two lanes

| Lane | Kinds | Refused when | Counted in |
| --- | --- | --- | --- |
| money (the spec's `record`) | `KindRaceReported`, `KindTimeoutVote`, `KindNonceBurned`, `KindBurnBudgetExhausted`, `KindDiffComposed`, `KindWarmupProbe`, `KindNonceStranded`, `KindHostDiverged`, `KindReplyNotCached`, `KindRequestFinished` | `MoneyCeiling` (100 000) money events are accepted and not yet delivered | `devshard_gateway_journal_money_refused_total` |
| progress (the spec's `offer`) | every other kind | `ProgressBacklog` (8 192) progress events are accepted and not yet delivered | `devshard_gateway_journal_progress_dropped_total` |

Both lanes share one queue, so a line and a ledger fact keep the order they happened in. A lane releases an event when the consumer has delivered it, not when the consumer takes it out of the queue, so the batch being delivered still counts against its lane. A producer never waits: admission is an append under the journal's mutex, and a refusal is a counter. The queued value stays a few words wide: every payload wider than a few words is copied once by the producer method that receives it, outside the mutex, and queued as a pointer, so neither the append nor a growing queue copies a payload under the mutex. The money ceiling is not back-pressure; it bounds memory when the consumer is stuck, and a refused money-lane event is a ledger fact never applied or a money-path line never written (a stranded nonce, a divergent host, an uncached reply, a finished request's own record), which is why `Close` returns an error when any were refused. After the batch that follows a progress drop, the consumer writes one Warn line, `journal skipped progress lines`, with `skipped_lines`. That batch may be empty: the consumer does not wait while a drop is still unwritten, so the line reaches the log before `Flush` or `Close` returns. A drop never needs to wake the consumer, because a lane is full only while an event it counts is queued or being delivered, and the consumer sleeps only when neither holds.

## Order and the locks it takes

- **The consumer never holds the journal's mutex while it calls a sink.** It swaps the pending slice out under the mutex and delivers the batch outside it.
- **The mutex guards a slice of small values.** A producer's payload is copied once, outside the mutex, when its method receives it; under the mutex only a pointer and a few scalars are appended, however long the queue grows.
- **The session diff observer calls in under the session lock** (`devshard/user/session.go`, `SetDiffObserver`). `DiffComposed` counts the diff's ledger facts before it allocates, copies them into `DiffFact` values and appends one small event; the book's own lock is taken later, on the consumer, never under the session's. The observer's copy of the diff is its one allocation; `DiffComposed` adds none for a diff with no verdict and no applied timeout, and queues nothing.
- **A slow ledger delays every line.** Lines and ledger facts share one consumer, so while `Book.Snapshot` holds the book's read lock the consumer waits on its next ledger fact, and progress lines past the backlog are dropped.
- **A line's timestamp is when the consumer wrote it**, not when the step happened, and lines of different kinds can interleave differently than when each producer wrote its own.

## Flush and Close

- **`Flush`** returns once every event accepted before the call has reached the sinks and every drop counted before it has been written in a skipped-lines warning. Tests call it before asserting; a sink must never call it.
- **`Close`** refuses later events, drains what was accepted, writes the skipped-lines warning still owed, stops the consumer, and returns an error naming how many money-lane events were refused over the journal's life. A second call returns at once.
- **An event after `Close`** reaches no sink. It is counted in `devshard_gateway_journal_late_events_total`, and the first of each kind is written straight to the log sink as the Error line `journal received an event after it closed` with its `kind`. Only work still running when the journal closes can be late: a timeout vote still posting after the `races` drain ran out of budget, an in-flight warmup probe, or a handler still running after the listener's shutdown.

## Shutdown

The `journal` step sits between `escrow sessions` and `nonce accounting` (`lifecycle.go`, `shutdownOrder`). Every producer stops above it and the ledger it feeds closes below it. It is not `needsQuiesced`: closing it destroys nothing a running step reads, and a late event is counted rather than lost unseen. The step closes through `closeWithin`: it waits for the drain up to the shutdown budget, and at least one second (`journalCloseFloor`) even when a drain step above spent the budget, then reports the step abandoned, so a sink that never returns cannot keep nonce accounting and the store from closing. A close that returns just as the budget runs out reports its own result, so a money-lane refusal is never replaced by the abandonment.

## Kinds

| Kind | Producer | Ledger | Line |
| --- | --- | --- | --- |
| `KindRaceReported` | `nonceAccountedRaces.RecordRace` (`observers.go`) | `RecordRace` | none |
| `KindTimeoutVote` | `nonceAccountedRaces.RecordTimeout` for a race vote; `probeVotes.RecordTimeout` through `RecordProbeTimeout` for a warmup vote (`observers.go`) | `RecordTimeout` | `timeout vote failed`, Warn (`render_money.go`): a race vote whose action is `failed` for any reason but `escrow_gone_from_hosts`, which the escrow's own line already reports once. A warmup vote writes none; the warmup writes its own line |
| `KindNonceBurned` | `tracedDispatches.GhostBurned` (`observers.go`) | `RecordGhost` | `nonce burned for nobody`, Warn (`render_money.go`) |
| `KindBurnBudgetExhausted` | `tracedDispatches.BurnBudgetExhausted` (`observers.go`) | none | `escrow stopped burning nonces at its budget`, Warn (`render_money.go`) |
| `KindDiffComposed` | the diff observer `nonces.Recorder.watchDiffs` installs, through `DiffComposed`, under the session lock | `RecordDiffFacts` | none |
| `KindWarmupProbe` | `warmup.Prober.record` through `ProbeRecorded` | `RecordProbe` | `escrow warmup could not settle its nonce`, Warn (`render_money.go`), when `RecordProbe` returns the book's refusal |
| `KindNonceStranded` | `raceCoordinator.strand` through `RecordStep` (`engine/race.go`) | none | `nonce stranded`, Warn (`render_race.go`) |
| `KindHostDiverged` | `raceCoordinator.stateDiverged` through `RecordStep` (`engine/report.go`) | none | `host blocked for state divergence` or `host rewound for state divergence`, Warn (`render_race.go`) |
| `KindNonceCommitted` | `raceCoordinator.launch` through `RecordStep` (`engine/pick.go`) | none | `nonce committed`, Info (`render_race.go`) |
| `KindEscalationUnfilled` | `raceCoordinator.reportUnfilledPick` through `RecordStep` (`engine/pick.go`) | none | `escalation unfilled`, Info (`render_race.go`) |
| `KindAttemptCrowned` | `raceCoordinator.crownWinner` through `RecordStep` (`engine/crown.go`) | none | `attempt crowned`, Info (`render_race.go`) |
| `KindAttemptFinished` | `raceCoordinator.complete` through `RecordStep` (`engine/report.go`) | none | `attempt finished`, or `attempt finished with no outcome` when the attempt reported nothing, Info (`render_race.go`) |
| `KindReplyNotCached` | `Server.chat` through `ReplyNotCached` (`api/routes.go`) | none | `a host stopped mid-answer: reply served, not cached`, Warn (`render_request.go`) |
| `KindRequestFinished` | `Server.finishRequest` (`api/finish.go`) and the cache hit in `Server.chat` (`api/routes.go`), through `RequestFinished` | none | `request finished` (`render_request.go`): Info when the request went out clean, Warn with a race or delivery error; a cache hit writes the short shape with `outcome` `cache_hit` |
| `KindRequestThrottled` | `Server.chat` through `RequestThrottled` (`api/routes.go`) | none | `gateway limiter turned a request away`, Warn (`render_request.go`) |

## Absent subjects are omitted

A key is written only when its subject exists. `request finished` carries `escrow` only when the race picked one, `host` only for the crowned attempt, and `hosts` — every host tried, comma-separated — only when attempts ran and nobody was crowned. A burn carries no `nonce` when the session committed none, which only a session test double does. An empty value reads as a subject with no name, and a query for it matches every line that never had one.

## Which request a burn and a vote belong to

A burn names the request it was spent during under `burned_during_request`: the request a refused slot was meant for, otherwise the oldest request still waiting in the dispatcher's queue when a drain burned the nonce, or the request whose assignment arrived after it had left. The key is deliberately not `request`, because the nonce served nobody and a search for one request's own lines must not return it. Neither id is ever a metric label.

## Read next

- [`nonces/README.md`](../nonces/README.md) — the ledger sink.
- [`docs/operations.md`](../docs/operations.md), "Shutdown" and "Metrics".
