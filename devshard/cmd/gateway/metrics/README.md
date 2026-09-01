# `metrics` — the Prometheus surface

The only package that knows Prometheus exists.

## What it owns

- **The registry** (`metrics.go`) and every collector: limits, capacity, perf, registry, chain, cache, capture, transport, accounting.
- **Recorders** (`race.go`, `dispatch_recorder.go`, `limit_recorder.go`) — the write side, called by the subsystems that produce the facts.
- **Label discipline** (`labels.go`) — what may become a label and what may not.

## Boundaries

- **Family names are frozen as `devshard_*`.** A dashboard and an operator's alert select on these strings; a rename is a silent alert that stops firing.
- **Unbounded labels stay out.** Escrow ids and nonces are absent — they would grow the series without end. What a metric cannot carry, the JSON accounting API serves instead.
- **A collector reads its source; it does not keep its own copy**, so a gauge cannot disagree with the thing it reports.

## Cardinality in practice

The three places an unbounded label could get in, and what stops it:

- **The route label is a pattern, never a raw path.** `InstrumentRoute` takes the pattern (`/devshard/{id}/v1/status`) as a fixed string; passing `r.URL.Path` would mint a series per request.
- **The method label is folded to a known set.** `net/http` hands the handler any RFC 7230 token the client sent, so labelling with `r.Method` directly would let one unauthenticated caller mint a permanent series per probe. Anything outside the seven standard methods becomes `other`.
- **Escrow series are deleted when the escrow retires.** Escrow ids are monotonic and never reused, so without `EscrowRetired` each rotation would leave one nonce-holds series, one budget series and one ghost series per reason, permanently. The sibling registry collector avoids the same growth differently — it rebuilds from the live escrow set on every scrape, so it has nothing to delete.

## Histogram buckets

Two ladders, and both are sized by what they must be able to *report*, not by what they expect:

- **`latencyBuckets`** — nineteen exponential buckets from 10 ms reach 2621 s, past the 2400 s drain timeout. A quantile cannot report above the highest finite bound, so a short ladder would silently pin p95 there instead of showing the slowest attempt the engine actually permits.
- **`chunkGapBuckets`** — a shorter, finer ladder that still reaches past the stall timeout. `latencyBuckets` stop at 20 s, so every stall would land in `+Inf` — which is the one case these exist to separate.

## How a collector reaches its source

- **Collectors are registered at construction time**, where a duplicate family is a wiring bug that must stop the process rather than silently drop a series.
- **Some sources are typed as plain integers rather than a shared struct** (`CacheSource`, `CaptureSource`). [`api`](../api/)'s own tests read this package, so importing `api` back here would close a cycle.
- **`RegistrySources` takes functions rather than objects**, which is what keeps [`registry`](../registry/) free of an edge to [`limits`](../limits/).
- **In-flight load is reported once per participant.** [`perf`](../perf/) counts it per participant while ejection is per participant *and* model, so emitting it inside the per-model loop would be a duplicate series.
- **The limits collector folds the configured model list into the models with live traffic**, so a model with no in-flight request still reports its capacity, and no model is emitted twice.

The response writer that records a status also forwards `Flush`, so streaming handlers keep working when wrapped, and exposes `Unwrap` for the standard-library calls that reach the underlying writer by type assertion.

## Read next

- [`docs/operations.md`](../docs/operations.md), "Metrics" — every family and what it means.
