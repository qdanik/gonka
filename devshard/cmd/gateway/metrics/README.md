# `metrics` — the Prometheus surface

The only package that knows Prometheus exists.

## What it owns

- **The registry** (`metrics.go`) and every collector: limits, capacity, perf, registry, chain, cache, capture, transport, accounting.
- **Recorders** (`race.go`, `dispatch_recorder.go`, `limit_recorder.go`) — the write side, called by the subsystems that produce the facts.
- **Label discipline** (`labels.go`) — what may become a label and what may not.

## Boundaries worth knowing

- **Family names are frozen as `devshard_*`.** A dashboard and an operator's alert select on these strings; a rename is a silent alert that stops firing.
- **Unbounded labels stay out.** Escrow ids and nonces are deliberately absent — they would grow the series without end. What a metric cannot carry, the JSON accounting API serves instead.
- **A collector reads its source; it does not keep its own copy**, so a gauge cannot disagree with the thing it reports.

## Read next

- [`docs/gateway-operations.md`](../docs/gateway-operations.md), "Metrics" — every family and what it means.
