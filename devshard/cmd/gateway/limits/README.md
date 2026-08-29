# `limits` — who gets admitted, and how many at once

Three limiters, each answering a different question.

## What it owns

- **The gateway limiter** (`gateway.go`) — a FIFO admission queue over concurrent requests and in-flight input tokens, per model. Refuses with a typed rejection that names which cap turned the request away, so an operator is not left a wall of identical statuses.
- **The participant limiter** (`participant.go`) — a per-host AIMD concurrency window with a circuit breaker. It narrows on host-attributable failures and widens on success, and half-opens after a cutoff to admit one real request rather than waiting for a probe.
- **The capacity model** (`capacity.go`, `weights.go`) — scales the caps by the host weight the chain reports for each model, so a shard that has lost half its hosts admits proportionally less.

## Boundaries worth knowing

- **Zero means unlimited**, for both the concurrency cap and the token budget. With `max_concurrent_requests` unset, the effective cap comes from the per-10 000-weight rate instead.
- **Only host-attributable verdicts move the window.** An empty stream or a model refusal is what the model produced, not what the host failed to carry, and narrowing for it would penalise the wrong party.
- **A corrupted capacity scale fails closed.** A NaN must not be read as unlimited capacity.

## Read next

- [`docs/gateway-capacity-and-health.md`](../docs/gateway-capacity-and-health.md) — the ladder, the thresholds, and how the three interact.
