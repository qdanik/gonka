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

## The gateway limiter's queue

A request that no cap can ever admit — one asking for more input tokens than the model's whole budget — is refused immediately rather than queued, because waiting could not help it.

Everything else queues in arrival order, and the queue is not a delay: a waiter is appended and the queue is swept in the same lock acquisition, so capacity that is already free is handed over at once. Sweeping **skips** a waiter its own model still cannot serve rather than stopping at it, so one saturated model does not block a different one behind it. A freed slot is given to the queue directly under the release's own lock, which is why an arriving request that fits never overtakes a waiter that does not.

Two races at the end of a wait are resolved by whether the waiter is still in the queue. If it was promoted just as its deadline fired, the slot is already its own and it proceeds; if it was promoted for a caller whose context had already been cancelled, the slot is released again rather than leaked.

`AdmissionQueuePerSlot` bounds the queue at that multiple of the model's own concurrency cap; past it the request is refused with `queue_depth` rather than joining a queue it cannot reach the front of within the wait budget.

Releasing leaves an idle model's counter in the map. Deleting it would cost an allocation on the model's next acquire, and a model that goes quiet would vanish from the snapshot — which a reader cannot tell apart from the gateway being gone. The map is bounded by the number of routable models.

## How one model's cap is computed

The configured maxima are **each model's own** budget, not a shared pool, and a per-model override *replaces* the configured maximum rather than narrowing it further.

The concurrency cap is then derived one of two ways:

- When a per-10 000-weight rate and a baseline weight are both present, the cap is `min(limit(currentWeight), limit(baselineWeight))` — the current weight must never lift the cap **above** the baseline-derived one.
- Otherwise the configured maximum is scaled by the capacity scale factor, rounded to nearest rather than floored, and never above the configured maximum itself.

The input-token cap only ever takes the second path.

## The AIMD window

- **Growth is judged on peak in-flight since the last adjustment, not the live count.** The engine releases an attempt's slot in a `defer` and reports its verdict afterwards, so a live read would see the slot already given back and refuse to grow a window that was genuinely saturated. The peak is set when the slot is taken and nothing can undo it, which makes the decision independent of which of the two runs first.
- **A half-open probe gets exactly one try.** Any fault while half-open reopens the cutoff immediately rather than after `AfterFailures` more.
- **A successful probe clears the trip itself**, not just the half-open flag, or the next `Acquire` would re-flag half-open forever.
- **Backoff is `base * 1.6^count` plus up to 20% jitter** (gRPC connection-backoff's `JITTER`), so reopened cutoffs across many hosts do not retry in lockstep. The count stops rising once the backoff saturates at `MaxOpen`, so `1.6^count` cannot overflow the duration.
- **`Available` peeks the admit decision** without touching in-flight, the cutoff, or creating state for a participant never seen before, which is what lets routing ask about a host it has never dispatched to.
- **`Snapshot` is taken under one lock acquisition** and returned in participant/model order, so a report cannot mix two moments.

## The capacity model

| Quantity | Definition |
| --- | --- |
| `weightConcurrencyLimit` | `floor(weight * per10000 / 10000)`, and 0 when either input is non-positive or not finite |
| `scaleFactor` | `clamp(currentAvailable / full, 0, 1)`, and 1.0 when the baseline is non-positive |
| `escrowWeight` | `Σ currentWeight[host] * hostShare[host]` over available hosts |
| `availableShare` | the same sum with every weight taken as one |

`hostShares[host]` is `slots(host, escrow) / totalSlots(host)` across every escrow that host serves. Raw slot counts would count a participant once per escrow it serves instead of splitting it between them.

Three fallbacks decide what a missing view means:

- **A model absent from a *populated* by-model view is served by nobody**, so it scores zero rather than inheriting the generic all-model view. With no by-model view at all, the generic view applies to everything.
- **An empty weight view means the chain named nobody**, not that everybody weighs nothing — a host the chain has reported is a key in the view whatever its weight. When neither view has been observed, escrow scoring falls back to the membership share alone, which serves requests correctly and silently; `WeightsUnobserved` is what makes that state visible.
- **The current and full views fall back to the generic one independently**, so a missing full-by-model entry does not suppress a present current-by-model one.

`ScaleFactor` takes the **effective** blocking state, never the chain's raw one. Relaxed mode is the operator's override of that fact, so a capacity that read the snapshot itself would zero the scale exactly when the override was meant to keep serving — and a zero scale clamps every weight-derived cap to nothing. The composition root in [`main.go`](../main.go) owns that fold.

## Read next

- [`docs/capacity.md`](../docs/capacity.md) — the ladder, the thresholds, and how the three interact.
