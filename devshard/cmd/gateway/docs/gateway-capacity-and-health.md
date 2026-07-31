# Devshard gateway — capacity and host health

Three separate questions, three separate mechanisms:

| Question | Owner |
|---|---|
| How many requests may the *gateway* have in flight, and for which model? | `limits.GatewayLimiter` |
| How many concurrent sends may *this participant* receive? | `limits.ParticipantLimiter` |
| Is this participant an outlier, and what can it not serve? | `perf.Tracker` |

They are deliberately not one thing. The first protects the gateway and respects the network's view of how much of the model's capacity this gateway commands; the second protects the participant from us; the third is an outlier detector plus the sticky record of what a host has proved it cannot do.

A host is removed from a pick by the participant limiter, by the capability flags, or by the ejection verdict — see [Outlier ejection](#outlier-ejection) for what the ejection verdict is capped by before routing honours it.

The design principle across all three is **adaptation instead of punishment**. The legacy gateway quarantined a host for thirty to sixty minutes; here an overloaded host receives less traffic within one round trip and recovers automatically, and the worst case is minutes.

## Capacity: what the chain says this gateway may use

`limits.Capacity` holds the latest chain snapshot and the escrow membership pushed by the registry, and answers two questions.

**Scale factor for a model** — the fraction of the network's reference weight for that model that is currently available to us:

```
scaleFactor(model) = clamp( Σ availableCurrentWeight / Σ fullWeight , 0, 1 )
```

with three fixed answers at the edges (`limits/capacity.go:47-56`, `limits/weights.go:18-24`):

- Requests blocked by the chain phase → **0**.
- The model is not served → **0**.
- No reference weight observed at all → **1.0**. An unobserved baseline means unlimited, not zero.

**Escrow weight** — how much serving weight one escrow commands for a model:

```
escrowWeight(escrow, model) = Σ_participant currentWeight[p] × hostShare[p] × available(p)
```

`hostShare` is a **normalised share**, `slots(p, this escrow) / slots(p, all live escrows)`, computed by the registry (`registry/membership.go:28-41`). The type is a plain float map, so nothing stops a caller passing raw slot counts — and doing so type-checks, works, and silently gives a participant serving three escrows its full weight in each, tripling its apparent capacity so the gateway over-admits. This is the sharpest example in the codebase of an invariant that lives only in prose.

`available` is the participant limiter's non-mutating peek, so AIMD and breaker state feed *back* into capacity weighting and escrow scoring (`main.go:163`).

### Two fail-safes with opposite directions

**Unknown model fails closed.** A model absent from a *populated* per-model weight view is served by nobody and gets zero — it does not inherit the all-model view (`limits/capacity.go:103-114`). With no per-model view at all (cold start, or an older chain response), the generic view still applies to everything, and the two sides fall back independently so a missing full-weight entry does not suppress a present current-weight one. This matters because `model` is unvalidated client input: without the guard an unrecognised model string inherited full network capacity.

**Unobserved weights fail open.** When neither weight view has any key at all, escrow scoring falls back to the availability-filtered membership share instead of zero (`limits/capacity.go:58-76`). Zero would make every escrow score as unusable, so every request in the boot window would be refused. The test is *emptiness*, not staleness: a chain-reported participant is a key in the view whatever its weight, so an empty view means the chain named nobody.

That fallback serves requests correctly and silently, which is its own hazard — an operator sees traffic flowing and cannot tell routing is running blind. So `WeightsUnobserved(model)` is derived from the *same two predicates the fallback branch itself reads* and published as `devshard_gateway_capacity_weights_unobserved_by_model`. A gauge computed from the same predicates cannot drift from the behaviour it reports.

## The gateway limiter

Two dimensions — in-flight requests and in-flight input tokens — at two scopes.

**The configured maxima are process-wide.** A model without an override *shares* them rather than receiving a fresh quota; a per-model override can only narrow (`limits/gateway.go:227-228`). This was a real hole: without a global counter, N distinct model strings meant N × the per-model cap in flight, and the limiter was bypassable by cycling model names.

**Effective limits come from weight when weight is known** (`limits/gateway.go:323-341`). With a per-10 000-weight rate configured and a baseline weight observed, the configured absolute maximum is ignored and the limit becomes the minimum of the current-weight-derived and baseline-derived caps — current weight can lower the cap, never lift it above the baseline. Otherwise the configured maximum is scaled by the scale factor. A NaN or out-of-range scale clamps to **zero**, never to one: a corrupted scale must not grant unlimited capacity.

A scale factor of zero therefore means an immediate 429 rather than a queue — which is what happens while the chain is blocking requests, if a request somehow reaches the limiter at all.

The capacity values are passed **in** to each acquire and never re-read from a snapshot inside the limiter, and the contract on that parameter is that they are already availability- and block-filtered (`limits/gateway.go:29-36`). Only the composition root satisfies it.

### The queue

If the request does not fit, it joins a FIFO queue and its own goroutine parks in a `select` on a channel and a timer. There is no per-waiter goroutine and no broadcast on release: a releasing request decrements the counters **and hands the freed capacity directly to the queued waiter, inside the same lock hold** (`limits/gateway.go:55-56, 248-258`). Admission is transferred, not re-contested, so an admitted waiter never competes with newly arriving requests.

The earlier design used a condition variable and woke every waiter on each release; measured, a single waiter was overtaken 1.67 million times in 1.2 seconds and timed out into a 429 while newer arrivals sailed past.

Two rules govern the promotion sweep (`limits/gateway.go:195-217`):

- A waiter blocked only by *its own model's* override is skipped, so one model cannot stall unrelated models.
- A waiter blocked by the *global* budget stops the sweep entirely, so shared capacity stays strictly first-come-first-served.

A new arrival that fits may pass a queued waiter without unfairness, because a queued waiter is by definition one that current capacity cannot serve.

Both races at the edges are handled explicitly. If the timer fires but the waiter was already promoted, the acquire *succeeds* — the slot is already ours. If the context is cancelled but the waiter was already promoted, the slot was handed to a caller that is gone, so it is released (`limits/gateway.go:164-179`).

`Reconfigure` swaps the caps and sweeps the queue, so a widened limit reaches a waiting request immediately rather than at the next release.

## The participant limiter: AIMD plus a breaker

State is per `{participant, model}`.

**Additive increase, multiplicative decrease.** A success widens the window by one, up to the maximum — but only when the host is actually being used, `inflight ≥ window/2`, so an idle host does not accumulate an imaginary window. A 429 or 503 halves it, with a floor of one, unconditionally (`limits/participant.go:221-235`).

Only host-attributable verdicts move the window. A model outcome — an empty stream, a burn-empty, an error stream, a capability refusal — returns before the lock is even taken and does not create state (`limits/participant.go:18, 210-212`). "An empty stream is what the model produced, not what the host failed to carry."

**The breaker** trips after a configured number of consecutive transport faults, or immediately if a half-open probe fails — a probe gets exactly one try. Backoff is `base × 1.6^count` with up to 20% jitter, clamped to the maximum; the count stops incrementing once saturated so the exponent cannot overflow (`limits/participant.go:236-251`). The jitter is gRPC's connection-backoff constant, and it exists so that breakers that opened together do not retry in lockstep.

Recovery walks the ladder back down: a success while half-open clears the trip *and* decrements the backoff count. The subtle part is that it must clear the open-until timestamp, not just the probe flag — any host with a non-zero open-until is treated as half-open, so clearing only the flag pins a recovered host at window one forever (`limits/participant.go:228`).

**`Available` is a peek, not an authority.** It evaluates the same predicate without mutating in-flight, without setting the half-open flag and without creating state for an unseen participant (`limits/participant.go:110-120`). It exists because routing needs a cheap pre-filter where a stale answer costs nothing. Using it *as* the authority is what produced the admission time-of-check-to-time-of-use hole described in [gateway-invariants.md](./gateway-invariants.md); the acquire that authorises a send happens inside the same step that commits the nonce.

The call order per attempt is **acquire, then result, then release** — the AIMD utilisation gate reads the in-flight count before release frees it, and any other order silently changes the increase rule.

**Manual reset.** `POST /v1/admin/participants/unquarantine` clears every model's breaker for one participant and restores the initial window, and reports "not found" for a participant the gateway is not tracking. In-flight counts are deliberately left alone: they count attempts still running, not penalty.

## Outlier ejection

`perf.Tracker` answers two questions in O(1) with no lock: **is this participant withheld from routing** (`Ejected`), and **did the detector want it out at all** (`Degraded`). They differ only by the pool-wide cap, and each has exactly one job.

**`Ejected` is a routing gate.** It is one of the scheduler's six host gates — excluded, proof-of-compute-required, throttled, ejected, state-blocked and capability — so a host it names receives no request and its nonces are burned as `participant_ejected_no_send` ghosts (`scheduler/match.go`, `scheduler/ghost.go`). It also drives the `devshard_gateway_host_ejected` gauge and one branch of the limiter-verdict ladder, where a `Stalled` attempt is charged to the host instead of excused as a model outcome, but only while that host is ejected (`engine/outcome.go`).

**`Degraded` is why the gate is not the whole story.** The cap below refuses to honour an ejection once too many of a model's hosts are failing at once, which is exactly the moment the gate stops protecting anything: those hosts stay in rotation. `Degraded` reports the verdict *before* the cap, and the race reads it for one decision — a primary the detector wanted out starts its second attempt immediately, under `primary_degraded`, rather than waiting out the receipt or first-token deadline. That hedge is bounded by the attempt budget, so a correlated outage costs at most one extra attempt per request and never an unbounded retry storm.

**What it tracks is health, not latency.** A sample is three fields: participant, model, and whether the host was responsive (`perf/sample.go:3-7`). There is no latency ring, no percentile and no host score in this package; response timings are recorded by the metrics layer from the race outcome, and escrow selection scores on in-flight load over chain weight. The only *exponentially* decayed quantities are the counts of successes and failures; the ejection count decays too, but in whole rungs rather than continuously.

**Ejection triggers** (`perf/ejection.go:31-34`): a run of consecutive failures, or a failure rate above the threshold once the decayed volume is large enough. The minimum-volume gate is why a quiet host is not ejected by one bad request.

**The ladder.** Only a *fresh* trigger starts an ejection — an already-ejected host rides out its current timer rather than having it pushed back. Each fresh trigger lengthens the next ejection linearly in the ejection count, capped, and resets the outcome counters so the rate restarts from zero. The count decays back one rung per full healthy window, with the anchor advancing so the ladder cannot unwind faster than that.

**The pool-wide cap.** Envoy's max-ejection-percent applies per model: at most `min(fraction × known hosts, known hosts − minimum available)` ejections are honoured, resolved by sorting participant keys. Ejections beyond the cap keep their timers running but are absent from the routing view, so they are `Degraded` and not `Ejected` (`perf/tracker.go`). This is what makes the routing gate safe to honour: a correlated outage can never remove a whole model's fleet from routing, and the hosts it leaves in rotation are the ones the race hedges instead.

**Why it is lock-free.** The shape is sized for a per-host, per-admission read: at five hundred hosts the old scan cost 2.35 ms per request and roughly 1 300 acquisitions of one global mutex. The tracker publishes two atomic maps of keys to expiry times, one capped for routing and one uncapped; a read is an atomic load, a map lookup and a time comparison, with no lock. The cap and the tie-break are resolved once, at rebuild time, and each entry carries its own expiry so ageing out needs no rebuild at all. The rebuild itself is conditional — only when the membership the cap is computed over actually moved (`perf/tracker.go:25-27, 55-59, 191-200`).

Stale host state is swept at most once per tenth of the staleness window, because entries age out over minutes and scanning every host on every sample costs O(hosts) under the global lock for nothing.

**Capability flags** — a host's known context limit and whether it rejects tool use — are per participant, sticky, and cleared only by process restart. They feed the scheduler's capability filter, which is how a retry skips every host already known to be too small.

## Nothing here is persisted

Every restart starts clean: no ejections, no capability flags, every AIMD window at its initial value, every breaker closed, every decayed counter at zero. That is a deliberate divergence from the legacy gateway, argued in [gateway-non-goals.md](./gateway-non-goals.md): minute-scale backoff self-heals faster than replaying stale penalties is worth. The cost is that a genuinely bad host gets one free window after every deploy.

The one host judgement that *is* persisted is the operator's manual suspicious-host pin, and for the opposite reason — a pin the gateway acts on but forgets on restart is a state an operator cannot see. The store is written before the in-memory copy (`main.go:748-751`).

Two asymmetries worth knowing, neither of which is stated in the code:

- The participant limiter's `{participant, model}` state map never evicts; only the performance tracker ages hosts out. The participant set is the bounded validator set, so this is survivable, but the two packages differ.
- Ejection thresholds are re-read from configuration on every sample, so they hot-reload — but a host's decay half-life is captured when the host is first seen, so a changed half-life applies only to hosts seen afterwards.

## Configuration

| Knob | Default | Effect |
|---|---|---|
| `max_concurrent_requests` | 512 | Process-wide in-flight request cap, scaled by capacity. |
| `max_input_tokens_in_flight` | 0 (unlimited) | Process-wide input-token budget, scaled by capacity. |
| `max_concurrent_requests_per_10000_weight` | 5.0 | Weight-derived cap; when set with an observed baseline it replaces the absolute cap. |
| `poc_max_concurrent_requests_per_10000_weight` | 10.0 | The same, used while the chain reports requests blocked. |
| `acquire_wait_ms` | 500 | Bounded queue wait before a 429. |
| `aimd_initial_window` / `aimd_max_window` | 4 / 64 | Per-participant concurrency window bounds. |
| `breaker_trip_threshold` | 3 | Consecutive transport faults before the breaker opens. |
| `breaker_base_open_ms` / `breaker_max_open_ms` | 5 000 / 300 000 | Backoff ladder bounds. The maximum must not exceed the performance ejection maximum, so ejection stays the dominant authority. |
| `perf_consecutive_fail_threshold` | 5 | Consecutive-failure ejection trigger. |
| `perf_failure_rate_threshold` / `perf_failure_rate_min_volume` | 0.15 / 20 | Rate-based ejection trigger and its volume gate. |
| `perf_ejection_base_seconds` / `perf_ejection_max_seconds` | 30 / 600 | Ejection duration ladder. |
| `perf_max_ejection_fraction` / `perf_min_available_hosts` | 0.5 / 1 | Pool-wide ejection cap, and the reason the routing gate cannot empty a model's fleet. |
| `perf_host_staleness_seconds` | 3 600 | When an unseen host is forgotten. |
| `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` | 600 | Half-life of the decayed success and failure counters. |

The rows down to `breaker_max_open_ms` are admin overrides, changeable at run time without a redeploy. The `perf_*` rows are **not**: they are neither overrides nor environment variables, only compile-time defaults, and the snake_case names above are the spellings the boot-time validator uses in its error messages, not knobs you can set. `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` is the one performance value with an environment variable, and it is read once at boot. Retuning ejection therefore means a new binary.

The default input-token budget of zero means unlimited, which is worth an operator's attention: with million-token contexts it is the only thing between concurrency and memory exhaustion, and the body-size cap deliberately does not throttle load.
