# Devshard gateway — capacity and host health

Three separate questions, three separate mechanisms:

| Question | Owner |
|---|---|
| How many requests may the *gateway* have in flight, and for which model? | `limits.GatewayLimiter` |
| How many concurrent sends may *this participant* receive? | `limits.ParticipantLimiter` |
| Is this participant an outlier, and what can it not serve? | `perf.Tracker` |

They are deliberately not one thing. The first protects the gateway and respects the network's view of how much of the model's capacity this gateway commands; the second protects the participant from the gateway; the third is an outlier detector plus the sticky record of what a host has proved it cannot do.

A host is removed from a pick by the participant limiter, by the capability flags, or by the ejection verdict — see [Outlier ejection](#outlier-ejection) for what the ejection verdict is capped by before routing honours it.

The design principle across all three is **adaptation instead of punishment**. The legacy gateway quarantined a host for thirty to sixty minutes; here an overloaded host receives less traffic within one round trip and recovers automatically, and the worst case is minutes.

## Capacity: what the chain says this gateway may use

`limits.Capacity` holds the latest chain snapshot and the escrow membership pushed by the registry, and answers two questions.

**Scale factor for a model** — the fraction of the network's reference weight for that model that is currently available to this gateway:

```
scaleFactor(model) = clamp( Σ availableCurrentWeight / Σ fullWeight , 0, 1 )
```

with three fixed answers at the edges (`limits/capacity.go`, `Capacity.ScaleFactor`; `limits/weights.go`, `scaleFactor`):

- Requests blocked by the chain phase → **0**.
- The model is not served → **0**.
- No reference weight observed at all → **1.0**. An unobserved baseline means unlimited, not zero.

**Escrow weight** — how much serving weight one escrow commands for a model:

```
escrowWeight(escrow, model) = Σ_participant currentWeight[p] × hostShare[p] × available(p)
```

`hostShare` is a **normalised share**, `slots(p, this escrow) / slots(p, all live escrows)`, computed by the registry (`registry/membership.go`, `hostShares`). The type is a plain float map, so nothing stops a caller passing raw slot counts — and doing so type-checks, works, and silently gives a participant serving three escrows its full weight in each, tripling its apparent capacity so the gateway over-admits. This is the sharpest example in the codebase of an invariant that lives only in prose.

`available` is the participant limiter's non-mutating peek, so AIMD and breaker state feed *back* into capacity weighting and escrow scoring (`main.go`, `compose`, which hands `ParticipantLimiter.Available` to `limits.NewCapacity`).

### Two fail-safes with opposite directions

**Unknown model fails closed.** A model absent from a *populated* per-model weight view is served by nobody and gets zero — it does not inherit the all-model view (`limits/capacity.go`, `Capacity.modelServedLocked`). With no per-model view at all (cold start, or an older chain response), the generic view still applies to everything, and the two sides fall back independently so a missing full-weight entry does not suppress a present current-weight one (`limits/capacity.go`, `Capacity.currentWeightsLocked` and `Capacity.fullWeightsLocked`). This matters because `model` is unvalidated client input: without the guard an unrecognised model string inherited full network capacity.

**Unobserved weights fail open.** When neither weight view has any key at all, escrow scoring falls back to the availability-filtered membership share instead of zero (`limits/capacity.go`, `Capacity.EscrowWeight`). Zero would make every escrow score as unusable, so every request in the boot window would be refused. The test is *emptiness*, not staleness: a chain-reported participant is a key in the view whatever its weight, so an empty view means the chain named nobody.

That fallback serves requests correctly and silently, which is its own hazard — an operator sees traffic flowing and cannot tell routing is running blind. So `WeightsUnobserved(model)` is derived from the *same two predicates the fallback branch itself reads* and published as `devshard_gateway_capacity_weights_unobserved_by_model`. A gauge computed from the same predicates cannot drift from the behaviour it reports.

## The gateway limiter

Two dimensions — in-flight requests and in-flight input tokens — at two scopes.

**The configured maxima are each model's own budget.** Two models under `max_concurrent_requests: 512` get 512 each, not 512 between them, and a per-model override replaces the configured maximum for its model rather than narrowing it further (`limits/gateway.go`, `GatewayLimiter.admissionFor`). This is forced by the dimension the cap is derived from: the effective limit below comes from *one model's* chain weight, so charging it against another model's in-flight count would make the enforced cap depend on which model's request happened to arrive first. Nothing here bounds the process across models — the escrow registry does, by rejecting an unserved model before it can reach the limiter (`api/routes.go`, `Server.chat` and `Server.routableModel`), so the model set is the operator's and not the client's, and cycling model strings mints nothing.

**Effective limits come from weight when weight is known** (`limits/gateway.go`, `effectiveConcurrencyLimit`). With a per-10 000-weight rate configured and a baseline weight observed, the configured absolute maximum is ignored and the limit becomes the minimum of the current-weight-derived and baseline-derived caps — current weight can lower the cap, never lift it above the baseline. Otherwise the configured maximum is scaled by the scale factor, **rounded to nearest**: flooring would take a configured cap of 1 to 0 at any partial capacity, refusing every request for that model instead of degrading it. A NaN or out-of-range scale clamps to **zero**, never to one: a corrupted scale must not grant unlimited capacity (`limits/gateway.go`, `scaleClamp` and `clampUnit`).

A scale factor of zero therefore means an immediate 429 rather than a queue — which is what happens while the chain is blocking requests, if a request somehow reaches the limiter at all.

The capacity values are passed **in** to each acquire and never re-read from a snapshot inside the limiter, and the contract on that parameter is that they are already availability- and block-filtered (`limits/gateway.go`, `ModelCapacity`). Only the composition root satisfies it.

### The queue

If the request does not fit, it joins a FIFO queue and its own goroutine parks in a `select` on a channel and a timer. There is no per-waiter goroutine and no broadcast on release: a releasing request decrements the counters **and hands the freed capacity directly to the queued waiter, inside the same lock hold** (`limits/gateway.go`, `waiter` and `GatewayLimiter.ReleaseForModel`). Admission is transferred, not re-contested, so an admitted waiter never competes with newly arriving requests.

The earlier design used a condition variable and woke every waiter on each release; measured, a single waiter was overtaken 1.67 million times in 1.2 seconds and timed out into a 429 while newer arrivals sailed past.

The promotion sweep walks the queue in arrival order and skips — rather than stops at — a waiter its own model still cannot serve (`limits/gateway.go`, `GatewayLimiter.promoteLocked`). Admission is therefore first-come-first-served within a model, and a saturated model cannot stall the others, which is the whole point of the budgets being per model.

A new arrival that fits may pass a queued waiter without unfairness, because a queued waiter is by definition one that current capacity cannot serve.

Both races at the edges are handled explicitly. If the timer fires but the waiter was already promoted, the acquire *succeeds* — the slot is already held, and refusing it would leak the transfer. If the context is cancelled but the waiter was already promoted, the slot was handed to a caller that is gone, so it is released (`limits/gateway.go`, the `select` in `GatewayLimiter.AcquireForModel`).

`Reconfigure` swaps the caps and sweeps the queue, so a widened limit reaches a waiting request immediately rather than at the next release.

## The participant limiter: AIMD plus a breaker

State is per `{participant, model}`.

**Additive increase, multiplicative decrease.** A success widens the window by one, up to the maximum — but only when the host is actually being used, so an idle host does not accumulate an imaginary window. The utilisation test is `peakInflight ≥ window/2`, where the peak is the highest concurrent count the host has reached since the last widening; a widening resets it to the live count so the next rung has to be earned again. A 429 or 503 halves the window, with a floor of one, unconditionally (`limits/participant.go`, `ParticipantLimiter.OnResult`).

Only host-attributable verdicts move the window. A model outcome — an empty stream, a burn-empty, an error stream, a capability refusal — returns before the lock is even taken and does not create state (`limits/participant.go`, the `Verdict` constants and the `ModelOutcome` early return in `ParticipantLimiter.OnResult`). An empty stream is what the model produced, not what the host failed to carry, so narrowing the host's window for it would penalise the wrong party.

**The breaker** trips after a configured number of consecutive transport faults, or immediately if a half-open probe fails — a probe gets exactly one try. Backoff is `base × 1.6^count` with up to 20% jitter, clamped to the maximum; the count stops incrementing once saturated so the exponent cannot overflow (`limits/participant.go`, the `TransportFault` branch of `ParticipantLimiter.OnResult`). The jitter is gRPC's connection-backoff constant, and it exists so that breakers that opened together do not retry in lockstep.

Recovery walks the ladder back down: a success while half-open clears the trip *and* decrements the backoff count. The subtle part is that it must clear the open-until timestamp, not just the probe flag — any host with a non-zero open-until is treated as half-open, so clearing only the flag pins a recovered host at window one forever (`limits/participant.go`, the `Success` branch of `ParticipantLimiter.OnResult`).

**`Available` is a peek, not an authority.** It evaluates the same predicate without mutating in-flight, without setting the half-open flag and without creating state for an unseen participant (`limits/participant.go`, `ParticipantLimiter.Available`). It exists because routing needs a cheap pre-filter where a stale answer costs nothing. Using it *as* the authority is what produced the admission time-of-check-to-time-of-use hole described in [gateway-invariants.md](./gateway-invariants.md); the acquire that authorises a send happens inside the same step that commits the nonce.

**The utilisation gate reads a peak, and that is what makes call order irrelevant.** `Acquire` records the in-flight high-water mark at the instant it takes the slot, where nothing can undo it, and `OnResult` compares that peak — not the live count — against half the window (`limits/participant.go`, `ParticipantLimiter.Acquire` and `ParticipantLimiter.OnResult`). This matters because the engine releases an attempt's slot in a `defer` and reports its verdict afterwards: a gate reading the live count would see the slot already given back and refuse to widen a window that had been genuinely saturated, so the window would grow slower than the rule says. Reading the peak decides identically whichever of release and result runs first, so the order of the three calls is an implementation detail and **not** a contract anyone has to maintain.

**Manual reset.** `POST /v1/admin/participants/unquarantine` clears every model's breaker for one participant and restores the initial window, and reports "not found" for a participant the gateway is not tracking. In-flight counts are deliberately left alone: they count attempts still running, not penalty.

Both limiters take a settings change without a restart. The gateway limiter swaps its whole configuration; the participant limiter keeps what it has learned, clamping any window above the new ceiling and lifting any that sits below the new initial. That lift is deliberate: an operator raising the initial window after a bad episode means it for the participants already tracked, and a limiter that applied it only to a restarted process would make the knob useless exactly when it is reached for. Nothing is lost by being generous — a participant that is still failing shrinks again within seconds, and the breaker is what protects against one that is failing badly.

The three admission defaults are set from what a day of production spent rather than from what looked reasonable. In 24 hours the shard burned 1618 nonces for nobody against 1607 client requests — one wasted nonce per request — and 35% of those burns were a nonce arriving at a participant whose window was full, with another 21% a nonce held for a host no queued request would accept. A hold grace of 200ms is shorter than the gap between arrivals at that load, so the nonce burned before its request could turn up; it is 2s now. An initial window of 64 per participant left three participants carrying sixteen slots at a fleet-wide 12 concurrent until AIMD grew it, which under 200-second requests takes hours; it was raised to 128 and then settled back at 64, because across three days no host sustained more than 59 concurrent streams. Queue depth follows the wait budget, and both were re-measured after that day: a slot is held 105 s at the median rather than ten, so at a five-minute budget it drains about three deep and the depth is four.

## The balance floor

An escrow that runs to zero does not stop serving — it starts failing. In one production minute three depleted escrows produced 178 client errors reading `insufficient escrow balance`, over half of that day's client-facing failures, because routing kept choosing an escrow that could no longer pay for what it was being handed.

The floor takes an escrow out of selection while it can still refuse cleanly. It scales with load rather than being a fixed reserve: the requests already in flight are what the escrow is about to owe, and one more covers the arrival being decided, so a fresh escrow must still afford a single request. There is nothing to tune: routing prices the request the way the chain does, `(input_tokens + max_tokens_cap) × token_price`, reading the price off the escrow's own session. An escrow whose price the gateway cannot yet read prices at zero and is never ejected on this rule.

It is zero by default, which disables it. A floor sized in the wrong unit would retire every escrow at once, so the gateway declines to guess.

## An empty stream has degrees

`empty_stream` is the bottom of the classification ladder: a receipt arrived, no error came, and nothing the client could render did either — not `content`, not `reasoning`, not `reasoning_content`, not a tool call. An empty string does not count, because upstream opens every reply with `{"delta":{"content":"","role":"assistant"}}` and crowning on that would hand the client a winner chosen for sending a preamble first.

That one label covers three very different hosts, so the finished-attempt line carries `stream_chunks` and `usage_tokens` whenever it fires:

| stream_chunks | usage_tokens | what it was |
|---|---|---|
| 0 | 0 | nothing after the receipt — the host took the nonce and never wrote a byte |
| > 0 | 0 | events arrived and all of them were empty |
| > 0 | > 0 | the host reported tokens it never delivered |

`stream_chunks` counts every write, not the content-bearing ones, which is what separates the first row from the second. On a thinking-budget route the third row is classified as `burn_empty` instead, because there the host's own token count is the one signal that separates a model producing nothing from a host carrying nothing.

The distinction matters for what it costs. A host that answers empty after a receipt cannot have its nonce closed early: the timeout vote is only accepted once the chain's execution deadline has passed, around thirty minutes from the receipt, and the escrow's in-flight count carries it the whole time.

## What in-flight actually counts

`devshard_runtime_active_requests` is not the number of clients waiting. The escrow hold a race takes is kept "for as long as the race's vote is owed" (`engine/engine.go`, `raceRegistration.holdEscrow`), and it is released on the goroutine that posts the timeout vote for every nonce the race left unfinished — after the losers have been given their grace, which defaults to ten minutes. So a request whose answer was delivered long ago keeps its escrow's count up until the chain has been told what became of each of its nonces. Reading the gauge as "requests still generating" overstates load by however much settlement is behind.

A retired escrow stays in `Snapshot` until that count reaches zero, reporting `devshard_runtime_active` as 0 while it drains (`registry/registry.go`). Dropping it at retirement ended the series mid-drain: the graph broke off at whatever the count happened to be — 208 in one production case — rather than falling to zero, so the most interesting minutes of an escrow's life were the ones no panel could show.

## Outlier ejection

`perf.Tracker` answers two questions in O(1) with no lock: **is this participant withheld from routing** (`Ejected`), and **did the detector want it out at all** (`Degraded`). They differ only by the pool-wide cap, and each has exactly one job.

**`Ejected` is a routing gate.** It is one of the scheduler's six host gates — excluded, proof-of-compute-required, throttled, ejected, state-blocked and capability — so a host it names receives no request and its nonces are burned as `participant_ejected_no_send` ghosts (`scheduler/match.go`, `scheduler/ghost.go`). It also drives the `devshard_gateway_host_ejected` gauge and one branch of the limiter-verdict ladder, where a `Stalled` attempt is charged to the host instead of excused as a model outcome, but only while that host is ejected (`engine/outcome.go`).

**`Degraded` is why the gate is not the whole story.** The cap below refuses to honour an ejection once too many of a model's hosts are failing at once, which is exactly the moment the gate stops protecting anything: those hosts stay in rotation. `Degraded` reports the verdict *before* the cap, and the race reads it for one decision — a primary the detector wanted out starts its second attempt immediately, under `primary_degraded`, rather than waiting out the receipt or first-token deadline. That hedge is bounded by the attempt budget, so a correlated outage costs at most one extra attempt per request and never an unbounded retry storm.

**What it tracks is health, not latency.** A sample is three fields: participant, model, and whether the host was responsive (`perf/sample.go`, `Sample`). There is no latency ring, no percentile and no host score in this package; response timings are recorded by the metrics layer from the race outcome, and escrow selection scores on in-flight load over chain weight. The only *exponentially* decayed quantities are the counts of successes and failures; the ejection count decays too, but in whole rungs rather than continuously.

**Ejection triggers** (`perf/ejection.go`, `ejectionPolicy.evaluate`): a run of consecutive failures, or a failure rate above the threshold once the decayed volume is large enough. The minimum-volume gate is why a quiet host is not ejected by one bad request.

**The ladder.** Only a *fresh* trigger starts an ejection — an already-ejected host rides out its current timer rather than having it pushed back. Each fresh trigger lengthens the next ejection linearly in the ejection count, capped, and resets the outcome counters so the rate restarts from zero. The count decays back one rung per full healthy window, with the anchor advancing so the ladder cannot unwind faster than that.

**The pool-wide cap.** Envoy's max-ejection-percent applies per model: at most `min(fraction × known hosts, known hosts − minimum available)` ejections are honoured, resolved by sorting participant keys. Ejections beyond the cap keep their timers running but are absent from the routing view, so they are `Degraded` and not `Ejected` (`perf/tracker.go`). This is what makes the routing gate safe to honour: a correlated outage can never remove a whole model's fleet from routing, and the hosts it leaves in rotation are the ones the race hedges instead.

**Why it is lock-free.** The shape is sized for a per-host, per-admission read: at five hundred hosts the old scan cost 2.35 ms per request and roughly 1 300 acquisitions of one global mutex. The tracker publishes two atomic maps of keys to expiry times, one capped for routing and one uncapped; a read is an atomic load, a map lookup and a time comparison, with no lock. The cap and the tie-break are resolved once, at rebuild time, and each entry carries its own expiry so ageing out needs no rebuild at all. The rebuild itself is conditional — only when the membership the cap is computed over actually moved (`perf/tracker.go`, `Tracker`'s two published views, `Tracker.RecordSample` and `ejectedIn`).

Stale host state is swept at most once per tenth of the staleness window, because entries age out over minutes and scanning every host on every sample costs O(hosts) under the global lock for nothing.

**Capability flags** — a host's known context limit and whether it rejects tool use — are keyed by participant and model and expire with the host staleness window. They feed the scheduler's capability filter, which is how a retry skips every host already known to be too small. A protocol-version refusal is counted and reported beside them but never routed on: it is the one verdict that would hold a host out of the rota wholesale rather than steer the requests it cannot serve, and a gateway serves one protocol version for its whole life, so holding out would retire the host for good.

## Nothing here is persisted

Every restart starts clean: no ejections, no capability flags, every AIMD window at its initial value, every breaker closed, every decayed counter at zero. That is a deliberate divergence from the legacy gateway, argued in [gateway-non-goals.md](./gateway-non-goals.md): minute-scale backoff self-heals faster than replaying stale penalties is worth. The cost is that a genuinely bad host gets one free window after every deploy.

The one host judgement that *is* persisted is the operator's manual suspicious-host pin, and for the opposite reason — a pin the gateway acts on but forgets on restart is a state an operator cannot see. The store is written before the in-memory copy (`main.go`, `suspiciousHosts.Add`).

Two asymmetries worth knowing, neither of which is stated in the code:

- The participant limiter's `{participant, model}` state map never evicts; only the performance tracker ages hosts out. The participant set is the bounded validator set, so this is survivable, but the two packages differ.
- Ejection thresholds are re-read from configuration on every sample, so they hot-reload — but a host's decay half-life is captured when the host is first seen, so a changed half-life applies only to hosts seen afterwards.

## Configuration

| Knob | Default | Effect |
|---|---|---|
| `max_concurrent_requests` | 1 536 | Per-model in-flight request cap, scaled by capacity. |
| `max_input_tokens_in_flight` | 0 (unlimited) | Per-model input-token budget, scaled by capacity. |
| `max_concurrent_requests_per_10000_weight` | 24.0 | Weight-derived cap; when set with an observed baseline it replaces the absolute cap. |
| `poc_max_concurrent_requests_per_10000_weight` | 48.0 | The same, used while the chain reports requests blocked. |
| `admission_queue_wait_ms` | 300 000 | How long a request waits for a free slot before a 503. The same value is returned as `Retry-After`. |
| `host_initial_inflight` / `host_max_inflight` | 64 / 256 | How many requests may be in flight to one host, to start and at most. The window opens near a host's known capacity and AIMD is left to back off from it, rather than discovering it upward from a cold start. |
| `host_cutoff_after_failures` | 3 | Consecutive transport faults before the host stops receiving requests. |
| `host_cutoff_ms` / `host_cutoff_max_ms` | 5 000 / 60 000 | How long a cut-off host stays out, first time and at most. The maximum must not exceed the performance ejection maximum, so ejection stays the dominant authority. |
| `perf_consecutive_fail_threshold` | 5 | Consecutive-failure ejection trigger. |
| `perf_failure_rate_threshold` / `perf_failure_rate_min_volume` | 0.15 / 20 | Rate-based ejection trigger and its volume gate. |
| `perf_ejection_base_seconds` / `perf_ejection_max_seconds` | 30 / 600 | Ejection duration ladder. |
| `perf_max_ejection_fraction` / `perf_min_available_hosts` | 0.5 / 1 | Pool-wide ejection cap, and the reason the routing gate cannot empty a model's fleet. |
| `perf_host_staleness_seconds` | 3 600 | When an unseen host is forgotten. |
| `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` | 600 | Half-life of the decayed success and failure counters. |

The rows down to `host_cutoff_max_ms` are admin overrides, changeable at run time without a redeploy. The `perf_*` rows are **not**: they are neither overrides nor environment variables, only compile-time defaults, and the snake_case names above are the spellings the boot-time validator uses in its error messages, not knobs an operator can set. `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` is the one performance value with an environment variable, and it is read once at boot. Retuning ejection therefore means a new binary.

The default input-token budget of zero means unlimited, which is worth an operator's attention: with million-token contexts it is the only thing between concurrency and memory exhaustion, and the body-size cap deliberately does not throttle load.

## The wait budget

A shard should not answer 429. That status means the client exceeded a quota, and a client that ran into the shard's own capacity exceeded nothing — it carries no hint of when to return, so a well-behaved client retries immediately and deepens the shortage it just hit. Every capacity refusal answers 503 instead, and every one of them carries `Retry-After`: the wait already spent when that is known, a default otherwise.

`admission_queue_wait_ms` is the budget a request may spend looking for capacity, not a delay. A queued waiter is promoted the instant a slot frees. The default is five minutes, and it is set from what a slot actually costs: across three days of load the median winning attempt held its slot 105 s and the p90 held it 317 s. A two-minute budget was shorter than the p90 hold, so most waiters could not be reached before their budget ran out.

That ratio is where `admission_queue_per_slot` comes from: five minutes against a 105 s median hold drains about three deep, so four holds a burst without admitting waiters that provably cannot be served. Waiting the whole budget out and being refused anyway costs the client the wait and the shard the connection, so a request that provably cannot reach the front in time is refused on arrival instead. Depth is counted per model against that model's own concurrency, because the caps are per model: a heavy model does not shrink a light one's queue.

Two gates can refuse, and they answer different questions. The gateway limiter asks whether there is budget — concurrency, input tokens, chain weights. The scheduler asks whether a live host will take it — participant windows, breakers, chain phase. A request can pass the first and stall at the second; production has shown exactly that, with zero limiter refusals and nine scheduler refusals in one burst. The budget is meant to bound the whole path, and the scheduler side of it is not built yet: it still refuses immediately rather than waiting. See `specs/2026-08-03-request-queue-design.md`.

Escrow membership must reach this layer: without it `EscrowWeight` returns zero for every escrow, escrow selection fails on every request, and the gateway serves nothing while every health check stays green.

## Nonce dispositions

Settlement credits each slot with `assigned_nonces - protocol_misses` completed inferences, whatever GNK those nonces paid. Three gateway behaviours break that count: policy burns nonces without sending work, a sent request can go unfinished while its timeout never applies, and overscheduling turns one client request into several completed inferences. The nonce ledger exists to say which of those happened and how often; the accounting model is `proposals/gateway-dashboard`.

The disposition model is taken from that proposal. The event vocabulary is not: it is built from the facts this gateway already produces, which are coarser than the reference's and carry the same information in fewer events.

| what the ledger folds | where it comes from |
|---|---|
| escrow membership, its latest nonce, and what the chain recorded per slot | a ten-second sweep of the published escrow set: `Snapshot` names the escrows, each session's `SnapshotState` carries the slot group, the latest nonce, and `HostStats` |
| a burned nonce and its reason | `tracedDispatches.GhostBurned`, which carries the nonce and one of six reasons |
| every attempt of one race | `nonceAccountedRaces.RecordRace`: per attempt the nonce, whether it was sent, whether the protocol finished it, and whether the client got its answer |
| a timeout's kind, action and reason | `nonceAccountedRaces.RecordTimeout` |

Three of those are pushed and three are swept. Membership and host stats have no event of their own and change slowly, so reading them on a timer is both simpler and less invasive than a callback on every nonce; a race and a burn are single moments and must be told.

One race outcome replaces six of the reference's events, because it already aggregates what they report separately: a send, a winner, a loser, an unknowable usage, and the finish. One timeout event replaces two.

**A nonce names its own slot.** The reference reads applied diffs to learn which slot spent a nonce. That is unnecessary here: the chain's own convention makes the executor the slot at nonce modulo group size, so the ledger attributes a nonce arithmetically and needs no protocol-transition channel at all.

Two consequences follow from folding coarser events. The reference deduplicates replayed callbacks by using each event as its own map key, which requires every event to be a comparable struct; a race outcome carries a slice of attempts and cannot be one. It does not need to be: a race reports itself once, a burn is recorded once, and swept facts are idempotent by construction because each observation replaces the last. The dedup map and the comparability constraint both go.

**An unapplied timeout is not a settled one.** Every path out of `user.HandleTimeout` returns an error, including its own success — that error carries "this inference timed out" back to the request. So the error says nothing about whether the vote reached the chain, and `TimeoutResult.Applied` is what the ledger reads instead. The distinction is the whole point: a nonce whose timeout never gathered enough votes still settles as a completed inference for a participant that never answered, and recording it as a posted vote hides exactly that.

**An unfinished nonce is not a verdict.** The protocol can finish a nonce after the race that gave up on it, so the sweep re-asks the session about every nonce still counted as unfinished and lifts the ones that landed. The session's own outcome map survives sealing, so a negative answer means "not finished", never "no longer known" — the correction only ever moves a nonce out of the bucket settlement reads as failure, never into it.

**What the ledger cannot see.** A nonce is invisible to it between commitment and the end of the race that spent it, because a race reports only when it ends. Those nonces fall into `unobserved` alongside genuinely protocol-only ones, so that number is a floor on protocol overhead rather than a measurement of it; a baseline that grows while traffic is steady is the signal worth watching. `pending` is the separate case of a nonce seen unfinished whose timeout has not settled, and `overcounted` — classified beyond what the chain assigned — should never be anything but zero.

The vocabulary is ours where ours is richer. Six ghost reasons, not five: `participant_ejected_no_send` and `request_abandoned_before_dispatch` are first-class rather than an unknown reason with a detail string. Timeout reasons likewise — `phase_transition_aborted` and `long_response_after_content` happen to match the reference verbatim, while `empty_stream_without_non_empty_winner`, `nonce_already_finished` and `no_poster` are ours alone.
