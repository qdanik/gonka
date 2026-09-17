# Devshard gateway — capacity and host health

Three separate questions, three separate mechanisms:

| Question | Owner |
|---|---|
| How many requests may the *gateway* have in flight, and for which model? | `limits.GatewayLimiter` |
| How many input and output tokens may *this participant* hold in flight? | `limits.ParticipantLimiter` |
| Is this participant an outlier, and what can it not serve? | `perf.Tracker` |

They are not one thing. The first protects the gateway and respects the network's view of how much of the model's capacity this gateway commands; the second protects the participant from the gateway; the third is an outlier detector plus the sticky record of what a host has proved it cannot do.

A host is removed from a pick by the participant limiter or by the ejection verdict — see [Outlier ejection](#outlier-ejection) for what the ejection verdict is capped by before routing honours it.

The design principle across all three is **adaptation instead of punishment**: an overloaded host receives less traffic within one round trip and recovers on its own, and the longest it is held out of rotation is minutes rather than a sentence it has to sit out ([rules.md](./rules.md), "Dropped from the legacy gateway").

## Capacity: what the chain says this gateway may use

`limits.Capacity` holds the latest chain snapshot and the escrow membership pushed by the registry, and answers two questions.

**Scale factor for a model** — the fraction of the network's reference weight for that model that is currently available to this gateway:

```
scaleFactor(model) = clamp( Σ availableCurrentWeight / Σ fullWeight , 0, 1 )
```

with three fixed answers at the edges (`limits/capacity.go`, `Capacity.ModelWeights`; `limits/weights.go`, `scaleFactor`):

- Requests blocked by the chain phase → **0**.
- The model is not served → **0**.
- No reference weight observed at all → **1.0**. An unobserved baseline means unlimited, not zero.

**Escrow weight** — how much serving weight one escrow commands for a model:

```
escrowWeight(escrow, model) = Σ_participant currentWeight[p] × hostShare[p] × available(p)
```

`hostShare` is a **normalised share**, `slots(p, this escrow) / slots(p, all live escrows)`, computed by the registry (`registry/membership.go`, `hostShares`). The type is a plain float map, so nothing stops a caller passing raw slot counts — and doing so type-checks, works, and silently gives a participant serving three escrows its full weight in each, tripling its apparent capacity so the gateway over-admits. The invariant lives only in this prose; no type enforces it.

`available` is the participant limiter's non-mutating peek, so congestion-window and cut-off state feed *back* into capacity weighting and escrow scoring (`main.go`, `compose`, which hands `ParticipantLimiter.Available` to `limits.NewCapacity`).

### Two fail-safes with opposite directions

**Unknown model fails closed.** A model absent from a *populated* per-model weight view is served by nobody and gets zero — it does not inherit the all-model view (`limits/capacity.go`, `Capacity.modelServedLocked`). With no per-model view at all (cold start, or an older chain response), the generic view still applies to everything, and the two sides fall back independently so a missing full-weight entry does not suppress a present current-weight one (`limits/capacity.go`, `Capacity.currentWeightsLocked` and `Capacity.fullWeightsLocked`). This matters because `model` is unvalidated client input: without the guard an unrecognised model string inherits full network capacity.

**Unobserved weights fail open.** When neither weight view has any key at all, escrow scoring falls back to the availability-filtered membership share instead of zero (`limits/capacity.go`, `Capacity.EscrowWeight`). Zero would make every escrow score as unusable, so every request in the boot window would be refused. The test is *emptiness*, not staleness: a chain-reported participant is a key in the view whatever its weight, so an empty view means the chain named nobody.

That fallback serves requests correctly and silently, which is its own hazard — an operator sees traffic flowing and cannot tell routing is running blind. So the `WeightsUnobserved` flag of `Capacity.ModelWeights` is derived from the *same two predicates the fallback branch itself reads* and published as `devshard_gateway_capacity_weights_unobserved_by_model`. A gauge computed from the same predicates cannot drift from the behaviour it reports.

## The gateway limiter

Two dimensions — in-flight requests and in-flight input tokens — at two scopes.

**The configured maxima are each model's own budget.** Two models under `max_concurrent_requests: 512` get 512 each, not 512 between them, and a per-model override replaces the configured maximum for its model rather than narrowing it further (`limits/gateway.go`, `GatewayLimiter.admissionFor`). This is forced by the dimension the cap is derived from: the effective limit below comes from *one model's* chain weight, so charging it against another model's in-flight count would make the enforced cap depend on which model's request happened to arrive first. Nothing here bounds the process across models — the escrow registry does, by rejecting an unserved model before it can reach the limiter (`api/routes.go`, `Server.chat` and `Server.routableModel`), so the model set is the operator's and not the client's, and cycling model strings mints nothing.

**Effective limits come from weight when weight is known** (`limits/gateway.go`, `effectiveConcurrencyLimit`). With a per-10 000-weight rate configured and a baseline weight observed, the configured absolute maximum is ignored and the limit becomes the minimum of the current-weight-derived and baseline-derived caps — current weight can lower the cap, never lift it above the baseline. Otherwise the configured maximum is scaled by the scale factor, **rounded to nearest**: flooring would take a configured cap of 1 to 0 at any partial capacity, refusing every request for that model instead of degrading it. A NaN or out-of-range scale clamps to **zero**, never to one: a corrupted scale must not grant unlimited capacity (`limits/gateway.go`, `scaleClamp` and `clampUnit`).

A scale factor of zero therefore means an immediate 429 rather than a queue — which is what happens while the chain is blocking requests, if a request somehow reaches the limiter at all.

The capacity values are passed **in** to each acquire and never re-read from a snapshot inside the limiter, and the contract on that parameter is that they are already availability- and block-filtered (`limits/gateway.go`, `ModelCapacity`). Only the composition root satisfies it.

### The queue

If the request does not fit, it joins a FIFO queue and its own goroutine parks in a `select` on a channel and a timer. There is no per-waiter goroutine and no broadcast on release: a releasing request decrements the counters **and hands the freed capacity directly to the queued waiter, inside the same lock hold** (`limits/gateway.go`, `waiter` and `GatewayLimiter.ReleaseForModel`). Admission is transferred, not re-contested, so an admitted waiter never competes with newly arriving requests.

Waking every waiter on each release starves the oldest: under measurement one waiter is overtaken 1.67 million times in 1.2 seconds and times out into a 429 while newer arrivals pass it.

The promotion sweep walks the queue in arrival order and skips — rather than stops at — a waiter its own model still cannot serve (`limits/gateway.go`, `GatewayLimiter.promoteLocked`). Admission is therefore first-come-first-served within a model, and a saturated model cannot stall the others, which is the whole point of the budgets being per model.

A new arrival that fits may pass a queued waiter without unfairness, because a queued waiter is by definition one that current capacity cannot serve.

Both races at the edges are handled explicitly. If the timer fires but the waiter was already promoted, the acquire *succeeds* — the slot is already held, and refusing it would leak the transfer. If the context is cancelled but the waiter was already promoted, the slot was handed to a caller that is gone, so it is released (`limits/gateway.go`, the `select` in `GatewayLimiter.AcquireForModel`).

`Reconfigure` swaps the caps and sweeps the queue, so a widened limit reaches a waiting request immediately rather than at the next release.

## The participant limiter: IOCW

State is per `{participant, model}`, and each pair carries two congestion windows rather than one: the input congestion window (ICW) over the tokens an attempt must prefill, and the output congestion window (OCW) over the tokens it reserved for its answer (`limits/participant.go`, `hostState`). Both are taken at admission and given back together when the attempt ends. A reservation that drained as its tokens arrived would make a host look freer the longer it generates, which is exactly the host with least left to give.

**A window is configured in requests and counted in tokens.** What one request is worth comes from the model: its context length on the input side, its output cap on the other (`limits/participant.go`, `boundsIn` and `ParticipantLimiter.windowsForLocked`). A context length is resolved in three steps — the operator's pin `model_limits[m].max_model_len`, then what governance reports as `Model.context_window` (read by `chain.Reader.Models` and carried on `PhaseSnapshot.Models`), then `fallback_max_model_len`. A context window above a hundred million tokens is not a unit a window could be priced in, so governance reporting one is dropped rather than clamped and that model keeps the length it was last priced at (`main.go`, `contextWindowsOf`), while an operator configuring one is refused (`config/validate.go`, `Config.Validate`). The output side takes `model_limits[m].max_tokens_cap` and falls back to the global `max_tokens_cap`. The additive step is that same unit — one request of that model — so there is no step to tune.

**No output budget passes `filters.MaxOutputTokens`, ten million.** Every path through `capOutputTokens` clamps to it, the admin bypass included, and `Validate` refuses a `max_tokens_cap`, a `default_max_tokens` or a per-model override above it (`filters/rules_tokens.go`, `capOutputTokens`; `config/validate.go`, `Config.Validate`). A request that cannot ask for more than the ceiling is a request whose price, and the window priced from it, stay inside the arithmetic that counts them ([rules.md](./rules.md), "10. Counters saturate").

**A host with nothing in flight always admits one request, whatever its size** (`limits/participant.go`, `admitsLocked`). Without that rule a prompt larger than the whole window is a request no host could ever serve. With work already in flight both dimensions have to fit, a half-open probe is one request whatever its size, and an open cut-off admits nothing at all.

That rule is a floor under the arithmetic rather than the common case, because a window priced in a model's context length is wide: two requests at the million-token fallback is a two-million-token input window, which admits ordinary prompts in the hundreds or thousands before it refuses one. It bites where a single request is priced above the whole window — a prompt longer than the context length its model is priced at, or a window a run of bad answers has left at its floor.

**`Acquire` returns a lease**: a closure that gives back exactly what it took, exactly once (`limits/participant.go`, `ParticipantLimiter.releaseFor`). The lease travels to the attempt on `scheduler.Assignment.HostSlot` and is released where the attempt ends, the same shape as the escrow hold beside it (`scheduler/scheduler.go`, `Assignment.ReleaseHostSlot`). The engine's view of the limiter can neither acquire nor release, so the pairing is not something a call site can get wrong.

**Growth is earned per window, against the peak.** A window widens only when its peak since the last adjustment reached half of it, so an idle host accumulates nothing (`limits/participant.go`, `ParticipantLimiter.growLocked`). Until a window has been narrowed once it is still looking for the host's capacity, so it takes the whole of what the answer carried — doubling per window served. After the first narrowing it earns one step per window's worth of tokens, so a wide window earns its next rung more slowly than a narrow one (`limits/congestion.go`, `grownBy`). Each window is credited in the currency it was charged: the tokens prefilled widen the input window, the tokens reserved for the answer widen the output one. **There is no ceiling** — where a window stops is what the host's congestion signals say, not a number an operator guessed.

**Narrowing is one ladder of four factors**: soft `0.85`, hard `0.70`, severe `0.50`, and `0.90` on the window the signal did *not* blame, because prefill and decode share one device and a host struggling at one end has less to give at the other (`limits/congestion.go`, `CongestionFactors.narrowingFor`). A signal that blames the host as a whole applies its own factor to both windows and carries no cross factor. Nothing goes below the floor, which is one request of that model. Any factor other than one ends slow start for the window it applies to and starts that window's peak again from what is in flight, including a narrowing that finds the window already at its floor (`limits/participant.go`, `narrowWindow`): a window whose floor and starting size are the same number would otherwise stay in slow start for ever and double after every congestion signal.

What each answer blames, and what it does to the cut-off (`limits/congestion.go`, `responseFor`):

| The attempt ended as | Input window | Output window | Cut-off |
|---|---|---|---|
| a healthy answer, with the delay signal quiet | grows | grows | cleared, and a half-open probe recovers |
| a healthy answer, with the delay signal naming one dimension | soft when it names input, cross when it names output | soft when it names output, cross when it names input | cleared, and a half-open probe recovers |
| a healthy answer, with the delay signal naming both | soft | soft | cleared, and a half-open probe recovers |
| a late answer, from a host that missed a deadline | untouched | untouched | cleared, and a half-open probe recovers |
| `429` or `503` | soft | soft | count cleared |
| a burn-empty the host held past the refusal timeout | soft | soft | count cleared |
| an attempt the twenty-minute backstop cut | soft | soft | count cleared |
| an upstream `5xx`, or an empty answer whose nonce closed | hard | hard | untouched |
| an empty answer that left its nonce open | severe | severe | counts |
| a missed receipt deadline | severe | severe | untouched |
| a missed first-token deadline | severe | cross | untouched |
| a stream that went silent between chunks | cross | severe | untouched |
| a transport fault — a dial failure, a `403`, a clock drift | untouched | untouched | counts |
| a model outcome — a burn-empty inside the refusal timeout, an error stream, a capability refusal | untouched | untouched | untouched |

A receipt arrives before any prefill, so a receipt deadline a host missed says nothing about which half of it is slow and narrows both. A first token is what prefill produces and a silence mid-stream is what decode failed to, so each of those blames its own window and touches the other through the cross factor. A silence is the one severe signal the cut-off ignores: a host answering slowly is not the host the cut-off is for, and one that stalls again and again is the outlier detector's to withhold. A transport fault is the mirror image — it moves no window at all, because it says the host is broken rather than full, and a broken host is exactly the cut-off's business. A model outcome moves nothing and creates no state: it is what the model produced, not what the host failed to carry, and narrowing for it would penalise the wrong party (`limits/congestion.go`, `response.inert`).

Two of the rows above are the ladder reaching past the terminal an attempt carries (`engine/outcome.go`, `RaceOutcome.Verdict`). A burn-empty is the model's own output and moves nothing, until the host has held the request past the chain's refusal timeout and still returned nothing, which is a host that took on more than it could carry. An attempt still running when the twenty-minute backstop cuts it says the same thing, and both report the overload a `429` reports.

**The delay signal narrows a host before anything has failed.** `perf` keeps 64-sample p75 rings for first content and for time per output token, and a baseline per ring: the best p75 the host has held (`perf/host.go`, `latencyWindow`). `Tracker.Pressure` is the current p75 over that baseline, and a healthy answer from a host more than `host_congestion_slack` above it is soft congestion on the dimension it names rather than growth (`limits/participant.go`, `ParticipantLimiter.OnResult`; `limits/congestion.go`, `Pressure.congestedDimension`). One congested dimension moves both windows, like any other signal that blames one: the named window takes the soft factor and the other takes the cross factor, and neither grows, because an answer that arrived late for its host is not an answer that earned it more room. The baseline falls to a new best at once and rises a thousandth of the gap per sample, an order of magnitude slower than the ring fills: a baseline that kept up with the ring would track the latency the host currently holds, and a host degrading steadily would never read as congested.

**The cut-off** trips after `host_cutoff_after_failures` consecutive faults of the two kinds that count towards it — a transport fault, and an empty answer that left its nonce open — or immediately when a half-open probe fails, because a probe gets exactly one try. Backoff is `base × 1.6^count` clamped to the maximum, with up to 20% jitter added after the clamp so hosts saturated together do not all reopen on the same tick; the count stops rising once saturated so the exponent cannot overflow the duration (`limits/participant.go`, `ParticipantLimiter.applyBreakerLocked`). The jitter is gRPC's connection-backoff constant.

Recovery walks the ladder back down: a healthy answer while half-open clears the trip *and* decrements the backoff count. The subtle part is that it must clear the open-until timestamp and not just the probe flag — any host with a non-zero open-until reads as half-open, so clearing only the flag pins a recovered host at one probe forever.

**`Available` is a peek, not an authority.** It evaluates the same predicate against the smallest possible request, without mutating in flight, without setting the half-open flag and without creating state for an unseen participant (`limits/participant.go`, `ParticipantLimiter.Available`). It exists because routing needs a cheap pre-filter where a stale answer costs nothing. Using it *as* the authority is what produced the admission time-of-check-to-time-of-use hole described in [rules.md](./rules.md); the acquire that authorises a send happens inside the same step that commits the nonce.

**The growth gate reads a peak, and that is what makes call order irrelevant.** `Acquire` records each window's in-flight high-water mark at the instant it takes the tokens, where nothing can undo it, and `OnResult` compares that peak — not the live count — against half the window (`limits/participant.go`, `window.take` and `ParticipantLimiter.growLocked`). This matters because the engine releases an attempt's lease in a `defer` and reports its verdict afterwards: a gate reading the live count would see the tokens already given back and refuse to widen a window that had been genuinely saturated. Reading the peak decides identically whichever of release and result runs first, so the order of the three calls is an implementation detail and **not** a contract.

**Manual reset.** `POST /v1/admin/participants/unquarantine` clears every model's cut-off for one participant, restores both windows to their initial size and puts them back in slow start, and reports "not found" for a participant the gateway is not tracking (`limits/participant.go`, `window.reopen`). In-flight counts are left alone: they count attempts still running, not penalty.

Both limiters take a settings change without a restart. The gateway limiter swaps its whole configuration; the participant limiter keeps what each host has earned. Governance naming a model's context length moves the bounds a host is judged against and lifts a window to the new floor, but never returns a window a run of bad answers took away (`limits/participant.go`, `ParticipantLimiter.ObserveModels`). A governance reply that names no model is no observation rather than a chain that named nobody: the read is not counted as fetched, each model keeps the length it was last priced at, and an observation that does arrive is merged into what is already known (`chain/observer.go`, `PhaseObserver.fetchModels`). The direction is load-bearing because the floor only ever rises — one empty reply read as an answer would reprice every model at `fallback_max_model_len` and leave every host's input window that wide for the life of the process. An operator raising the initial window is the deliberate act, and it does lift a window that collapsed below it (`limits/participant.go`, `ParticipantLimiter.Reconfigure`) — a knob that reached only a restarted process would be useless exactly when it is reached for. Nothing is lost by being generous: a host that is still failing narrows again within seconds, and the cut-off is what protects against one failing badly.

Two of the admission defaults are sized from measured load. **Hold grace is 2 s**: at 1618 nonces burned for nobody against 1607 client requests in 24 hours — one wasted nonce per request, 35% of them arriving at a participant whose window was full and 21% held for a host no queued request would accept — a 200 ms grace is shorter than the gap between arrivals, so the nonce burns before its request arrives. **Queue depth is four**: a slot is held 105 s at the median, so a five-minute budget drains about three deep.

## The balance floor

An escrow that runs to zero does not stop serving — it starts failing. A depleted escrow left in selection fails every request routing hands it: three of them produce 178 `insufficient escrow balance` errors in a minute, over half of a day's client-facing failures.

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

A retired escrow stays in `Snapshot` until that count reaches zero, reporting `devshard_runtime_active` as 0 while it drains (`registry/registry.go`). Dropping it at retirement instead would hide the most interesting minutes of an escrow's life behind a panel that no longer has a series to draw.

## Outlier ejection

`perf.Tracker` answers two questions in O(1) with no lock: **is this participant withheld from routing** (`Ejected`), and **did the detector want it out at all** (`Degraded`). They differ only by the pool-wide cap, and each has exactly one job.

**`Ejected` is a routing gate.** It is one of the scheduler's six host gates — outside the allowlist, proof-of-compute-required, throttled, ejected, state-blocked and excluded by this waiter — so a host it names receives no request while the gate holds.

**`Degraded` is why the gate is not the whole story.** The cap below refuses to honour an ejection once too many of a model's hosts are failing at once, which is exactly the moment the gate stops protecting anything: those hosts stay in rotation. `Degraded` reports the verdict *before* the cap, and the race reads it for one decision — a primary the detector wanted out starts its second attempt immediately, under `primary_degraded`, rather than waiting out the receipt or first-token deadline. That hedge is bounded by the attempt budget, so a correlated outage costs at most one extra attempt per request and never an unbounded retry storm.

**What it tracks is health and latency both.** A sample carries participant, model, whether the host was responsive, and two timings: first content and time per output token (`perf/sample.go`, `Sample`). The timings feed two 64-sample rings whose p75 the escalation ladder reads; the health counters are decayed and feed ejection.

**Ejection triggers** (`perf/ejection.go`, `ejectionPolicy.evaluate`): a run of consecutive failures, or a failure rate above the threshold once the decayed volume is large enough. The minimum-volume gate is why a quiet host is not ejected by one bad request.

**The ladder.** Only a *fresh* trigger starts an ejection — an already-ejected host rides out its current timer rather than having it pushed back. Each fresh trigger lengthens the next ejection linearly in the ejection count, capped, and resets the outcome counters so the rate restarts from zero. The count decays back one rung per full healthy window, with the anchor advancing so the ladder cannot unwind faster than that.

**The pool-wide cap.** Envoy's max-ejection-percent applies per model: at most `min(fraction × known hosts, known hosts − minimum available)` ejections are honoured, resolved by ejection count first, most chronic kept out, with the participant key breaking ties.

**Why it is lock-free.** The shape is sized for a per-host, per-admission read: at five hundred hosts a scan under one global mutex costs 2.35 ms and roughly 1 300 lock acquisitions per request. The tracker publishes two atomic maps of keys to expiry times, one capped for routing and one uncapped; a read is an atomic load, a map lookup and a time comparison, with no lock. The cap and the tie-break are resolved once, at rebuild time, and each entry carries its own expiry so ageing out needs no rebuild at all. The rebuild itself is conditional — only when the membership the cap is computed over actually moved (`perf/tracker.go`, `Tracker`'s two published views, `Tracker.RecordSample` and `ejectedIn`).

Stale host state is swept at most once per tenth of the staleness window, because entries age out over minutes and scanning every host on every sample costs O(hosts) under the global lock for nothing.

**Capability refusals** — an unsupported protocol version, a tool call the build does not implement, a context length it will not take — are counted, and the smallest context a host has admitted to is kept beside them. Nothing here withholds a host from routing: the counts say what to fix and how often it happened, and a refusal that repeats is a build that refuses everything rather than a one-off. Version refusals are keyed by participant, because a protocol version is a property of the build; tool and context refusals are keyed by participant and model.

## Buffered replies have a ceiling of their own

A client that asked for one whole answer is still served over a stream: the gateway forces `stream: true` upstream. The events are folded into the answer as they arrive and the raw stream is dropped, so what such a request holds is the reply being assembled — and the internal fields nobody will see are removed before the merge, not after it, so a client that did not ask for logprobs never accumulates them. `max_buffered_response_bytes` (`GATEWAY_MAX_BUFFERED_RESPONSE_BYTES`, 512 MiB) is the ceiling on every such reply at once.

The per-request cap is separate and much smaller — 32 MiB, bounding one unterminated SSE frame rather than a whole accumulated stream. This ceiling is the only bound on the *sum*, and the request limiter does not stand in for it: with `max_concurrent_requests` unset its cap comes from network weight and routinely admits thousands at once, which made the exposure one per-request ceiling times however many requests arrived.

Past the ceiling a request is refused with `503`, the same answer as a shard with no room — the gateway has nothing left to hold, which is not the caller's doing. A ceiling of zero holds nothing back, for a deployment that would rather be killed by the kernel than refuse a request. Lowering it at runtime stops admitting rather than repossessing: what is already held drains on its own.

`devshard_gateway_buffered_response_bytes` is what is held right now. Watch it before choosing a number: the sum is driven by the *typical* reply, and the ceiling only bounds the tail.

## Nothing here is persisted

Every restart starts clean: no ejections, no capability counts, every congestion window at its initial size, every cut-off closed, every decayed counter at zero. The divergence from the legacy gateway is argued in [rules.md](./rules.md): minute-scale backoff self-heals faster than replaying stale penalties is worth. The cost is that a genuinely bad host gets one free window after every deploy.

The one host judgement that *is* persisted is the operator's manual suspicious-host pin, and for the opposite reason — a pin the gateway acts on but forgets on restart is a state an operator cannot see. The store is written before the in-memory copy (`main.go`, `suspiciousHosts.Add`).

Two things the code does not state:

- The participant limiter forgets a `{participant, model}` pair on the window the performance tracker ages hosts out on, `perf_host_staleness_seconds`, but only a pair with nothing in flight and no cut-off still running. The scan runs inside `Acquire`, at most once per tenth of the window, after the pair it is asked about is marked used (`limits/participant.go`, `ParticipantLimiter.forgetIdleLocked`). Giving a lease back marks the pair used as well, so a host whose verdict has not arrived yet is not forgotten between the release and the result it is about to be judged on (`limits/participant.go`, `ParticipantLimiter.release`). A forgotten host that returns starts at the initial windows with a closed cut-off, as it would after a restart.
- Ejection thresholds are re-read from configuration on every sample, so they hot-reload — but a host's decay half-life is captured when the host is first seen, so a changed half-life applies only to hosts seen afterwards.

## Configuration

| Knob | Default | Effect |
|---|---|---|
| `max_concurrent_requests` | 2 048 | Per-model in-flight request cap, scaled by capacity. |
| `max_input_tokens_in_flight` | 0 (unlimited) | Per-model input-token budget, scaled by capacity. |
| `max_concurrent_requests_per_10000_weight` | 8.0 | Weight-derived cap; when set with an observed baseline it replaces the absolute cap. |
| `poc_max_concurrent_requests_per_10000_weight` | 16.0 | The same, used while the chain reports requests blocked. |
| `admission_queue_wait_ms` | 300 000 | How long a request waits for a free slot before a 429. The same value is returned as `Retry-After`. |
| `fallback_max_model_len` | 1 000 000 | The context length one request is priced at for a model neither the operator nor governance names one for. It is deliberately generous: an unknown context must not strangle prefill, and a window that opens too wide narrows itself within a round trip while one that opens too narrow burns nonces. |
| `model_limits[].max_model_len` | unset | The operator's pin for one model's context length, outranking what governance reports for it. |
| `host_input_window_min_requests` / `host_input_window_initial_requests` | 1 / 2 | The input congestion window's floor and starting size, counted in requests of the model and held in tokens. One request always fits, however far the window has been narrowed; a host opens at two and doubles for every window's worth of tokens it carries, until its first congestion signal. |
| `host_output_window_min_requests` / `host_output_window_initial_requests` | 1 / 2 | The same two for the output congestion window, where one request is worth the model's output cap rather than its context length. |
| `io_aimd_beta_soft` / `io_aimd_beta_hard` / `io_aimd_beta_severe` | 0.85 / 0.70 / 0.50 | What a narrowing multiplies the blamed window by, one factor per rung of the ladder. Each is above 0 and below 1: a factor of one never narrows, and one of zero closes a window nothing could reopen. |
| `io_aimd_beta_cross` | 0.90 | What the window a signal did not blame takes with it, because prefill and decode share one device. One is the operator saying there is no cross-dimension penalty. |
| `host_congestion_slack` | 0.30 | How far above the best latency it has held a host may drift before a healthy answer counts as soft congestion on that dimension instead of growth. |
| `host_cutoff_after_failures` | 3 | Consecutive faults of the kinds that count towards the cut-off before the host stops receiving requests. |
| `host_cutoff_ms` / `host_cutoff_max_ms` | 5 000 / 60 000 | How long a cut-off host stays out, first time and at most. The maximum must not exceed the performance ejection maximum, so ejection stays the dominant authority. |
| `perf_consecutive_fail_threshold` | 5 | Consecutive-failure ejection trigger. |
| `perf_failure_rate_threshold` / `perf_failure_rate_min_volume` | 0.15 / 20 | Rate-based ejection trigger and its volume gate. |
| `perf_ejection_base_seconds` / `perf_ejection_max_seconds` | 30 / 600 | Ejection duration ladder. |
| `perf_max_ejection_fraction` / `perf_min_available_hosts` | 0.5 / 4 | Pool-wide ejection cap, and the reason the routing gate cannot empty a model's fleet. |
| `perf_host_staleness_seconds` | 3 600 | When an unseen host is forgotten by the performance tracker and the participant limiter, and when its participant-labelled race series are deleted. |
| `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` | 600 | Half-life of the decayed success and failure counters. |

Every row above is an admin override, changeable at run time without a redeploy. `max_concurrent_requests`, `admission_queue_wait_ms` and the `perf_*` rows also take a `GATEWAY_*` environment variable read at boot; the window, congestion and cut-off rows are reachable through the admin surface alone.

The default input-token budget of zero means unlimited, which is worth an operator's attention: with million-token contexts it is the only thing between concurrency and memory exhaustion, and the body-size cap does not throttle load.

## The wait budget

A shard with no room should not answer 429. That status means the client exceeded a quota, and a client that ran into the shard's own capacity exceeded nothing — it carries no hint of when to return, so a well-behaved client retries immediately and deepens the shortage it just hit. Those refusals answer 503. The gateway's own limiter is the other case and keeps 429, because there the caller did exceed a quota (`api/errors.go`, `statusForError`, and api/README.md, "Errors and statuses"). Both carry `Retry-After`: the wait already spent when that is known, a default otherwise.

`admission_queue_wait_ms` is the budget a request may spend looking for capacity, not a delay. A queued waiter is promoted the instant a slot frees. The default is five minutes, and it is set from what a slot actually costs: across three days of load the median winning attempt held its slot 105 s and the p90 held it 317 s. A budget shorter than the 317 s p90 hold cannot reach most waiters before it runs out.

That ratio is where `admission_queue_per_slot` comes from: five minutes against a 105 s median hold drains about three deep, so four holds a burst without admitting waiters that provably cannot be served. Waiting the whole budget out and being refused anyway costs the client the wait and the shard the connection, so a request that provably cannot reach the front in time is refused on arrival instead. Depth is counted per model against that model's own concurrency, because the caps are per model: a heavy model does not shrink a light one's queue.

Two gates can refuse, and they answer different questions. The gateway limiter asks whether there is budget — concurrency, input tokens, chain weights. The scheduler asks whether a live host will take it — participant windows, breakers, chain phase. A request can pass the first and stall at the second: one burst answers zero limiter refusals against nine scheduler refusals. The budget bounds the limiter side only; the scheduler refuses immediately rather than waiting.

Escrow membership must reach this layer: without it `EscrowWeight` returns zero for every escrow, escrow selection fails on every request, and the gateway serves nothing while every health check stays green.

## Nonce dispositions

Settlement credits each slot with `assigned_nonces - protocol_misses` completed inferences, whatever GNK those nonces paid. Three gateway behaviours break that count: policy burns nonces without sending work, a sent request can go unfinished while its timeout never applies, and overscheduling turns one client request into several completed inferences. The nonce ledger exists to say which of those happened and how often; the accounting model is `proposals/gateway-dashboard`.

The disposition model follows that proposal; the event vocabulary is the gateway's own, built from the facts this gateway already produces, which are coarser than the reference's and carry the same information in fewer events.

| what the ledger folds | where it comes from |
|---|---|
| escrow membership, its latest nonce, and what the chain recorded per slot | a ten-second sweep of the published escrow set: `Snapshot` names the escrows, each session's `SnapshotState` carries the slot group, the latest nonce, and `HostStats` |
| a burned nonce and its reason | `tracedDispatches.GhostBurned`, which carries the nonce and one of seven reasons |
| every attempt of one race | `nonceAccountedRaces.RecordRace`: per attempt the nonce, whether it was sent, whether the protocol finished it, and whether the client got its answer |
| a timeout's kind, action and reason | `nonceAccountedRaces.RecordTimeout` |

Three of those are pushed and three are swept. Membership and host stats have no event of their own and change slowly, so reading them on a timer is both simpler and less invasive than a callback on every nonce; a race and a burn are single moments and must be told.

One race outcome replaces six of the reference's events, because it already aggregates what they report separately: a send, a winner, a loser, an unknowable usage, and the finish. One timeout event replaces two.

**A nonce names its own slot.** The reference reads applied diffs to learn which slot spent a nonce. That is unnecessary here: the chain's own convention makes the executor the slot at nonce modulo group size, so the ledger attributes a nonce arithmetically and needs no protocol-transition channel at all.

Two consequences follow from folding coarser events. The reference deduplicates replayed callbacks by using each event as its own map key, which requires every event to be a comparable struct; a race outcome carries a slice of attempts and cannot be one. It does not need to be: a race reports itself once, a burn is recorded once, and swept facts are idempotent by construction because each observation replaces the last. Neither a dedup map nor a comparable event struct is required.

**An unapplied timeout is not a settled one.** Every path out of `user.HandleTimeout` returns an error, including its own success — that error carries "this inference timed out" back to the request. So the error says nothing about whether the vote reached the chain, and `TimeoutResult.Applied` is what the ledger reads instead. The distinction is the whole point: a nonce whose timeout never gathered enough votes still settles as a completed inference for a participant that never answered, and recording it as a posted vote hides exactly that.

**An unfinished nonce is not a verdict.** The protocol can finish a nonce after the race that gave up on it, so the sweep re-asks the session about every nonce still counted as unfinished and lifts the ones that landed. The session's own outcome map survives sealing, so a negative answer means "not finished", never "no longer known" — the correction only ever moves a nonce out of the bucket settlement reads as failure, never into it.

**What the ledger cannot see.** A nonce is invisible to it between commitment and the end of the race that spent it, because a race reports only when it ends. Those nonces fall into `unobserved` alongside genuinely protocol-only ones, so that number is a floor on protocol overhead rather than a measurement of it; a baseline that grows while traffic is steady is the signal worth watching. `pending` is the separate case of a nonce seen unfinished whose timeout has not settled, and `overcounted` — classified beyond what the chain assigned — should never be anything but zero.

The gateway's vocabulary is richer where its facts are. Seven ghost reasons, not five: `participant_ejected_no_send`, `participant_outside_allowlist`, `participant_state_diverged_no_send` and `request_abandoned_before_dispatch` are first-class rather than an unknown reason.
