# `perf` — what each host has been doing

Per-host history, and the one verdict derived from it that routing honours.

## What it owns

- **Samples and windows** (`sample.go`, `host.go`) — decayed success and failure counts, and the first-content quantile the escalation ladder measures a host against.
- **Ejection** (`ejection.go`) — Envoy-style outlier detection: a host far worse than its peers is taken out of the rota, capped so ejection can never remove more than a fraction of the hosts serving a model.
- **In-flight load** (`inflight.go`) — what each host is carrying right now.
- **Capability refusals** (`capability.go`) — counts of what a host's build refused: an unsupported protocol version, a tool call it does not implement, a context length it will not take, plus the smallest context it has admitted to.

## Boundaries

- **Capability refusals are counted, never routed on.** They are reported so an operator knows what to fix; nothing here withholds a host from the rota over one. A version refusal in particular would retire a host for good, because a gateway serves one protocol version for its whole life.
- **Ejection is capped by a floor of available hosts**, so it cannot empty a model.
- **Every restart starts clean.** No ejections, no counts, every window at its initial value — a divergence from the legacy gateway, argued in [`docs/rules.md`](../docs/rules.md).

## How the numbers are kept

Two shapes, for two different questions.

**Outcomes are decayed counters.** Success and failure each hold a running count and the time they were last touched; reading one multiplies it by `2^(-elapsed / halfLife)`. That answers "how has this host been doing lately" without keeping a history. A consecutive-failure integer sits beside them for the trigger that does not care about rates at all. When ejection trips, the outcome counters are reset, so the host is judged on what it does after the ejection rather than on the evidence that caused it.

**Latencies are a ring, not a decayed counter**, because the escalation ladder needs a *quantile* and a decayed counter cannot produce one. Sixty-four samples per host and model, and the p75 is refused until at least ten of them exist rather than reported from a window too short to mean anything.

## Ejection, and its two views

A host is ejected when either trigger fires: consecutive failures past the threshold, or a failure rate past its threshold once the window carries enough volume to be worth reading. The ejection lasts `base * ejectionCount`, capped at the maximum, so a repeat offender is out for longer each time. That count relaxes one rung per full healthy window since the last ejection ended, and the anchor advances with it so a long quiet stretch cannot cascade several rungs at once.

The pool-wide cap is `min(MaxEjectionFraction * hostsKnownForModel, hostsKnownForModel - MinAvailableHosts)`, floored at zero, and it is applied per model. Which hosts survive the cap is decided by ejection count, most-ejected first, with participant order breaking ties, so the cap keeps the least chronic offenders in rotation and the same set of ejections always yields the same routable set.

That cap is why there are two published views:

- **`Ejected`** is the capped verdict — whether routing actually withholds the host.
- **`Degraded`** is the verdict *before* the cap, so a host the cap had to leave in rotation is still known to be one the detector wanted out.

Both are rebuilt under the tracker's lock and published as atomic pointers, so a routing decision reads a map rather than taking a lock and scanning.

State for a host and model unseen for the staleness window is evicted, swept at most once per tenth of that window rather than on every sample. `Snapshot` takes its decode quantiles in one pass under one lock — asking per host would take the lock again each time and search the map twice for a report that wants a single moment — and reads the in-flight counts afterwards, because those live behind their own lock.

## What a capability refusal is keyed on

A tool call and a context length are properties of what the *model* asks for, so those refusals are keyed by participant **and** model. A protocol version is a property of the *build*, so that one is keyed by participant alone.

For context, the **smallest** refusal is the bound that holds: a later, larger refusal does not lift it. Each recorder reports whether the observation was new, which lets its caller log once instead of on every repeat, and log it outside the lock.
