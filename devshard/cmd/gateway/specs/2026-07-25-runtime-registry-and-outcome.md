# The escrow runtime registry and RaceOutcome

Design addendum for the gateway rewrite, written before Phase 8 (engine) starts. It names two things the architecture needs, that four already-built packages depend on, and that currently exist nowhere — so that neither of them lands by default inside `api/` as an untyped field bag and regrows the 26-field `Gateway` god-struct this rewrite exists to remove.

## Why now

Four built packages already hold a hole shaped like the same missing object.

`scheduler.escrowSource.Candidates(model)` must return each candidate escrow with its `Session` handle, model, and in-flight user count. `escrow.SettlementSource` must answer whether an escrow is busy, finalize it, and build its settlement payload — all three are properties of a live runtime. `limits.Capacity.SetEscrowMembership` needs each escrow's per-host slot share and today has **zero callers**, which matters more than it looks: without membership, `EscrowWeight` returns zero for every escrow, so escrow selection fails on every request and the gateway serves nothing. `scheduler.Escrow.ActiveUsers` is declared and never incremented.

All four want the same map: escrow id to the live thing that can serve requests for it. Nobody owns it. The default outcome, if it stays unnamed, is that `api/` grows it as fields, and every later feature reaches into those fields the way the legacy `Gateway.attach*` methods reached into `rt.proxy.redundancy.*`.

## The registry

A new package, `cmd/gateway/runtime`, owning exactly one thing: the set of escrow runtimes and their lifecycle.

**What a runtime is.** An escrow id, its model, the `user.Session` that owns the nonce stream, the session's group and participant keys, the escrow's slot membership per host, an in-flight request count, and an active flag. It is the union of what the four consumers ask for, and nothing else — no HTTP, no racing, no chain client.

**Dependencies.** `runtime → store, user, signing, config`. It must not import `scheduler`, `engine`, `api`, `escrow`, `limits` or `perf`: every one of those is a consumer, and each already defines the narrow interface it needs on its own side. The registry satisfies those interfaces; `main` does the wiring. This is the same shape that resolved `engine ↛ escrow` and it is the reason those interfaces were defined consumer-side in the first place.

**Who fills it.** Three sources, in order of who acts first.

At startup, `main` reads the devshard registry from `store` and builds a runtime per active escrow — **lazily**, not eagerly. The legacy `buildRuntimes` fired one unbounded goroutine per escrow, each hitting chain REST, and produced LCD rate-limit storms at a hundred escrows. Lazy construction on first use is the design's own answer (§11), and any batch build must use a bounded worker pool. This is a hard invariant, already recorded, and it is the single easiest thing to get wrong here because the eager version is shorter to write.

During operation, `escrow.Manager` creates and retires escrows. It writes to `store` and must not gain a dependency on the registry — that would invert the arrows. The registry instead learns by re-reading the registry table on the same cadence it already needs for other reasons, or by a callback `main` installs into the manager. Prefer the callback: polling means a freshly created escrow is invisible to routing for up to a tick, and a freshly retired one keeps receiving traffic for the same window.

On demand, a settlement for an escrow that is no longer resident rehydrates a transient runtime, uses it, and closes it — the legacy behaviour, preserved because settlement must work for escrows that stopped serving.

**What it exposes.** Deliberately not one fat interface. It offers exactly the views its consumers already declared: candidates for the scheduler, the settlement triple for escrow, the membership push into `limits.Capacity`, and acquire/release of the in-flight counter for whoever dispatches. Each view is small enough that a fake in tests stays cheap — the property that made the Phase 6 interfaces work.

**Locking.** The registry is read on every request (candidate enumeration) and written rarely (create, retire, rehydrate). That is an `RWMutex` or a copy-on-write atomic pointer over the map. It must never hold its lock across a chain call, a session operation, or a store write — the same leaf-lock discipline that `perf.Ejected` now follows after it turned out to cost 2.35 ms per request at 500 hosts.

## RaceOutcome

The design calls this "one struct → perf + accounting + metrics (single recording point)". It does not exist in code. It should be defined **first** in Phase 8, before anything starts writing to those destinations independently, because the failure mode is five call sites that each record a slightly different subset and drift apart — which is exactly how the legacy code ended up with metrics nobody could reconcile.

**What it carries.** The request identity (request id, escrow, model), the per-attempt facts (participant, host index, nonce, whether it was the winner, whether it was a probe), the timing (send, receipt, first token, completion), the terminal classification (success, model outcome, transport fault, overload, cancelled), token counts and cost, and the two lifecycle signals the escrow manager needs (escrow missing, balance exhausted).

**Who writes it.** The engine, once, when a race completes — including when it completes badly. One construction site, so a new field is added in one place.

**Who reads it.** `perf.Tracker.RecordSample` and `RecordFirstToken` for latency and ejection; `limits.ParticipantLimiter.OnResult` for the AIMD window and breaker, where the verdict mapping matters — only host-caused failures may move the window, and a model-caused empty stream must never penalise a host; `store` for request accounting; `metrics` for the race families; and `escrow.Manager.OnBalanceExhausted` / `TriggerEscrowCheck` for the two lifecycle hooks. Note that the same outcome speaks three vocabularies today (`perf.Sample.Responsive`, `limits.Verdict`, and the design's terminal classification) — `RaceOutcome` is where that translation belongs, written once, rather than at each call site.

## What this changes in the plans

Phase 8 gains a first task: define `RaceOutcome` and the registry package skeleton, before the racing code. Phase 9's wiring list shrinks to actually wiring — the registry supplies `escrowSource`, `SettlementSource`, and the membership push, and `main` becomes the composition root that hands each package the view it declared.

Two smaller items belong to the same pass. The `SignerSource` implementation needs `os.Getenv`, which today lives only in `env/`; route it through a new `env.PrivateKey(name)` helper rather than letting a second package read the environment, and never wrap a decode error that could embed key material. And `store.Commitment.Epoch` is `uint64` while `store.DevshardRecord.RotationEpoch` is `int64`, converted at every persist — harmless while the value stays opaque, and exactly the sort of thing that becomes a silent truncation the first time someone does arithmetic on it.
