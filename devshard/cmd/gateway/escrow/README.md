# `escrow` — the escrow's life

An escrow is funds on chain plus a group of hosts. This package creates one, keeps it honest across restarts and epoch boundaries, and closes it.

## What it owns

| File | What it holds |
| --- | --- |
| `manager.go` | the lifecycle: create, activate, drain, retire |
| `rotation.go`, `commitments.go` | replacing an escrow across the proof-of-compute boundary, and the intent recorded before the transaction so a crash cannot lose it |
| `settlement.go` | closing an escrow and paying what its hosts earned |
| `depletion.go` | noticing the funds will not cover the work in flight |
| `checker.go`, `dedup.go` | crash-recovery reconciliation: what the chain holds versus what this process recorded |
| `breaker.go` | refusing to keep creating escrows when creation keeps failing |

## Boundaries

- **Intent is written before the transaction, not after.** An interrupted rotation is recoverable only if the record of what was attempted survives the interruption.
- **An escrow parks before it is checked for busy, not after.** Otherwise a nonce can be committed between the two.
- **Settlement waits for the votes its nonces owe.** Concluding while one is in flight pays for work the chain has not yet judged.

## The tick

`Start` runs one tick immediately and then every `escrowTickInterval` (15 s). `Stop` cancels the context the tick runs under, so a tick already in flight is interrupted, and blocks until it has exited — it is a barrier for every caller. Both are idempotent: a second call is a no-op rather than a second loop.

Four steps run whatever the `Rotation.Enabled` toggle says, because each of them is about an escrow that already exists rather than about creating one:

| Step | Why it ignores the toggle |
| --- | --- |
| `reconcile` | crash recovery must not depend on a runtime toggle |
| `settlePending` | a parked escrow's row is the only record of which key can settle it, so nothing else will ever pick it up |
| `checkMissing` | an escrow the chain no longer holds must stop taking traffic |
| `checkDepletion` | an exhausted escrow must stop taking traffic; only creating its replacement is rotation's business, which is why `rotationModels` returns an empty set when rotation is off and no caller downstream has to re-read the toggle |

The chain snapshot is pulled from the observer once per tick rather than subscribed to: at this cadence a poll is equivalent and it avoids callback races. The devshard rows are likewise loaded once and passed down; the steps below filter that one slice rather than reloading it.

Only after those four does the bridge run, and only when rotation is enabled and the snapshot carries chain data (`EpochIndex` and `BlockHeight` both non-zero — otherwise it is a cold start). Within `PrePoCBlocks` of the epoch switch `prepareBridge` runs and wins even when PoC is also inactive; otherwise `finishBridge` runs while requests are not blocked.

## Creating an escrow

Every create — the bridge's, an operator's `CreateEscrow`, and a depleted escrow's replacement — goes down one path, so all three are recovered by `reconcile` in the same way. See [`docs/escrows.md`](../docs/escrows.md), "Creating an escrow".

1. Resolve the signer named by the model's `private_key_env`.
2. `onPrepared` writes the commitment row — model, role, epoch, key env, block height, tx hash, creation time. A failure there aborts before any chain broadcast: no broadcast without durable intent.
3. Broadcast, wait for the escrow id, register the devshard row, then drop the commitment.

`persistEscrow` resets the create breaker on both of its paths. An escrow found already registered is a create that succeeded, and leaving the breaker tripped would back off the next create for a failure that did not happen.

### Reconciling a commitment row

`reconcileOne` asks the chain what became of the commitment's transaction. The four answers mean different things:

| Lookup result | What it means | What happens |
| --- | --- | --- |
| found | the escrow exists | register it, drop the commitment |
| committed, no escrow event | terminal: no escrow will ever exist | drop the commitment |
| `ErrTxNotFound`, inside the grace window | the unordered tx may still land | keep the row, retry next tick |
| `ErrTxNotFound`, past the grace window | it can no longer land | drop the commitment |
| transport error | nothing is known — the endpoint is unreachable | keep the row, retry next tick |

The grace window is `commitmentReconcileGrace`: the chain's `UnorderedTxTTL` plus `commitmentIndexLagMargin` (2 min), which allows for a landed transaction staying unqueryable a little past its TTL. A row with a zero `CreatedAt` — malformed, or written before the stamp existed — counts as still pending, because keeping a commitment costs one row while dropping a live one costs an escrow nobody knows about.

`clearCommitment` takes the reason it logs rather than deriving it: the two callers know it, the row does not.

## The bridge across proof-of-compute

`prepareBridge` swaps temp escrows in ahead of an epoch switch; `finishBridge` swaps regulars back in once PoC is over. Both work one model at a time and isolate failures: a model that fails is recorded in the rotation status and skipped, never stopping the rest. `finishBridge` only acts on a model that actually has an active temp escrow, and `isActiveTemp` is both that gate and the retirement predicate, so the two can never disagree about which rows are bridged.

`ensureToTarget` creates up to the target count for one (model, role, epoch). It stops before creating anything in two cases:

- **The network serves no such model** — `servedByNetwork` reports known-and-not-served. That is logged, because it is a rotation that produced nothing by design: without the line an operator looking for the escrow that never appeared would find no reason anywhere. A cold start where both weight-by-model maps are empty reads as *unknown* rather than *not served*, so nothing is skipped.
- **The create breaker is gated**, which returns `errCreateSuppressed`. That is not "nothing needed": the breaker is gated exactly when creation has been failing, so `prepareBridge` must take its degrade path and keep the escrows it has instead of retiring them for replacements that were never created.

The degrade path is `promoteRegularsToTemp`: it relabels the existing regulars in place so the epoch still has bridge coverage, and keeps going past a write failure.

`deferredRetire` separates "not yet" from "failed". `ErrDevshardBusy` and `ErrSettlementInFlight` are both retried by the next tick, so surfacing them would make an error of every ordinary rotation; anything else is a real failure and reaches the tick.

### The create breaker

The breaker is keyed by (model, role). A failed create opens a cooldown of `escalatedCooldownTicks` — doubling per consecutive failure, capped at `maxCreateBreakerCooldownTicks` (4). The cooldown is counted in ticks, meaning calls to `gated`, not in wall-clock time, and a successful create resets the key.

## Replacing a depleted escrow

An exhausted escrow is exactly the one the load score prefers, because its in-flight count stays low while it fails every request. So `OnBalanceExhausted` takes it out of routing first and the tick asks questions afterwards.

Where the model is configured for rotation, the replacement is created **before** the depleted escrow is retired, so coverage never drops to zero; a failed create leaves the depleted escrow in place for the next attempt. Where no model is configured, the escrow is retired anyway and the fact is logged. The replacement always takes the `regular` role: inheriting a temp role would hand the next bridge an escrow to retire rather than the lasting coverage the depleted one was providing.

Replacement is refused when the snapshot carries no chain data yet. A replacement is keyed by the epoch that funded it, and an escrow created under an epoch-less snapshot is counted by no epoch at all — so the next bridge would fund a full set on top of it. Refusing re-marks the escrow, and the next tick with a snapshot tries again.

## Settlement and retirement

See [`docs/escrows.md`](../docs/escrows.md), "Settlement and retirement", for the states this walks through.

`retire` reads the `SettlementEnabled` toggle. With settlement off the escrow is only **parked**: its row carries the private-key env name that is the sole way to settle it later, so the row outlives retirement and is dropped only once a settlement has actually been confirmed on chain. With settlement on the escrow settles first, and the row is dropped only once the settle succeeded — a busy, deduped or failed settle leaves it registered, inactive and pending.

`park` writes inactive and settlement-pending in one statement, so a crash leaves the escrow recoverable, and only then stops routing to it — which is what the busy check that follows relies on.

`settle` runs in a fixed order:

1. **Dedup** on the escrow id. A concurrent caller gets `ErrSettlementInFlight` rather than success, because the row carries the only key that can settle the escrow and belongs to that other caller.
2. **Park** — before the reconciliation, not after. The caller deletes the row on success, and a row that is gone can no longer take the escrow out of routing, so an escrow put back by hand would keep serving with nothing left to un-publish it.
3. **Reconcile** against the transaction this escrow last broadcast (below).
4. **Busy check**. `ErrDevshardBusy` is a deferred-settle signal, not a failure: the now-retired escrow drains, and a retrigger settles it.
5. Resolve the signer, finalize, build the settlement input.
6. Record the tx hash before the broadcast, broadcast, and only then clear `settlement_pending`. Any earlier failure leaves it set for recovery.

`settlePending` drains at most `pendingSettleBudget` (4) parked escrows per tick; a busy or failing escrow simply stays parked. `Settle` is the operator's entry point onto the same path, so it carries the same dedup, deactivate-first ordering and busy check — and it drops the row itself, because retirement drops it on its own paths and the row's only purpose was naming the key this settle just used.

### Reconciling a settle that may already have landed

`alreadySettled` reads the hash and broadcast stamp the row carries and asks the chain about it:

| Answer | What happens |
| --- | --- |
| committed and succeeded | reported as settled; no second transaction is built |
| committed and rejected | the hash is cleared, so a retry is right |
| not found, inside the grace window | `ErrSettlementInFlight` — an unordered transaction stays landable for its whole TTL, and rebroadcasting inside that window pays a fee per tick for a settle still on its way, then loses to it and starts over |
| not found, past the grace window | the hash is cleared: a fresh transaction is right |
| transport error | fails the tick — an unreachable endpoint is not an answer, and must not be read as "the settle never happened" |

A settle row with no broadcast stamp defaults the opposite way to a commitment row: it counts as **past** the window. Keeping a commitment costs a row, while keeping a hash here costs an escrow that is never settled at all. Rebroadcasting one that was in fact still landing costs a fee once, and the stamp that write leaves governs every tick after it.

## Escrow checks

`TriggerEscrowCheck` deactivates an escrow only on a **confirmed** not-found. A lookup error and a found escrow both keep it active: ambiguity is never a reason to deactivate. Concurrent callers for the same id dedup to one check. When the chain does confirm the escrow is gone, routing stops before the row is written, so an escrow the chain confirms absent takes no further request even if that write fails.

## Keeping the request path off the chain

Two hooks are called from the request path — `OnEscrowMissing` and `OnBalanceExhausted` — and neither does I/O. Each only marks an escrow id in a `markSet`, and the next tick takes the whole set at once, which is what keeps a per-request event from fanning out into a per-escrow chain call.

`markSet.drain` steals the map rather than copying it, so a key marked while the tick is running belongs to the following tick and cannot be dropped. `mark` reports whether the key was new, which is how a depletion is logged once per tick rather than once per request. A step that fails re-marks its key, so a failed check never un-schedules itself.

`inFlightSet` is the other half: it dedups concurrent operations by key. The first caller enters and gets a `leave` func to call when done; a caller whose key is already in flight is told it is busy and should no-op.

## What this package expects of others

- `escrowTxClient`, satisfied by `*chain.TxClient`. `TxCommitted` is what tells a row still marked pending apart from one whose settle genuinely failed: the settle may have reached the chain after the wait gave up.
- `escrowStore`, satisfied by `*store.Store`; `snapshotSource`, satisfied by `*chain.PhaseObserver`.
- `SettlementSource`, wired by `api/` to the live engine runtime. `Retire` is synchronous — no nonce can be committed on the escrow after it returns — and that is what makes `IsBusy` monotone, so an idle answer stays true until the settlement it gates is broadcast. `Finalize` is idempotent.
- `ModelConfig`'s json tags are the `GATEWAY_ROTATION_MODELS_JSON` wire contract and are not renameable.

## Read next

- [`docs/escrows.md`](../docs/escrows.md) — the states, the transitions, and what each one costs.
