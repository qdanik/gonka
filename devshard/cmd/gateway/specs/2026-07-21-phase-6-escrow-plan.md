# Phase 6 (escrow) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a clean modular `cmd/gateway/escrow/` package — escrow lifecycle (rotation, pre-PoC bridge swap, settlement, depletion replacement, crash-recovery, escrow-missing checker) — plus the small additive `chain/` and `store/` extensions it needs, replacing the old ~1990-line `cmd/devshardctl` escrow subsystem with a smaller, boundary-clean design.

**Architecture:** A `Manager` owns a 15s tick loop. Each tick: reconcile pending intent-commitments by tx hash (crash recovery), then — when rotation is enabled — read a `chain.PhaseSnapshot`, and either swap in temp/bridge escrows before the epoch's validator-set switch or restore regulars after PoC ends, then run a proactive depletion check. Settlement, signer resolution, and "is this runtime busy" are **injected consumer-side interfaces** (`SettlementSource`, `SignerSource`) that `api/` wires against the engine in Phase 9 — escrow never imports engine/. All chain access goes through `chain/`; all SQL through `store/`.

**Tech Stack:** Go 1.24, `devshard` module. Consumes the already-built `cmd/gateway/{chain,store,config,env}` and `devshard/signing`. Backing research (old-code cites, invariants, state machine): `.superpowers/sdd/p6-escrow-research.md` — implementers should read the sections referenced per task.

## Global Constraints

Every task's requirements implicitly include this section.

- **Dependency boundary:** `escrow → chain, store, config, signing` ONLY. NEVER import `engine`, `api`, `limits`, `perf`, or `devshard/bridge`. Runtime-owned capabilities (settlement build/finalize, busy-state, per-key signing) enter through the consumer-side interfaces defined here.
- **Money/determinism invariants (the parity FLOOR — see research §7, cite the line in the test):**
  1. No chain broadcast without a prior *durable* intent record: `onPrepared(txHash)` must persist the commitment and, if that persist fails, `CreateEscrow` must abort without broadcasting.
  2. `active=false` (deactivated) is a traffic flag only, never "settled." The settle-intent marker (`SettlementPending`) is set *before* the attempt and cleared *only* on `err == nil`.
  3. Escrow IDs come only from the chain create event, never invented locally.
  4. Re-registering the same escrow_id is idempotent success, not an error.
  5. "Not found" is concluded only when all endpoints agree; a transient endpoint error is never treated as not-found; a freshly-broadcast tx is protected by an 11-minute TTL grace (`unorderedTxTTL 9m + 2m`) before its commitment may be cleared.
  6. No swallowed errors on the money path: retry next tick, surface on a status row, or be an explicitly-documented no-op — never a silent drop.
  7. Settlement is deduplicated by an in-flight guard (own lock), independent of the chain's own duplicate rejection.
  8. Escrow-missing deactivation requires a positive, unambiguous chain "does not exist"; every other outcome (found, or any error) keeps the escrow active.
- **No `panic`/`Must*`; no `os.Getenv` outside `env/`; no mutable package-level state (no `var` function-pointer test seams — inject interfaces).**
- **Determinism:** iterate maps by sorted keys where order is observable; use the injected `now func() time.Time` for all time, never `time.Now()` directly in logic.
- **Comments — HARD RULE (calibrate against `cmd/gateway/limits/weights.go`):** default to ZERO comments. Write one only where the name+signature genuinely cannot convey a constraint (target ≤1 per ~15 lines). NEVER reference tasks, phases, plan docs, old files, or "ported from". State constraints, not provenance.
- **Tests:** table-driven, `t.Run` subtests, deterministic (injected clock, fake chain/store — no real network), pass under `-race`. Every money-path behavior ships a test that FAILS when the logic is wrong (research-checklist #144). Goroutine-spawning code is goleak-verified.
- **Test fixtures use abstract model placeholders** (`model-a`, `model-b`, `model-x` — the existing codebase convention), NEVER invented real-looking model IDs. No `llama-70b`/`qwen-235b`-style fictional names: the network's models are governance-registered (Kimi, MiniMax, Qwen families) and a fake-but-real-looking ID misleads readers. A model name in these tests is just a map key — keep it obviously abstract.
- **All new `context.Context` params are first; honor cancellation in loops and retries.**

## Shared interfaces (defined in `escrow/`, satisfied by real packages, faked in tests)

```go
// escrowTxClient is satisfied by *chain.TxClient.
type escrowTxClient interface {
	CreateEscrow(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(txHash string) error) (chain.CreateEscrowResult, error)
	SettleEscrow(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error)
	GetTxEscrowID(ctx context.Context, txHash string) (escrowID uint64, found bool, err error)
	GetEscrow(ctx context.Context, escrowID string) (info chain.EscrowInfo, found bool, err error) // added in Task 1
}

// escrowStore is satisfied by *store.Store.
type escrowStore interface {
	ListDevshards(ctx context.Context) ([]store.DevshardRecord, error)
	UpsertDevshard(ctx context.Context, record store.DevshardRecord) error
	SetDevshardActive(ctx context.Context, escrowID string, active bool) error
	SetDevshardSettlementPending(ctx context.Context, escrowID string, pending bool) error
	DeleteDevshard(ctx context.Context, escrowID string) error
	SaveCommitment(ctx context.Context, c store.Commitment) error        // added in Task 2
	LoadCommitments(ctx context.Context) ([]store.Commitment, error)     // added in Task 2
	DeleteCommitment(ctx context.Context, txHash string) error           // added in Task 2
	SaveRotationStatus(ctx context.Context, s store.RotationStatus) error // added in Task 2
	WithRetry(ctx context.Context, fn func() error) error                // added in Task 2
}

// snapshotSource is satisfied by *chain.PhaseObserver.
type snapshotSource interface{ Snapshot() chain.PhaseSnapshot }

// SettlementSource is wired by api/ against the engine-owned live runtime (Phase 9).
type SettlementSource interface {
	IsBusy(escrowID string) bool
	Finalize(ctx context.Context, escrowID string) error
	BuildSettlement(ctx context.Context, escrowID string) (chain.SettlementInput, error)
}

// SignerSource resolves a per-devshard private-key env-var NAME to a signer.
// Concrete impl (env-var name -> hex -> signing.SignerFromHex) is wired in main (Phase 9); it lives outside escrow/ so escrow never reads env.
type SignerSource interface {
	SignerFor(privateKeyEnv string) (*signing.Secp256k1Signer, error)
}
```

The `Manager` holds these plus `config.*Holder`, an injected `now func() time.Time`, and a `SettlementEnabled`/`RotationEnabled` view read from the config snapshot per tick.

---

## Task 1: chain/ escrow-support extension

**Files:**
- Modify: `cmd/gateway/chain/observer_fetch.go` (decode epoch-switch fields), `cmd/gateway/chain/snapshot.go` (new `PhaseSnapshot.EpochSwitchBlockHeight int64` + derivation)
- Create: `cmd/gateway/chain/escrow_query.go` (`TxClient.GetEscrow`), `cmd/gateway/chain/escrow_query_test.go`
- Test: extend `cmd/gateway/chain/observer_fetch_test.go` (or the snapshot test) for the new field

**Interfaces:**
- Produces: `PhaseSnapshot.EpochSwitchBlockHeight int64`; `func (c *TxClient) GetEscrow(ctx, escrowID string) (chain.EscrowInfo, bool, error)`; `type EscrowInfo struct { EscrowID string; Balance uint64 }` (extend only if a field is actually consumed).

**Behavior:**
- Decode `epoch_stages.set_new_validators`, `next_epoch_stages.set_new_validators`, `epoch_stages.next_poc_start` into the observer's epoch structs (currently absent — research §Interfaces/chain "What's missing"). Derive `EpochSwitchBlockHeight` with the old fallback ladder (research §3, old `phase_gate.go:932-947`): `EpochStages.SetNewValidators` → `NextEpochStages.SetNewValidators` → `NextPoCStart` → `LatestEpoch.PocStartBlockHeight`. It is the next validator-set-switch height, NOT an epoch index.
- `GetEscrow` mirrors `GetTxEscrowID`'s multi-endpoint discipline (research §7 invariant 5): query primary + fallbacks; `found=false` only when every reachable endpoint agrees the escrow is absent; a per-endpoint error is kept as `lastErr` and returned as `err` (never silently "not found"). The REST escrow-info path + response shape: see old `devshard/bridge/rest.go:133-160` for the endpoint and JSON.

**Tests (must fail if logic wrong):**
- Observer decodes the three new fields from a canned epoch JSON and `Snapshot().EpochSwitchBlockHeight` equals the `set_new_validators` height; fallback order verified by omitting the primary field and asserting the next one wins.
- `GetEscrow`: found (returns info, true, nil); confirmed-not-found (all endpoints 404 → false, nil); ambiguous (one endpoint errors, another 404s → err != nil, found==false is NOT concluded).

**Steps:** write failing observer-decode test → add fields+derivation → green; write failing `GetEscrow` table test (fake multi-endpoint transport) → implement → green; `-race`, gofmt/vet; commit.

---

## Task 2: store/ escrow persistence + retry wrapper

**Files:**
- Create: `cmd/gateway/store/commitments.go` (+`_test.go`), `cmd/gateway/store/rotation_status.go` (+`_test.go`), `cmd/gateway/store/retry.go` (+`_test.go`)
- Modify: `cmd/gateway/store/store.go` (additive DDL for the two new tables in `Open`)

**Interfaces:**
- Produces:
  ```go
  type Commitment struct { TxHash, Model, Role, PrivateKeyEnv, ProtocolVersion string; Epoch uint64; BlockHeight int64; CreatedAt time.Time }
  func (s *Store) SaveCommitment(ctx context.Context, c Commitment) error
  func (s *Store) LoadCommitments(ctx context.Context) ([]Commitment, error)
  func (s *Store) DeleteCommitment(ctx context.Context, txHash string) error
  type RotationStatus struct { Model, Role, Stage string; Epoch uint64; Completed bool; CreateError string; UpdatedAt time.Time }
  func (s *Store) SaveRotationStatus(ctx context.Context, st RotationStatus) error
  func (s *Store) LoadRotationStatuses(ctx context.Context) ([]RotationStatus, error)
  func (s *Store) WithRetry(ctx context.Context, fn func() error) error
  ```
- `WithRetry`: up to 10 attempts, 200ms backoff between, returns immediately on `nil`, aborts on `ctx.Err()`, returns the last error after exhausting attempts (research §2c, old `withDBRetry`). This REPLACES escrow-local retry and closes the old inconsistent-coverage gap (research §8 finding 2) — every escrow store write goes through it.

**Behavior:** new tables are additive gateway.db DDL created in `Open` (no chain proto, no upgrade handler — gateway-local SQLite). `LoadCommitments`/`LoadRotationStatuses` return deterministically ordered rows (ORDER BY a stable key).

**Tests:** commitment Save→Load→Delete round-trip incl. `CreatedAt` preserved; rotation-status upsert-by-(model,role); `WithRetry` — succeeds first try (1 call), retries a transient failure then succeeds, gives up after 10 and returns last err, aborts promptly on cancelled ctx (assert attempt count). Use a real temp store (fast) + an injected failing fn for retry.

**Steps:** per method, failing test → impl → green; `-race`; commit.

---

## Task 3: escrow/models.go

**Files:** Create `cmd/gateway/escrow/models.go`, `cmd/gateway/escrow/models_test.go`

**Interfaces:**
- Produces: `type ModelConfig struct { ModelID string; TempCount, TargetCount int; Amount uint64; PrivateKeyEnv string }`; `func parseModels(raw string) ([]ModelConfig, error)`; `func servedByNetwork(snapshot chain.PhaseSnapshot, modelID string) (served, known bool)`.

**Behavior:**
- `parseModels` unmarshals `config.Rotation.ModelsJSON` (old shape: `EscrowRotationModelSettings{ModelID, TempCount, TargetCount, Amount uint64, PrivateKeyEnv}`, old `gateway_store.go:93-99`). Empty/blank → empty slice, nil error. Validate: non-empty ModelID, TargetCount≥1, Amount>0 — return a wrapped error naming the offending entry; do NOT panic.
- `servedByNetwork` (research §8 finding 9 — no capacity/limits dependency): `known = (union of keys of snapshot.FullWeightsByModel and snapshot.CurrentWeightsByModel) is non-empty`; `served = modelID ∈ that union`. On cold start (`known==false`), callers treat the model as served (never skip before the observer has data).

**Tests:** parse valid multi-model JSON; blank→empty; malformed JSON→err; invalid entry (empty model / zero target)→err naming it; `servedByNetwork` served / not-served / cold-start(known=false).

---

## Task 4: escrow/breaker.go

**Files:** Create `cmd/gateway/escrow/breaker.go`, `cmd/gateway/escrow/breaker_test.go`

**Interfaces:**
- Produces: `type createBreaker struct{...}` (own `sync.Mutex`, NOT a shared manager lock — research §8 finding 3); `func newCreateBreaker() *createBreaker`; `gated(model, role string) bool`; `recordFailure(model, role string)`; `reset(model, role string)`.

**Behavior (research §4):** key = `model+"|"+role`. State `{failures, cooldownTicks}`. `gated` returns true (suppress) while `cooldownTicks>0`, decrementing it by one each call. `recordFailure` sets `cooldownTicks = min(2^(failures-1), 4)` and increments `failures`. `reset` deletes the entry entirely (Closed). Separate from `limits/`'s breaker by design (different timescale/purpose — research Q6).

**Tests:** fresh key not gated; after 1 failure gated for exactly 1 tick then not; escalation 1→2→4-cap across failures; `reset` clears (not gated, failures back to 0); `gated` decrements cooldown.

---

## Task 5: escrow/commitments.go — crash-recovery core

**Files:** Create `cmd/gateway/escrow/commitments.go`, `cmd/gateway/escrow/commitments_test.go` (mirror old `escrow_recovery_test.go`)

**Interfaces:**
- Consumes: `escrowTxClient` (CreateEscrow, GetTxEscrowID), `escrowStore` (SaveCommitment/LoadCommitments/DeleteCommitment/UpsertDevshard/WithRetry), `SignerSource`, `now func() time.Time`, `createBreaker`.
- Produces (methods on `Manager`): `createEscrow(ctx, model ModelConfig, role string, epoch uint64, blockHeight int64) error`; `reconcile(ctx) error`; `persistEscrow(ctx, escrowID string, model, role string, epoch uint64) error`.

**Behavior (research §2, invariants 1/3/4/5/6):**
- `createEscrow`: resolve signer via `SignerSource.SignerFor(model.PrivateKeyEnv)`; build a `store.Commitment{Model,Role,Epoch,PrivateKeyEnv,ProtocolVersion,BlockHeight}`; call `txClient.CreateEscrow(..., onPrepared)` where `onPrepared(txHash)` sets `c.TxHash=txHash; c.CreatedAt=now()` and `return store.WithRetry(ctx, func() error { return store.SaveCommitment(ctx, c) })`. If `CreateEscrow` returns a result, `persistEscrow(escrowID)`; if persist fails, DO NOT clear the commitment and return the error (reconcile recovers it). Invariant 1: an `onPrepared` error must abort before broadcast (guaranteed by `chain.TxClient`, research §7-1).
- `persistEscrow`: `WithRetry` → `UpsertDevshard(active=true, role, epoch)`; treat store's already-exists as success (invariant 4); on success `DeleteCommitment(txHash)` and `breaker.reset(model,role)`.
- `reconcile`: `LoadCommitments`; per commitment call `txClient.GetTxEscrowID(txHash)`:
  - found → `persistEscrow`; on persist success clear is done inside persist; on persist failure KEEP (retry next tick).
  - `found==false, err==nil` (committed-but-failed) → `DeleteCommitment` (terminal).
  - `errors.Is(err, chain.ErrTxNotFound)` → if `txMayStillLand(c)` (age ≤ 11min grace; zero `CreatedAt` ⇒ still pending) KEEP; else `DeleteCommitment`.
  - any other err → KEEP (retry next tick).
- Grace: `commitmentReconcileGrace = chain.UnorderedTxTTL + 2*time.Minute` (11m). If `chain` doesn't export the TTL, define the 9m locally with a comment tying it to `chain`'s value — prefer exporting from `chain`.

**Tests (mirror escrow_recovery_test.go — each pins an invariant):**
- Abort-when-commitment-write-fails: fake store fails all writes → `createEscrow` errors, zero devshards, zero commitments (invariant 1).
- Persist-failure-recovers-via-commitment: create succeeds on chain, first `persistEscrow` fails, commitment remains; next `reconcile` resolves by tx hash and persists (invariant 1/6).
- Reconcile: keeps fresh not-found (age<grace), clears expired not-found (age>grace), keeps on chain error, clears committed-but-failed, idempotent double-persist is success (invariants 4/5/6). Inject `now` to control age.

---

## Task 6: escrow/rotation.go

**Files:** Create `cmd/gateway/escrow/rotation.go`, `cmd/gateway/escrow/rotation_test.go`

**Interfaces:**
- Consumes: T3 (`parseModels`/`servedByNetwork`), T4 (`createBreaker`), T5 (`createEscrow`), `escrowStore.ListDevshards`, `retire` (T7, same package), `now`.
- Produces (methods on `Manager`): `prepareBridge(ctx, snapshot, models, devshards)`, `finishBridge(ctx, snapshot, models, devshards)`, `ensureToTarget(ctx, role string, target int, model ModelConfig, epoch uint64, snapshot, devshards) (created int, err error)`, `promoteRegularsToTemp(ctx, model, epoch, devshards)`.

**Behavior (research §1c/1d/3):**
- **State is loaded ONCE per tick** by the caller (Manager) and the `devshards []store.DevshardRecord` slice is passed down; `ensureToTarget` and the retire loops filter it in memory — NO per-model/per-direction reloads (fixes research §8 finding 1).
- `ensureToTarget`: count active records matching (role, epoch, model); if `count ≥ target` return; if `!served` (via `servedByNetwork`, and `known`) skip silently; if `breaker.gated(model,role)` skip; else loop `createEscrow` until `count==target`, and on the first error `breaker.recordFailure` + return (created-so-far, err).
- `prepareBridge` per model: `ensureToTarget(temp, TempCount)`. On error: `promoteRegularsToTemp` (degrade — relabel active regulars to temp role in place so bridge coverage exists), `SaveRotationStatus(stage=prepare_temp, Completed=false, CreateError)`, continue. On success: `retire` every active regular escrow for the model; `SaveRotationStatus(Completed = settleFailed==0)`.
- `finishBridge` per model: skip if no active temp with `RotationEpoch ≤ epoch`; `ensureToTarget(regular, TargetCount)`; on error `SaveRotationStatus(Completed=false)`, continue; on success `retire` temps with `RotationEpoch ≤ epoch`; `SaveRotationStatus(stage=finish_regular)`.
- All `SaveRotationStatus`/`UpsertDevshard` (incl. promote) go through `store.WithRetry` (fixes research §8 finding 2).

**Tests:** `ensureToTarget` creates up-to-N, skips when not served, skips when breaker-gated, stops+records-failure on error; `prepareBridge` retires regulars only after temp success, promotes on short; `finishBridge` creates regular + retires temp, skips model with no temp. Fakes for store/tx/signer; assert exact create/retire counts.

---

## Task 7: escrow/settlement.go

**Files:** Create `cmd/gateway/escrow/settlement.go`, `cmd/gateway/escrow/settlement_test.go`

**Interfaces:**
- Consumes: `escrowTxClient.SettleEscrow`, `SignerSource`, `SettlementSource`, `escrowStore` (SetDevshardActive/SetDevshardSettlementPending/DeleteDevshard/UpsertDevshard/WithRetry).
- Produces: the `SettlementSource` interface (declared here); methods on `Manager`: `settle(ctx, id string, opts settleOptions) error`, `retire(ctx, record store.DevshardRecord) error`; `type settleOptions struct { ChainID, FeeDenom string; FeeAmount, GasLimit uint64; KeyOverride string }`.

**Behavior (research §5, invariants 2/7):**
- `settle`: if `settlementSource.IsBusy(id)` → `SetDevshardSettlementPending(id,true)` and return `errDevshardBusy` (defer-and-retrigger; the drain hook re-invokes later — Phase 9 wires the hook). In-flight dedup via a `settlementInFlight map[string]bool` + own mutex (invariant 7); double-fire is a harmless no-op.
- Order (invariant 2): `SetDevshardActive(id,false)` (traffic flag) → `SetDevshardSettlementPending(id,true)` (intent) → resolve signer (`opts.KeyOverride` else record's `PrivateKeyEnv`) → `settlementSource.Finalize(ctx,id)` (if not already settled) → `input := settlementSource.BuildSettlement(ctx,id)` → `txClient.SettleEscrow(signer, input)` → on `err==nil` `SetDevshardSettlementPending(id,false)` (clear intent ONLY on success) → retire runtime bookkeeping. Any error before success leaves `SettlementPending` set.
- `retire`: if `SettlementEnabled` → `settle` then `DeleteDevshard`; else (toggle off) → `SetDevshardActive(false)` + `DeleteDevshard`, NO chain tx (research §5 toggle). Startup reconcile leaves the `SettlementPending` marker intact when settlement is disabled so a later re-enable still settles.

**Tests (pin invariants):** busy → `errDevshardBusy` + pending marked, no broadcast; happy path → active set false before finalize, Finalize+BuildSettlement+SettleEscrow called in order, pending cleared, deleted; broadcast error → pending NOT cleared; `SettlementEnabled=false` retire → no `SettleEscrow` call, just deactivate+delete; concurrent settle for same id → exactly one broadcast (dedup). Fakes record call order.

---

## Task 8: escrow/depletion.go

**Files:** Create `cmd/gateway/escrow/depletion.go`, `cmd/gateway/escrow/depletion_test.go`

**Interfaces:**
- Consumes: T1 `txClient.GetEscrow` (authoritative on-chain balance), T6 `ensureToTarget`/`createEscrow`, T7 `retire`, `escrowStore`, `now`.
- Produces (methods on `Manager`): `OnBalanceExhausted(escrowID string)` (reactive hook the engine/api calls), `checkDepletion(ctx, snapshot, devshards)` (proactive, run inside the 15s tick — NO separate 30s timer, per the approved decision), `replaceDepleted(ctx, record) error`.

**Behavior:**
- Reactive `OnBalanceExhausted`: dedup (in-flight set, own lock, like the checker); schedule `replaceDepleted` for the escrow's (model,role). Covers both balance- and nonce-limit depletion, since the engine detects those during a race and reports the id.
- Proactive `checkDepletion`: a **per-tick bounded scan** — a round-robin cursor on the `Manager` walks at most `depletionScanBudget` (a small const, e.g. 16) active escrows each tick, calling `txClient.GetEscrow(id)`; `found && Balance` at/below the depletion threshold → `replaceDepleted`. The budget caps chain-RPC RATE at a constant regardless of escrow count (100+ escrows sweep over several ticks, ~minutes — fine for a safety net; the reactive path handles urgent depletion immediately). NEVER scan every active escrow in one tick, and NEVER fan out unbounded per-escrow goroutines — that is the old `buildRuntimes` thundering-herd bug against the LCD/RPC (see Cross-task notes). On-chain balance (not runtime `sm.Balance()`) keeps it boundary-clean; nonce-limit depletion of an idle escrow is left to the reactive path (documented simplification — research §8 finding 8 / Q7).
- `replaceDepleted`: create a replacement of the same (model,role) via the T6 create path FIRST, then `retire` the old one (never leave zero coverage). Honors `SettlementEnabled` through `retire`.

**Tests:** reactive hook triggers exactly one `replaceDepleted` (dedup under concurrency); proactive scan replaces a below-threshold escrow and leaves an above-threshold one; replacement is created before the old is retired (assert order); `GetEscrow` error on a host keeps the escrow (fail-safe, no spurious replace).

---

## Task 9: escrow/checker.go

**Files:** Create `cmd/gateway/escrow/checker.go`, `cmd/gateway/escrow/checker_test.go`

**Interfaces:**
- Consumes: T1 `txClient.GetEscrow`, `escrowStore.SetDevshardActive`.
- Produces (methods on `Manager` or a small `Checker` it owns): `TriggerEscrowCheck(ctx, escrowID string)`.

**Behavior (research §6, invariant 8):** dedup via `inflight map[string]bool` + own `sync.Mutex` (no-op if a check for that id is running; cleared via `defer`). Call `GetEscrow(id)`: `found==false && err==nil` (positive, all-endpoints-agree not-found) → `SetDevshardActive(id,false)`; any `err` → keep active (log); `found==true` → keep active (host's "missing" report was false; log). ONLY the confirmed-not-found path mutates state.

**Tests (pin invariant 8):** 10 concurrent `TriggerEscrowCheck` for one id against a slow fake → exactly one `GetEscrow` and one deactivate (dedup); keeps-active-when-found; keeps-active-on-chain-error; deactivates-on-confirmed-not-found.

---

## Task 10: escrow/manager.go — lifecycle, tick, admin surface

**Files:** Create `cmd/gateway/escrow/manager.go`, `cmd/gateway/escrow/types.go` (SignerSource + `Status` view), `cmd/gateway/escrow/manager_test.go`

**Interfaces:**
- Consumes: everything above + `snapshotSource` (`chain.PhaseObserver.Snapshot()`), `config.*Holder`, `now`.
- Produces:
  ```go
  type Deps struct { Tx escrowTxClient; Store escrowStore; Snapshots snapshotSource; Settlement SettlementSource; Signer SignerSource; Config *config.Holder; Now func() time.Time }
  func NewManager(d Deps) *Manager
  func (m *Manager) Start(ctx context.Context) // starts the 15s tick goroutine
  func (m *Manager) Stop()                     // idempotent; blocks until the goroutine exits
  // admin ops api/ will call (Phase 9): Settle, Deactivate, Activate, Clean, ListEscrows, Import, plus OnBalanceExhausted, TriggerEscrowCheck, and Status() for /v1/debug/rotation
  ```

**Behavior (research §1a/1b):**
- Tick every 15s (`escrowRotationInterval = 15*time.Second`, a package const — cadence is plumbing, not configurable) plus an immediate first run. Per tick: `reconcile` ALWAYS (even when rotation disabled); then if `!RotationEnabled` return; validate settings (skip+log on invalid); `snapshot := snapshots.Snapshot()`, skip if cold (`EpochIndex==0 || BlockHeight==0`); load devshards ONCE; `blocksToEpochSwitch = snapshot.EpochSwitchBlockHeight - snapshot.BlockHeight`; if `0 ≤ blocksToEpochSwitch ≤ PrePoCBlocks` → `prepareBridge` and return (wins over the PoC-over branch); else if `!snapshot.RequestsBlocked`-equivalent PoC-inactive → `finishBridge`; then `checkDepletion`.
- Use **pull** (`snapshots.Snapshot()`), not `Subscribe` — a 15s pull is behaviorally equivalent for this cadence and avoids callback races (KISS; note this once).
- `Start`/`Stop` idempotent and guarded; `Stop` cancels the derived ctx and blocks on the goroutine's done channel (research §1a) so a config swap or shutdown never races a live tick.
- NOT wired into `main` — Phase 9 (api) wires `SettlementSource`/`SignerSource` and the drain-retrigger hook. This mirrors limits/perf (built + fully tested against fakes, unwired).

**Tests:** tick control flow with fakes — reconcile runs even when disabled; cold-start skip; pre-PoC window → `prepareBridge`; PoC-over → `finishBridge`; depletion runs after. `Start`/`Stop` idempotent and goleak-clean (no goroutine leak). Admin methods delegate correctly (Settle→settle, etc.).

---

## Cross-task notes

- Old-code cites for any behavioral question live in `.superpowers/sdd/p6-escrow-research.md` (§1-§7 map, §8 the improvements to bake in). The improvements are REQUIREMENTS here, not options: single per-tick state load (§8-1), uniform `store.WithRetry` (§8-2), breaker own-lock (§8-3), no package-var seams (§8-4), no `os.Getenv`/protocol-version-from-config (§8-5), injected `SettlementSource` (§8-6), snapshot-derived served-by-network (§8-9).
- **No per-escrow chain fan-out (RPC-herd invariant).** The old `buildRuntimes` (devshardctl `gateway.go:4210`) fired one unbounded `go buildRuntime()` per configured escrow at startup, each hitting chain REST → 100+ concurrent RPC → LCD/RPC rate-limit storms. The new gateway centralizes all chain reads through the single PhaseObserver (fixed pollers) + the single escrow tick; nothing here scales goroutines or RPC with escrow count. Enforce everywhere: reconcile stays serial (bounded by in-flight creates); the depletion scan is per-tick budgeted (T8). CROSS-PHASE INVARIANT for engine (P8) + main wiring (P9): runtime rehydration MUST be lazy (build on first use, design §11), and any batch build MUST use a bounded worker pool — never `go buildRuntime` per escrow unbounded.
- `request_accounting.go` is OUT of scope (it is request-outcome accounting → `store/`, research Appendix).
- Phase-end: KISS/over-engineering audit + comment sweep (remove any task/phase/old-file references) + comprehensive review, then hand off for the user's commit.
