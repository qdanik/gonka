# Gateway Phase 4: perf — host performance tracking (research-grounded redesign) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** The `cmd/gateway/perf` package — per-(participant, model) latency estimates, health/ejection, first-token percentiles, capability flags, and an in-flight gauge — computed with standard, principled algorithms (not the old bespoke ones), exposing what the scheduler (host selection) and engine (hedging) phases consume.

**Architecture:** Phase 4 of 9 (design spec §14, §4). Perf has **no downstream consumers built yet** → no parity oracle, and full freedom to design correctly. This is a RESEARCH-GROUNDED REDESIGN. The old code (`cmd/devshardctl/hostperf.go`, `pairwise.go`, `perfstore.go`) is accreted, bespoke, and crude in specific citable ways; the research (`.superpowers/sdd/perf-research.md`, primary-source-cited: Finagle, Linkerd, Envoy, Dean & Barroso "Tail at Scale", gRPC A6, Mitzenmacher) established the standard alternatives. Approved by Daniil 2026-07-20.

**Tech Stack:** Go 1.24.2. Consumes `config.Config.Perf` (revised this phase). In-memory core; a minimal SQLite snapshot for warm-restart (`modernc.org/sqlite`). std otherwise.

## Global Constraints
All prior constraints hold (naming, error style, `-race -count=1`, gofmt/vet, no `os.Getenv` outside env/, **no mutable package-level state** — all tunables are config fields, terse comments, phase-end sweep, commits are Daniil's).

## Research verdicts → design (the "why", cited in perf-research.md)
1. **Latency: REPLACE windowed-mean → time-decayed peak-EWMA.** O(1) state/read vs O(256) scan; recency-correct; removes the old hourly tumbling-window "amnesia cliff." Keep the linear estimate `receipt_ms + cttfl_ms_per_token × input_tokens` (matches LLM prefill economics). Half-life sized to our sparse traffic (minutes, configurable) + a cold-host prior. [Finagle/Linkerd/Envoy peak-EWMA]
2. **Percentiles: KEEP store-and-sort (KISS)** — t-digest is gold-plating at N≈100. Fix the real defects: activation gate 100→~20, timestamped samples + staleness bound, cold-bucket interpolation, coarsen to 6 buckets.
3. **Health/ejection: REPLACE** — the old `any-failure→unresponsive(1.0)` + `100/1/0.01` predicates have no timed recovery, no min-volume, no backoff, no ejection cap (correlated failures can empty the pool). Adopt Envoy-shaped: consecutive-failure counter + failure-rate-with-min-volume, timed ejection with exponential backoff, and a max-ejection cap that keeps routing alive. [Envoy outlier detection]
4. **Hedging: DROP the pairwise machinery, keep the reactive p95 trigger.** First-token-p95 fallback IS tail-at-scale hedging — keep it (perf exposes the p95; the token-bucket budget + cancel-on-winner are the engine's Phase 8 job). The O(H²) pairwise/transitive-0.75-decay/A-B-sampling budget is a data-starved bespoke reinvention — **delete `pairwise.go` entirely**; its one real insight (paired confounders) is already covered by per-token normalization + buckets.
5. **Selection: P2C on `estimate × (inflight+1)`** — scheduler's job (Phase 7); perf exposes `Estimate` + the in-flight gauge.

## Design decisions (baked in)
- **Peak-EWMA scalars, not sample rings, for the latency metric.** The generic ring (Task 2, done) stays for the p95 first-token reservoir only.
- **In-memory core + minimal warm-restart snapshot** (NOT sample storage). Per-(participant,model) state is a handful of floats + ejection state; persist a compact periodic/shutdown snapshot to perf.db and reload at start. This eliminates the old sample-storage layer, the dead-`Prune()` bug, load-into-rings, and unbounded growth. The prune loop now just evicts stale (participant,model) rows past a staleness bound.
- **No pairwise, no accounting.** `pairwise.go` deleted (research). Accounting stays a separate concern for store/engine (spec §6/§14, off RaceOutcome).
- **Capability filters kept as-is** (context-limit, tool-unsupported) — correct, cheap, orthogonal.
- **Hedge token-bucket budget lives in the engine** (Phase 8); perf exposes `FirstTokenP95`. P2C lives in the scheduler (Phase 7); perf exposes `Estimate`, health, in-flight.

## File Structure
```
cmd/gateway/perf/
  doc.go
  ring.go        generic ring[T]  (DONE — Task 2)
  ewma.go        time-decayed peak-EWMA scalar: Add(value, now), Value(now) with decay-to-now; cold-prior
  sample.go      Sample{ParticipantKey, Model, Responsive, SendTime, ReceiptTime, FirstToken, TotalTime, InputTokens} + ReceiptMs/CTTFL derived
  host.go        hostKey (participant,model) + hostPerf{ewmaReceipt, ewmaCTTFL, decayed success/fail, consecutiveFail}; RecordSample; Estimate(inputTokens); health inputs
  ejection.go    Envoy-shaped ejection: consecutive-fail + rate-with-min-volume, timed {ejectedUntil, count} exponential backoff; Ejected(now); the max-ejection cap is enforced in tracker
  capability.go  context-limit + tool-unsupported maps + CannotServe (kept from old, correct)
  buckets.go     the ONE input-token bucketing (6 buckets)
  firsttoken.go  per-(model,bucket) timestamped p95 reservoir (ring[timedSample]); p95 by sort; gate ~20; staleness bound; cold interpolation
  inflight.go    per-participant in-flight gauge: Acquire/Release/Count
  tracker.go     Tracker façade: RecordSample/RecordFirstToken/RecordContextLimit/RecordToolUnsupported/Acquire/Release; queries Estimate/Ejected/Healthy/FirstTokenP95/CannotServe; max-ejection cap; config-driven; snapshot + prune-stale loop
  store.go       perf.db: compact per-(participant,model) state snapshot (NOT samples) — Save/Load; real prune of stale rows; goleak-clean loop
  config.go      (config package) revised config.Config.Perf
  *_test.go
```

## Tasks (TDD; each green + gofmt/vet + suite)

### Task 1 (REVISE): config.Config.Perf for the researched design
Replace the v1 Perf group (which had obsolete pairwise-decision fields) with:
- Latency/EWMA: `EWMAHalfLifeSeconds int64` (default 600 = 10 min), `ColdStartReceiptMs float64` (prior, e.g. 2000), `ColdStartCTTFLMsPerToken float64` (prior, e.g. 1.0), `PeakDecaySeconds int64` (peak-EWMA decay, default 600).
- Ejection: `ConsecutiveFailThreshold int` (5), `FailureRateThreshold float64` (0.15), `FailureRateMinVolume float64` (20 — decayed volume), `EjectionBaseSeconds int64` (30), `EjectionMaxSeconds int64` (600), `MaxEjectionFraction float64` (0.5), `MinAvailableHosts int` (1).
- First-token p95: `FirstTokenReservoir int` (100), `FirstTokenActivationSamples int` (20), `FirstTokenPercentile float64` (0.95), `FirstTokenStalenessSeconds int64` (86400).
- Buckets: keep the 6-bucket boundaries as config or consts (document).
- Health/staleness/snapshot: `HostStalenessSeconds int64` (3600 — evict host-model state unseen this long), `SnapshotIntervalSeconds int64` (60), `PruneIntervalSeconds int64` (300).
- **Remove** all `Pairwise*` fields and `WindowSize`/`RingCapacityMultiplier`/`UnresponsiveThreshold`/`FailureMinSamples`/`FailureAbsoluteMax`/`FailureRatio` (obsolete). Env: only `GATEWAY_PERF_EWMA_HALFLIFE_SECONDS` (coarse operational knob). Update Defaults/Validate (bounds + snake_case messages)/TestDefaultsMatchSpec/TestValidateCatchesEveryRuleBreach.

### Task 3: EWMA primitive (ewma.go)
Time-decayed peak-EWMA scalar. `Add(value float64, now time.Time)`: decay the current value toward `value` with weight from elapsed/half-life; PEAK variant — if `value > current`, jump to it (or bias toward peaks) then decay. `Value(now)`: current value decayed to `now` (so a host not sampled recently decays toward the prior, not held stale). Cold: before first sample, `Value` returns the configured prior. Unit-test: single-sample convergence, decay-over-time toward prior, peak sensitivity (a spike raises the value immediately), half-life correctness (after one half-life the deviation halves), monotonic time only.

### Task 4: hostPerf + Estimate (host.go, sample.go)
`Sample` + `ReceiptMs`/`CTTFL` derived (same formulas as old: receipt=(ReceiptTime−SendTime)ms; cttfl=(FirstToken−ReceiptTime)ms/InputTokens, guarded for zero/negative). `hostPerf` per (participant,model): ewmaReceipt, ewmaCTTFL, decayed success+fail counters, consecutiveFail, lastSeen. `RecordSample(s, now)`: updates the EWMAs (only from valid receipt/cttfl), success/fail counters (decayed), consecutiveFail (reset on success). `Estimate(inputTokens, now) float64` = ewmaReceipt.Value + ewmaCTTFL.Value × inputTokens (prior when cold). Unit-test the estimate math, cold-prior, counter decay, consecutiveFail reset.

### Task 5: Ejection (ejection.go)
Envoy-shaped, per hostPerf. Triggers: `consecutiveFail ≥ ConsecutiveFailThreshold`, OR (`decayedVolume ≥ FailureRateMinVolume` AND `failRate ≥ FailureRateThreshold`). On trigger: set `{ejectedUntil = now + min(EjectionBase × ejectionCount, EjectionMax), ejectionCount++}`. `Ejected(now)` = `now < ejectedUntil`. Recovery: when `ejectedUntil` passes, host rejoins; `ejectionCount` decays while healthy. The **max-ejection cap** (never eject below `MinAvailableHosts`, never more than `MaxEjectionFraction` of the pool) is enforced in the Tracker (it knows the pool). Unit-test: consecutive-fail ejection, rate ejection (incl. min-volume refusal on tiny samples), backoff ladder (base→2×→…→cap), recovery + count decay, and the tracker cap keeping the pool alive under correlated failure.

### Task 6: Capability (capability.go)
Port the correct parts: `RecordContextLimit(participant, maxTokens)`, `RecordToolUnsupported(participant)`, `ContextLimits()`/`ToolUnsupported()` snapshots, `CannotServe(participant, requiresTools bool, contextHint uint64) (reason string, blocked bool)` (tool-unsupported when tools required; context-limit-exceeded when hint>limit), `AllKnownToolUnsupported(participants)`. Unit-test each + CannotServe integration.

### Task 7: First-token p95 (firsttoken.go, buckets.go)
`inputBucket(tokens) int` — 6 buckets (document boundaries). Per (model,bucket): a `ring[timedSample]` (value=abs first-token ms, ts). `RecordFirstToken(model, inputTokens, firstTokenMs, now)`. `FirstTokenP95(model, inputTokens, now) (time.Duration, ok bool)`: filter samples newer than staleness bound; if count ≥ `FirstTokenActivationSamples` → p95 by sort; else `ok=false` (caller uses a per-token-estimate fallback). Unit-test: p95 correctness, activation gate at exactly the threshold, staleness exclusion, bucket boundaries, cold bucket → ok=false.

### Task 8: In-flight gauge (inflight.go)
Per participant: `Acquire(participant)`, `Release(participant)`, `Count(participant) int`. Concurrency-safe (atomic or mutex-mapped). Unit-test Acquire/Release balance, concurrent -race, Count.

### Task 9: Tracker façade + snapshot persistence + wiring (tracker.go, store.go)
`Tracker` composes hostPerf map + capability + firsttoken + inflight; config via `*config.Holder`. Public API (what scheduler/engine consume): `RecordSample`, `RecordFirstToken`, `RecordContextLimit`, `RecordToolUnsupported`, `Acquire`/`Release`, `Estimate(participant, model, inputTokens) float64`, `Ejected(participant, model) bool` (cap-aware), `Healthy`/health snapshot, `FirstTokenP95(model, inputTokens)`, `CannotServe(...)`. Enforce the max-ejection cap here. `store.go`: perf.db with ONE compact table (participant, model, ewma_receipt, ewma_cttfl, success, fail, consecutive_fail, ejected_until, ejection_count, last_seen) — Save (periodic + shutdown), Load at start; real `Prune()` evicting rows past staleness; a `Start(ctx)` loop for snapshot+prune with clean ctx-exit (goleak). Wire into main.go run() (construct with perf.db path + config Holder, Start the loop, shutdown after HTTP drain / before gateway store close). Unit-test the façade end-to-end (record→query), the ejection cap under a mass-failure scenario, snapshot round-trip (save→reload restores estimates+ejection), prune-stale, loop goleak-clean, concurrency -race.

## Definition of Done
- `go test ./cmd/gateway/perf/ -race -count=1` green; snapshot/prune loop goleak-clean; every algorithm unit-pinned (EWMA decay/peak, estimate, ejection triggers+backoff+cap, p95 gate+staleness, capability, inflight).
- `go test ./cmd/gateway/... -race -count=1` green; gofmt/vet; no `os.Getenv` in perf; no mutable package state; no pairwise, no accounting in perf/.
- Old devshardctl untouched.
- Phase-end comment sweep.
- Final report confirms the research-grounded shape: peak-EWMA (was windowed-mean), Envoy-ejection (was crude predicates), fixed p95 gating, pairwise DELETED, in-memory + compact snapshot (was sample storage + dead prune), O(H) not O(H²).

## What later phases consume
- **scheduler (Phase 7):** `Estimate`, `Ejected`, `CannotServe`, in-flight — P2C on `estimate × (inflight+1)` after filters.
- **engine (Phase 8):** `FirstTokenP95` for hedge-after-p95 + the token-bucket budget + cancel-on-winner; feeds `RecordSample`/`RecordFirstToken`/`Acquire`/`Release` from RaceOutcome.
- **api (Phase 9):** capability + health snapshots for debug endpoints; head-to-head/accounting telemetry lands here + in store/ off RaceOutcome.
