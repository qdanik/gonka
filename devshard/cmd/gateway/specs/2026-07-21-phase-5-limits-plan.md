# Gateway Phase 5: limits — GatewayLimiter v2 + ParticipantLimiter v2 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** The `cmd/gateway/limits` package — capacity-scaled gateway admission (GatewayLimiter v2, bounded-wait), a per-(host,model) AIMD concurrency window with a window-collapse breaker (ParticipantLimiter v2, replacing the old fixed quarantines), and the capacity math (scale factor + escrow effective weight) derived from the chain PhaseSnapshot.

**Architecture:** Phase 5 of 9 (design spec §9). Two NEW designs (not ports) — validated by research (`.superpowers/sdd/limits-research.md`: Netflix concurrency-limits, Envoy, Finagle, gRPC A6, RFC 6585, cited). Consumes `config.Config.Limits` (present), the chain `PhaseObserver.Subscribe`/`PhaseSnapshot` (Phase 3), and an injected availability predicate wired to `perf.Ejected` (Phase 4). Consumed later by scheduler (Phase 7 escrow/host pick) + api (Phase 9 admission).

**Tech Stack:** Go 1.24.2. std + config + chain types + an injected `func(participant, model) bool` availability callback. No SQLite (v2 state is NOT persisted).

## Global Constraints
All prior constraints hold: naming, error style, `-race -count=1`, gofmt/vet, no `os.Getenv` outside env/, **no mutable package-level state** (no `sharedParticipantRequestLimiter` global — the limiter is constructed and injected), **terse comments (DEFAULT ZERO — 1 line only where the name can't convey it; no doc-comment per func; trim as files land, no phase-end backlog), no bare doc.go**, phase-end sweep, commits are Daniil's, KISS/over-engineering audit at phase end.

## Research verdicts → design (cited in limits-research.md)
- **Per-host limiter = AIMD, not gradient.** We have an explicit overload signal (vLLM 429/503) + sparse/bursty per-host traffic + streaming — the exact case where AIMD wins and gradient (Netflix Gradient2 / Envoy adaptive_concurrency) fails (empty latency windows, stream-length ≠ congestion). Refinement: **utilization gate** — grow the window only when `inflight ≥ window/2` (Netflix AIMDLimit), so successes that never tested the limit don't inflate it.
- **Breaker = the AIMD window's Open state**, not a separate circuit-breaker object (Envoy circuit breakers ARE concurrency limits; Finagle FailFast; gRPC backoff). Trips on **transport faults only** (not 429/503 — those do ×0.5); window→0 for a gRPC-style backoff cooldown (`base × 1.6^count` + jitter, capped at `MaxOpenMS`); half-open = window 1; cooldown strictly SHORTER than perf's ejection horizon so perf remains the pool authority.
- **Gateway-wide bounded-wait admission** (semaphore + short bounded wait → 429 + `Retry-After`, RFC 6585) — standard (resilience4j SemaphoreBulkhead). CoDel/adaptive-LIFO is a documented future upgrade, not v1.

## BOUNDARY DECISIONS (the load-bearing architecture calls — flagged for veto)
1. **perf ↔ limits (Option B):** perf is the SOLE host-ejection (route/no-route) authority. limits does NOT re-implement any health/quarantine logic — the old quarantine machinery (429/503→60min, transport/EOF/empty/stalled→30min, probe/shadow/probation, persistence, ~600 lines) is DELETED. The limiter's only health-adjacent behavior is the breaker = window→0, which is a send-concurrency gate (synchronous, "how many WE send"), a different axis from perf ejection ("is the host in the pool"), on a shorter cooldown that perf ejection dominates. Only `HostFault` verdicts move the window/breaker; `ModelOutcome` (e.g. Kimi empty-stream) NEVER penalizes.
2. **chain ↔ limits:** limits SUBSCRIBES to `PhaseSnapshot` (weights current/full, preserved, blocked already there) and drops the old `phase_gate → CapacityState.Set*` push path. limits DERIVES what the snapshot doesn't carry: the scale factor `W_tot/W_ref` (availability-filtered), per-model share, and escrow effective weight `W(e)` (needs runtime escrow→slot membership — session topology, injected, not chain data). The old `CapacityState` triple-ingestion + `pocActive`-freeze branch are dead (snapshot already separates current vs full).
3. **availability is INJECTED, not imported:** `Capacity` takes an `available func(participant, model) bool` callback (wired to `!perf.Ejected` at construction) so limits has no hard `perf` import and is testable standalone.
4. **single factor policy for relaxed-PoC:** the scattered `capacityAwareLimitsEnabled() || !relaxedPoCBypassActive()` flags are replaced by ONE rule — scale→0 when `snapshot.RequestsBlocked` (relaxed-mode overlay decided at the api boundary), expressed as the capacity scale, not a bypass branch.

## File Structure
```
cmd/gateway/limits/
  gateway.go       GatewayLimiter v2: per-model in-flight + input-token budget, capacity-scaled, bounded-wait admission → 429+Retry-After
  participant.go   ParticipantLimiter v2: per-(participant,model) AIMD window + window-collapse breaker; Acquire/Release/OnResult
  capacity.go      Capacity: consumes PhaseSnapshot (+ injected availability + injected escrow membership) → scale factor, per-model share, escrow W(e)
  weights.go       the weight math (W_tot, W_ref, W(e), weightConcurrencyLimit) shared by gateway.go + capacity.go
  *_test.go
```
No `doc.go` — package comment atop `gateway.go`. All tunables from `config.Config.Limits` (present: MaxRequests, Concurrency.RequestsPer10000Weight/PoC, MaxInputTokensInFlight, AcquireWaitMS, AIMD.InitialWindow/MaxWindow, Breaker.TripThreshold/BaseOpenMS/MaxOpenMS, per-model ModelLimits). Window floor 1 + decrease ×0.5 + utilization gate ratio + breaker backoff ×1.6 are code constants (document); Retry-After derives from AcquireWaitMS.

## Tasks (TDD; each green + gofmt/vet + suite; injected clock for time)

### Task 1: weight math (weights.go)
Pure functions: `weightConcurrencyLimit(weight, per10000 float64) int64` = floor(weight×per10000/10000); `scaleFactor(currentAvailable, full float64) float64` = clamp(current/full, 0, 1) (0 when full=0 → treat as unlimited-baseline per old `scaleClampLimit`); `escrowWeight(currentWeights map[string]float64, membership map[string]float64, available func(string)bool)` = Σ w_current(h)·share(h)·avail(h). Unit-test each incl. the clamp edges, full=0, empty maps.

### Task 2: Capacity (capacity.go) — subscribe PhaseSnapshot, derive scale + W(e)
`Capacity` holds the latest `PhaseSnapshot` (updated via `Subscribe`), an injected `available func(participant, model) bool`, and injected escrow membership (`SetEscrowMembership(escrowID, map[participant]slotShare)`). Derives: `ScaleFactor(model) float64` (availability-filtered W_tot/W_ref for that model), `ScaleFactorAcrossModels() float64`, `EscrowWeight(escrowID, model) float64`. On each snapshot it recomputes cheaply (or lazily on read). Unit-test: scale from a canned snapshot with some hosts unavailable (available=false drops them from W_tot but not W_ref → scale<1); per-model; escrow W(e) with membership + availability; preserved-set handling (nil = all preserved); blocked → scale 0 path.

### Task 3: ParticipantLimiter v2 — AIMD window + breaker (participant.go)
Per (participant,model): `window float64` (init `AIMD.InitialWindow`, floor 1, cap `AIMD.MaxWindow`), `inflight int`, breaker state `{openUntil, consecutiveTransportFail, backoffCount}`.
```go
func (l *ParticipantLimiter) Acquire(participant, model string, now time.Time) bool  // false if breaker-open OR inflight >= floor(window); else inflight++
func (l *ParticipantLimiter) Release(participant, model string)                       // inflight--
func (l *ParticipantLimiter) OnResult(participant, model string, verdict Verdict, now time.Time) // AIMD + breaker updates
type Verdict int // Success | Overload(429/503) | TransportFault | ModelOutcome
```
- **Success:** if `inflight >= window/2` (utilization gate) → `window = min(window+1, MaxWindow)`; reset `consecutiveTransportFail`; if breaker was half-open and this succeeded → close breaker (backoffCount decays).
- **Overload (429/503):** `window = max(window*0.5, 1)`; does NOT trip the breaker; reset consecutiveTransportFail.
- **TransportFault:** `consecutiveTransportFail++`; if `>= Breaker.TripThreshold` → open breaker: `openUntil = now + min(BaseOpenMS × 1.6^backoffCount + jitter, MaxOpenMS)`, `backoffCount++`, window→0 (effectively). Half-open when `now >= openUntil` → allow exactly 1 (window treated as 1) as a probe.
- **ModelOutcome:** NO penalty (never moves window or breaker).
- NOT persisted; NOT goroutine-safe per-entry (the limiter owns a mutex over its map). Unit-test: additive increase gated by utilization; multiplicative decrease on overload (floor 1); breaker trips on N transport faults not on 429; backoff ladder + jitter bounded by max; half-open single-probe then close-on-success / re-open-on-fail; ModelOutcome never penalizes; Acquire blocks at window / when open; Release balances; concurrency -race. Assert the breaker cooldown default (`MaxOpenMS`) is < a documented perf ejection horizon so perf dominates (a comment/const cross-check, not a runtime dep).

### Task 4: GatewayLimiter v2 (gateway.go) — capacity-scaled admission + bounded-wait
Per-model in-flight cap + input-token budget, effective caps = baseline × capacity scale (or `weightConcurrencyLimit` when per-10k-weight configured). `AcquireForModel(ctx, model, inputTokens, cap LimiterModelCapacity) error` with a **bounded wait**: if the model is at cap, wait up to `AcquireWaitMS` (on a per-model condition/semaphore) for a slot; on timeout → `RateLimitError{RetryAfter}` (429 + Retry-After derived from AcquireWaitMS) + `RecordLimitRejection`. `ReleaseForModel(model, inputTokens)`. Scale from `Capacity` (scale 0 = block all; baseline 0 = unlimited). Drop the old dead `Acquire`/`Release`/`AcquireForModel`/dup snapshot methods — one Acquire path. Unit-test: admit under cap; block-then-admit when a slot frees within the wait; timeout→429+Retry-After; input-token budget; scale 0 blocks; per-10k-weight dynamic cap; per-model override; concurrency -race (bounded wait under contention, no leak/goleak on ctx-cancel).

### Task 5: Facade + wiring
A thin `Limiter` (or expose the three types directly) + main.go wiring: construct `Capacity` (subscribe the PhaseObserver from Phase 3, inject availability = a closure the api/engine later backs with perf; for now inject a default-all-available), `GatewayLimiter`, `ParticipantLimiter` from the config Holder; keep references for later phases (no fake consumer — if nothing consumes yet, construct + hold on a struct, do NOT `_ =`). Shutdown: the bounded-wait must release waiters on ctx-cancel. Unit-test the wiring path (construct from config, a request acquires/releases through the gateway limiter with a canned capacity). Metrics: reuse the preserved `devshard_gateway_limit_rejections_total` + `devshard_gateway_participant_limit_rejections_total` family names.

## Definition of Done
- `go test ./cmd/gateway/limits/ -race -count=1` green; bounded-wait goleak-clean; every algorithm unit-pinned (AIMD increase/decrease/utilization-gate, breaker trip/backoff/half-open, scale/W(e), bounded-wait timeout).
- `go test ./cmd/gateway/... -race -count=1` green; gofmt/vet; no `os.Getenv`; no mutable package state; no quarantine/persistence; no bare doc.go.
- Old devshardctl untouched.
- Phase-end comment sweep + KISS/over-engineering audit (challenge: is the facade needed; is weights.go pulling its weight; any exported method without a future consumer; breaker/perf overlap truly resolved).
- Final report confirms: AIMD (research-cited) not gradient; breaker = window-open (not a separate object); quarantines/persistence DELETED (~600 lines); capacity from PhaseSnapshot (not CapacityState re-impl); availability injected (no perf import); single factor policy.

## What later phases consume
- **scheduler (Phase 7):** `Capacity.EscrowWeight` (pick escrow with most spare W(e)), `ParticipantLimiter.Acquire` (per-host send gate during nonce advance), `Capacity.ScaleFactor`.
- **engine (Phase 8):** `ParticipantLimiter.OnResult` from RaceOutcome (HostFault→breaker/window; Overload→×0.5; Success→+1; ModelOutcome→nothing); half-open probes ride the hedger.
- **api (Phase 9):** `GatewayLimiter.AcquireForModel` (bounded-wait admission → 429+Retry-After) on the hot path; relaxed-PoC overlay decides the scale-0 policy here.
