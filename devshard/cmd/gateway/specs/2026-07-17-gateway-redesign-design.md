# Devshard Gateway Redesign — Design Spec

Date: 2026-07-17. Status: approved in brainstorming, pending implementation plan. Replaces: `devshard/cmd/devshardctl` (111 Go files, ~41k LOC single `package main`). New home: `devshard/cmd/gateway`.

## 1. Context and goals

`devshardctl` is the gateway between the broker and race participants. It grew organically through bug-fixing (PR #1284 → #1289 → #1348 → #1427 → #1434 → #1435 → #1454/#1456) into a single 41k-line package with god-structs (`Gateway`: 24 fields + 5 mutexes; `inflight`: ~90 fields), 250–330-line functions, cross-layer field poking (`Gateway.attach*` writes into `rt.proxy.redundancy.*`), mutable package globals, config spread across three planes, and a two-mux internal re-dispatch on the hot path. Adding routing features (e.g. KV-cache affinity) is currently hard because target selection is spread across three uncoordinated points (`reserveRuntimeForModel`, `sessionPicker.run`, `Redundancy.Decide`).

Goals, in priority order:

1. Simple modular architecture with clear ownership boundaries, so features like KV-cache-affinity routing become local changes.
2. Full rewrite of internals (user decision), with old *behavior* preserved as explicit contracts and tests — not old code.
3. A highload-polite gateway: it must not DDoS participants, the chain REST, or the public API.

## 2. Decisions log (brainstorming outcomes)

| # | Question | Decision |
|---|---|---|
| 1 | Compatibility | **External HTTP API preserved** (broker- and participant-facing routes and schemas). Env/config/state layout redesigned from scratch; deploy scripts will be updated. |
| 2 | KV-cache affinity routing | **Extension point only** (`AffinityHint` in scheduler). The feature itself is a follow-up project. |
| 3 | Single-mode | **Dropped.** Multi-only; one escrow = pool of one. Top-level single-mode aliases (`/v1/finalize`, `/v1/state`, top-level `/v1/debug/*`) removed; everything remains reachable under `/devshard/{id}/...`. |
| 4 | Config model | **Immutable snapshot** built from three sources: defaults ← env ← admin overrides (SQLite). Hot apply = rebuild + `atomic.Pointer[Config]` swap. No mutable globals. |
| 5 | Approach | **Full rewrite including the race engine.** Old tests/fixtures become behavioral contracts; golden-parity harness for filters. |
| 6 | Limiters | **New designs** (not ports): AIMD per-host concurrency + soft circuit breaker instead of fixed 30–60 min quarantines; bounded-wait gateway limiter. |

## 3. Scope and non-goals

In scope: everything currently inside `devshard/cmd/devshardctl`, rebuilt in `devshard/cmd/gateway` as the packages listed in §4.

Non-goals:

- No changes to shared `devshard/` module packages (`user`, `state`, `bridge`, `signing`, `types`, `transport`, `logging`, host-side stack). The gateway consumes them as-is.
- No chain/protocol changes. The nonce→executor binding (`executor = nonce % group`) stays; the scheduler models it.
- No KV-cache feature implementation.
- No deletion of `devshardctl` in this project (separate PR after cutover; the old package is touched only to add the golden-corpus generator test).
- Prometheus metric names are **not** renamed — dashboards and alerts keep working.

## 4. Package map and dependency rules

```
cmd/gateway/
  main.go        entry: env.Load → config.Build → wire constructors → serve (~100 lines, no logic)
  env/           the ONLY place that reads environment variables; typed table: name, type, default, description
  config/        Config snapshot: defaults + constants, validation, merge (defaults ← env ← overrides), atomic publish
  api/           HTTP boundary: routes, typed request/response schemas, auth middleware, OpenAPI, response cache, kill-switch
  filters/       request/response boundary: parameter rule catalog (single registration table), model profiles, response stripping
  scheduler/     target selection behind one interface: escrow pick + host pick + nonce burn mechanics; AffinityHint extension point
  engine/        race engine rewrite: attempt state machine, race coordinator, escalation policy, SSE classifier, detach/drain
  escrow/        escrow lifecycle: rotation, settlement, intent-commitment crash recovery, per-(model,role) create breaker
  chain/         chain I/O: TxClient (sign/broadcast/query) and PhaseObserver (epoch/participant polling + subscriptions)
  limits/        GatewayLimiter v2 (capacity-scaled admission) and ParticipantLimiter v2 (AIMD + breaker + hedged probes)
  perf/          per-host perf rings (receipt / first-token / CTTFL / total), pairwise comparator, capability flags, prune
  store/         SQLite: gateway.db (registry, overrides, suspicious hosts, intent commitments, rotation status) and perf.db (samples, accounting); debug capture sink
  metrics/       prometheus registry; all existing devshard_* families preserved by name
  logging/       thin wrapper over devshard/logging (request-id context)
```

Dependency rules (arrows are one-way; no cycles):

- `api → filters, limits, scheduler, engine, escrow (admin ops), config`
- `engine → scheduler, perf, limits, metrics`
- `scheduler → chain (phase snapshots), perf, limits`
- `escrow → chain, store`
- everything → `config` (snapshot read), `metrics`, `logging`
- nobody → `api`; `engine ↛ escrow`; `filters` depends only on `config`
- `os.Getenv` outside `env/` is forbidden (review rule). Mutable package-level state is forbidden everywhere; the only process-wide mutability is the config snapshot pointer and store contents.

## 5. HTTP API surface (`api/`)

External contract preserved from devshardctl, minus single-mode. Every route handled by one mux; `/devshard/{id}/...` and pooled `/v1/...` resolve to the same handlers with an explicit target argument — no request cloning, no second mux, no internal re-dispatch.

Public routes: `GET /v1/models`, `POST /v1/chat/completions`, `GET /v1/status`, `GET /metrics`, and per-escrow `GET /devshard/{id}/` (swagger), `GET /devshard/{id}/openapi.json`, `/devshard/{id}/v1/models|chat/completions|status|state|finalize (GET/POST)|requests/{request_id}`.

Admin routes (bearer `GATEWAY_ADMIN_API_KEY`): `/v1/admin/state`, `/v1/admin/settings` (GET/PUT — overrides), `/v1/admin/devshards`, `/v1/admin/devshards/{id}/...` (settle/activate/deactivate/clean/participants/import), `/v1/admin/escrows`, `/v1/admin/suspicious-hosts`, `/v1/admin/participants/unquarantine`, `/v1/debug/rotation`, `/v1/debug/memstats`, per-escrow `/devshard/{id}/v1/debug/*` (pending/state/inferences/perf/pairwise/signatures, signatures/collect, sync-hosts), `/debug/pprof/*`.

Removed (decision #3): top-level `/v1/finalize`, `/v1/state`, top-level `/v1/debug/{pending,state,inferences,perf,pairwise,signatures,signatures/collect,sync-hosts}`.

All request/response bodies get named Go types in `api/` with json tags and doc comments; the OpenAPI spec is generated from those types (single source of truth). Model access modes (`open`/`api_key`/`admin_only`) enforced per-model in one middleware. Kill-switch (disabled flag + optional redirect message/URL) is an `api/` middleware reading the snapshot.

## 6. Hot path

```
POST .../v1/chat/completions
  api:       request-id → model access check → decode into typed schema
  filters:   Request pipeline → 400 with capture on reject | normalized body + forced fields
  cache:     response-cache lookup on the normalized body → short-circuit before consuming any limiter slot
  admission: PhaseSnapshot predicate (requests blocked during PoC) → cheap reject before queueing
  limits:    GatewayLimiter.Acquire(model, inputTokens) → bounded wait → 429 + Retry-After on timeout
  scheduler: Pick(profile) → Assignment{escrow, host, nonce}
  engine:    Run(assignment, body, clientW) → race, stream winner
  filters:   response stream stripped of internal fields on the fly
  outcome:   one RaceOutcome struct → perf + accounting + metrics (single recording point)
```

## 7. Engine (`engine/`)

Replaces `redundancy.go`. One logical request = a race of 1..N attempts against distinct participants.

**Attempt state machine.** States: `Pending → Dispatched → Receipt → FirstToken → Streaming → Terminal`, with `Terminal ∈ {Won, Lost, HostFault(reason), ModelOutcome(kind), Stalled}`. Each attempt runs in its own goroutine and communicates only via events (receipt, first-token, chunk, done, error) on a channel to the coordinator. No shared mutable attempt struct across goroutines.

**Race coordinator.** One event loop per request: consumes attempt events, consults the escalation policy, crowns exactly one winner, pipes winner bytes to the client writer, cancels/drains losers in the background, and emits one final `RaceOutcome`.

**Escalation policy.** `EscalationPolicy` interface: given race state (elapsed, attempt states, per-host perf/pairwise stats), returns the next action (add attempt now / at deadline / never; give up). One implementation configured from the snapshot replaces today's legacy/pairwise/hybrid switch; winner-hold (brief wait for a known-faster host before crowning) is part of the policy.

**SSE classifier.** A standalone component: consumes an attempt's byte stream incrementally (bounded by classify byte caps), yields verdicts: content / empty / error, and fault attribution — `HostFault` (feeds ParticipantLimiter) vs `ModelOutcome` (Kimi reasoning-burn empty streams, short content — host is innocent). All battle-tested classification heuristics live here with fixture tests.

**Client detach.** Two contexts per race: client ctx and race ctx. Client disconnect does not kill the race; it continues up to `DrainTimeout` (today's meta-drain semantics: finish reading host SSE so receipts/accounting/nonce bookkeeping complete), implemented with contexts and deadlines, not a hand-rolled cancel flag.

**Probes.** HalfOpen hosts (see §9) are included as extra race attempts alongside a healthy favorite; probe success/failure feeds the breaker without ever costing user latency.

**Lifecycle signals.** Escrow-missing and balance-exhausted conditions detected during a race are reported in `RaceOutcome`, not acted on by the engine. The `api` layer forwards them to the escrow manager, which verifies against the chain (deduplicated check, fail-safe on ambiguity — today's EscrowChecker semantics) and deactivates/replaces the devshard. This replaces devshardctl's `onEscrowMissing`/`onBalanceExhausted` callback poking and keeps `engine ↛ escrow`.

## 8. Scheduler (`scheduler/`)

Hides the protocol constraint: executors are bound to nonces (`executor = nonce % group`), nonces are issued in order by `user.Session`, so "choosing a host" = advancing nonces and burning the ones bound to unusable hosts (ghost probes for PoC-required, breaker-open, window-exhausted, capability-blocked, excluded hosts).

```go
type Scheduler interface {
    Pick(ctx context.Context, profile RequestProfile) (Assignment, error)
}
type RequestProfile struct {
    Model        string
    InputTokens  int
    Exclude      []ParticipantKey // already raced in this request
    AffinityHint *AffinityHint    // future KV-cache: "prefer host X if cheap"
}
type Assignment struct { Escrow EscrowID; Host HostRef; Nonce PreparedNonce }
```

Selection order: escrow by spare effective weight `W(e)` (capacity view from PhaseObserver data + live availability from ParticipantLimiter; ties round-robin; all-zero ⇒ 429-class error), then host filters, then nonce advance/burn. Nonce-limit (~19.8k per escrow) and `acceptsNewInferences` gating live here. Exhaustion returns explicit `ErrNoAvailableHost` / `ErrAllHostsExcluded`. KV-affinity later = an extra stop-condition during nonce advance; the interface does not change.

## 9. Limits (`limits/`) — new designs

**ParticipantLimiter v2** (per participant×model) — adaptation instead of punishment:

1. **AIMD concurrency window**: how many parallel sends *we allow ourselves* to a host. Success → +1 (up to cap); 429/503 → ×0.5 (floor 1). An overloaded host immediately and automatically receives less traffic — the gateway physically cannot DDoS a participant into the ground.
2. **Circuit breaker with soft backoff** for hard failures only (N consecutive transport errors): `Closed → Open(5s → 10s → 20s … cap 3–5 min) → HalfOpen(probes) → Closed`. No fixed 30–60 min sentences; worst case is minutes and only for repeated confirmed downs.
3. **Hedged recovery probes**: HalfOpen hosts ride as extra attempts in real races next to a healthy favorite (§7). Success reopens the window quickly; failure extends backoff; the user never waits on a probe.

Only `HostFault` verdicts from the engine move the window/breaker; `ModelOutcome` never penalizes a host. State is **not persisted** — restart = clean slate (with minute-scale backoff, self-healing beats replaying stale penalties). Suspicious-host pinning (admin list) remains a persisted override in store.

**GatewayLimiter v2** (protects the gateway and aggregate capacity): per-model in-flight request cap + input-token budget, both from snapshot × capacity factor (PhaseObserver subscription; PoC shrinks capacity). Short bounded queue (hundreds of ms) absorbs bursts; timeout → 429 with `Retry-After`. PoC relaxed-mode / capacity-aware toggles express as factor policies here, not scattered bypass flags.

## 10. Chain (`chain/`)

**TxClient** — same semantics as today's hand-rolled REST client, behind an interface: build protobuf msgs (`MsgCreateDevshardEscrow`, `MsgSettleDevshardEscrow`), unordered txs with 9-minute TTL, precompute tx hash and invoke `onPrepared(hash)` **before** broadcast (intent persistence hook), BROADCAST_MODE_SYNC, poll for confirmation, resolve create-tx → escrow_id from events, query across primary + fallback endpoints, fail-closed on 404 only when all endpoints agree, distinguish not-found vs committed-but-failed.

**PhaseObserver** — polls public API (epoch info, participants, weights, preserved sets, versions) every ~5s, publishes an immutable `PhaseSnapshot` (block height, epoch index/phase, confirmation-PoC phase, per-host weights, preserved sets, requests-blocked verdict + reason). Consumers **subscribe** (scheduler: weights/preserved; limits: scale factor; escrow: epoch boundaries; engine: speculative-attempt policy inputs). Admission check ("requests blocked during PoC phase X") becomes a snapshot predicate evaluated in `api/`, not a per-devshard gate call.

## 11. Escrow (`escrow/`)

Manager owning the rotation loop; semantics preserved exactly: intent commitment saved to store before broadcast → tick (15s) reconciles commitments by tx hash (found → persist devshard + clear; failed → clear; 404 within TTL → keep waiting); pre-PoC window (N blocks) → create bridge (`temp`) escrows and retire regulars; PoC over → create regulars, retire temps; idempotent create-to-target gated by per-(model,role) exponential breaker; skip models not served by the network; settlement path blocks on busy runtimes, lazily rehydrates non-resident escrows, finalizes, broadcasts settlement JSON, retires; leftover routing rules unchanged. DB writes go through the store retry wrapper.

## 12. Config (`env/` + `config/`)

`env.Load()` reads every variable in one pass into a typed `EnvValues`; each var declared once with name, type, default, description. `config.Build(values, overrides)` produces an immutable `*Config`, validated fail-fast at startup (bad config = process refuses to start). Admin `PUT /v1/admin/settings` writes overrides to store, rebuilds, validates, atomically swaps the snapshot pointer; subscribers (limits, engine policy, escrow targets) observe the new snapshot on next read or via a change notification. Per-model limits are part of the snapshot: the output-token pair plus optional per-model `max_concurrent_requests` and `max_input_tokens_in_flight` (nil = inherit global). Repeated-prefix limit fields are grouped into sub-types (`Concurrency`, `AIMD`, `Breaker`) in the Config struct; override JSON keys stay flat.

Env naming unified under `GATEWAY_*`. Mapping (old → new):

| Old | New |
|---|---|
| `DEVSHARD_PORT` | `GATEWAY_PORT` |
| `DEVSHARD_STORAGE_PATH` / `DEVSHARD_STORAGE_DIR` | `GATEWAY_STORAGE_DIR` |
| `DEVSHARD_API_KEYS` | `GATEWAY_API_KEYS` |
| `DEVSHARD_ADMIN_API_KEY` | `GATEWAY_ADMIN_API_KEY` |
| `DEVSHARDS_JSON` | `GATEWAY_DEVSHARDS_JSON` |
| `DEVSHARD_ESCROW_ID` / `DEVSHARD_PRIVATE_KEY` | dropped (single-mode); topology comes from `GATEWAY_DEVSHARDS_JSON` / store; per-devshard `PrivateKeyEnv` indirection kept |
| `DEVSHARD_CHAIN_REST` | `GATEWAY_CHAIN_REST` |
| `DEVSHARD_PUBLIC_API` | `GATEWAY_PUBLIC_API` |
| `DEVSHARD_MODEL` | dropped — `model` is required in pooled requests (400 without it); per-escrow routes fall back to the escrow's own model |
| `DEVSHARD_TX_QUERY_REST` | `GATEWAY_TX_QUERY_FALLBACK_URLS` — comma-separated list of independent tx-confirmation cross-check endpoints (default `http://node1.gonka.ai:8000/chain-api`), polled only during active tx confirmation with backoff |
| `GATEWAY_DEFAULT_MAX_TOKENS` / `GATEWAY_MAX_TOKENS_CAP` | unchanged |
| `GATEWAY_MAX_CONCURRENT_REQUESTS` | unchanged |
| `GATEWAY_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT` | dropped from env — admin override only (`max_concurrent_requests_per_10000_weight`) |
| `GATEWAY_POC_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT` | dropped from env — admin override only (`poc_max_concurrent_requests_per_10000_weight`) |
| `GATEWAY_MAX_INPUT_TOKENS_IN_FLIGHT` | dropped from env — admin override only (`max_input_tokens_in_flight`) |
| `DEVSHARD_TX_*` (gas, fee denom/amount, poll interval/timeout) | `GATEWAY_TX_FEE_AMOUNT` / `GATEWAY_TX_GAS_LIMIT` survive as env; fee denom and poll interval/timeout are config constants |
| `DEVSHARD_CHAIN_ID` | `GATEWAY_CHAIN_ID` |
| `DEVSHARD_GATEWAY_DISABLED` (+`_MESSAGE`/`_NEW_URL`) | `GATEWAY_DISABLED` (+`_MESSAGE`/`_REDIRECT_URL`) |
| `DEVSHARD_ESCROW_ROTATION_*` (enabled, settlement, pre-PoC blocks, models JSON) | `GATEWAY_ROTATION_ENABLED` / `GATEWAY_ROTATION_SETTLEMENT_ENABLED` / `GATEWAY_ROTATION_MODELS_JSON` survive as env; pre-PoC blocks dropped from env — admin override only (`rotation_pre_poc_blocks`) |
| `DEVSHARD_POC_REQUEST_MODE` / `DEVSHARD_CAPACITY_AWARE_LIMITS` | `GATEWAY_POC_MODE` / `GATEWAY_CAPACITY_AWARE_LIMITS` |
| `DEVSHARD_CHAT_CACHE_MAX_BYTES` | `GATEWAY_CHAT_CACHE_MAX_BYTES` |
| `DEVSHARD_MAX_CONCURRENT_RUNTIME_BUILDS` | dropped from env — config constant (`Server.MaxConcurrentRuntimeBuilds`) |
| `DEVSHARD_ROUTE_PREFIX` | dropped — derived from the protocol version in code, not configuration |
| `DEVSHARD_META_DRAIN_TIMEOUT_SECONDS` | dropped from env — config constant (`Stream.DrainTimeoutSeconds`) |
| `DEVSHARD_REQUEST_CAPTURE_*` / `DEVSHARD_CAPTURE_SHORT_CONTENT_*` | `GATEWAY_CAPTURE_ENABLED` / `GATEWAY_CAPTURE_DIR` survive as env; short-content thresholds are config constants |
| `GATEWAY_CLASSIFY_MAX_*_BYTES` | dropped from env — config constants (`Stream.ClassifyMax{Attempt,Participant,Global}Bytes`) |
| new | limiter v2 knobs (AIMD caps, breaker ladder, queue wait) exist as config defaults + admin overrides — deliberately NOT env |

Env diet (decided 2026-07-18): only 24 variables survive as env — deployment identity (port, storage, keys, topology, endpoints, tx fee/gas escape hatches, coarse token/concurrency limits, PoC/kill-switch/rotation toggles, capture on/off+dir). Run-time tuning (AIMD, breaker, per-weight scaling, input-token budget, rotation pre-PoC blocks) is admin-override-only; plumbing (tx poll cadence, fee denom, classify caps, drain timeout, runtime-build parallelism, capture thresholds) is config-constant-only. `ROUTE_PREFIX` derives from the protocol version in code; `CHAIN_ID` is auto-discovered from node_info. Full lists: plan Amendment F.

## 13. Filters (`filters/`)

One module owning the request/response boundary. Semantics are ported 1:1 from devshardctl (golden-verified, §15); structure is new:

- **Single registration table**: every top-level parameter declared once — name, stage(s), rule chain, model scope — in one file. Model scoping expressed **one** way (profile match, replacing today's three conventions: `ModelScopedParameterHandler`, `[]string+MatchesModel`, scalar `==`).
- **Model profiles**: per-model deltas (kimi: max_tokens floor 16, thinking_token_budget resolution, penalties forced 0, structured_outputs rejected, safety_identifier allowed, thinking→chat_template_kwargs mirror; minimax: strip enable_thinking/thinking, keep reasoning_split; qwen/default: no deltas) in one profile file per model.
- **Force/strip pairing**: forced request fields (`logprobs=true`, `top_logprobs=5`, `return_token_ids=true`) and the client-side response stripping of those same fields are declared together, so they cannot drift apart.
- **Pipeline order preserved**: nesting-depth pre-scan (≤32) → decode → extra_body unwrap → unknown-parameter whitelist reject → pre-validation rules → message normalizers (orphan-tool drop, empty-assistant drop, empty-content sentinel, legacy name strip, text-parts flatten) → message validation (role policies, tool_call_id matching) → output-token limits (default/cap, per-model overrides — unified into the table, ending today's three-place `max_tokens` handling) → post-limit rules → marshal.
- **Security bounds preserved**: body ≤10 MiB, schema walkers (depth/nodes/size/branch/enum/pattern caps, `$ref` ban, pattern compile check), chat_template_kwargs forbidden-key list, grammar nesting cap — the CVE mitigations keep their exact thresholds, consolidated into one shared `SchemaBounds` helper instead of three copies.
- **Response side**: SSE rewrite (strip internal fields, preserve `[DONE]` handling) and the non-streaming equivalent live here, next to the force table. Cacheability classification of upstream errors moves here too (it is response policy).
- Numeric coercion helpers unified on the shared `devshard/json.go` primitives (the local string-accepting fork in `paramvalidators` is retired; golden tests pin the resulting behavior).
- Rejected requests go to the capture sink with the same trigger conditions as today.

## 14. Perf, store, metrics, observability

**perf/**: per-host rings for receipt latency, first-token, CTTFL, total, responsiveness (window 256 samples / 60 min); pairwise relative-speed tracker bucketed by request shape; capability flags (context-limit, tools-unsupported); consumed by scheduler filters and engine policy; persisted to perf.db with prune. **store/**: gateway.db and perf.db, WAL, busy-timeout, single-writer discipline, retry wrapper; request accounting (request start / attempts / outcome / aliases) written from `RaceOutcome`; import/backfill for settled escrows kept for settlement continuity. **metrics/**: one registry, `promhttp` on `GET /metrics`, HTTP instrumentation middleware; every existing `devshard_*` family keeps its name and labels; new limiter v2 gauges (window size, breaker state) added alongside. **logging/**: request-id context logging as today.

**Shutdown** (ordered): stop accepting → drain in-flight races bounded by drain timeout → stop loops (rotation, phase observer, prune) → flush stores → close DBs. One context tree rooted in `main`.

## 15. Testing and verification (no live node available)

1. **Golden filter parity.** A corpus (old package's test cases + real request corpora from `~/develop/gonka-test/`) is run once through the **old** pipeline by a generator test added to devshardctl; committed goldens capture normalized output or rejection (code + message). New `filters/` must reproduce them byte-for-byte. Any intentional divergence = a reviewed golden change with justification.
2. **Race simulator for engine.** Scripted mock hosts (receipt delay, inter-chunk stall, empty stream, reasoning-burn, disconnect, garbage SSE); injectable `Clock` for virtual time; deterministic assertions on escalation timing, winner crowning, drain-after-detach, classifier verdicts. Old SSE fixtures move in as testdata; every named regression scenario (empty-stream, ghost, cleanup-barrier) becomes a simulator case.
3. **Component tests.** AIMD convergence and floors; breaker ladder and HalfOpen probing; snapshot merge of three sources and atomic swap under load; scheduler burn logic, Exclude handling, weight-based escrow pick; escrow intent-commit → crash → reconcile against a fake chain REST.
4. **In-process end-to-end.** Full gateway with stub chain REST + stub public API + httptest hosts: cold bootstrap, chat round-trip (streaming and non-streaming), cache hit, admin overrides with hot apply, rotation across a PoC boundary, kill-switch. Entire suite runs `-race`; new code must be race-clean.

Benchmarks: filter hot path (compare against existing bench), per-request allocation budget in the engine (target: no worse than devshardctl).

## 16. Migration / cutover

1. Land `cmd/gateway` alongside `devshardctl` (both build; no deploy change).
2. Deploy scripts switch binary + env names using the §12 mapping table; state starts fresh (new gateway.db bootstrap from `GATEWAY_DEVSHARDS_JSON`), perf/accounting history intentionally not migrated except escrow registry import via the existing admin import endpoint if needed.
3. Old `devshardctl` removed in a separate PR after the new gateway has been validated in the devshard environment.

## 17. Risks

- **Engine rewrite risk** (highest): mitigated by the simulator porting every named regression scenario and by fault-attribution parity checks; no live A/B possible until a node is available.
- **Filter drift**: mitigated mechanically by goldens.
- **Limiter behavior change is intentional** (softer quarantines): watch participant error rates after cutover; AIMD caps and breaker ladder are env-tunable without redeploy via admin overrides.
- **Ops churn from env renames**: one-time, covered by the mapping table; old names are not aliased (clean break, decision #1).
