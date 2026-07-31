# Phase 9 — `api` + the composition root

> **For agentic workers:** use `superpowers:subagent-driven-development`. Steps are checkboxes.

**Goal:** turn eleven tested packages into a running process that serves the legacy client contract.

**Architecture:** a new `api` package owning the HTTP boundary, a `registry` owning live escrow state and satisfying every consumer interface the earlier phases declared, thin adapters over `devshard/user`, and a `main` that wires them and owns shutdown order. Research: `.superpowers/sdd/p9-api-research.md` (961 lines).

**This phase is where every contract meets its first real consumer.** Nothing outside the packages imports the engine today, so several hazards recorded in earlier phases fire here for the first time.

---

## Global Constraints

- **Dependency direction:** `main → api → {engine, scheduler, escrow, limits, perf, chain, filters, store, metrics, config}`. `api` may import `devshard/user` and `devshard/transport`. No package below `api` gains a new import.
- **The registry is the only place live escrow state lives.** It satisfies consumer interfaces; it does not reach back into them except through explicitly declared outbound interfaces.
- **No `panic`/`Must*` on a request path, no `os.Getenv` outside `env`, no mutable package-level state, no `time.Now()` in logic — injected clock.**
- **`WriteTimeout` must never be set on the HTTP server** — it kills SSE. `ReadHeaderTimeout` must be set.
- **`/metrics` must not be instrumented** or the scrape self-counts and the duration histogram measures its own exposition.
- **Shutdown order is contract, established in Phase 1:** stop accepting HTTP first, close the store last. Every component added here takes its place in that order explicitly.
- **Comments — HARD RULE:** default ZERO; only where a name and signature cannot carry a constraint. Never reference tasks, phases, plan docs, audits, or old files.
- **Tests:** table-driven, deterministic, `-race`, goleak-clean. Every task ships mutation evidence — confirm the mutant COMPILES **and reproduces the real defect**, then restore byte-identical. A mutant that builds but does not reproduce the bug proves nothing.
- **The git index must match the worktree** at the end of every task. The user commits from the index.

## Decisions taken before planning

1. **Request accounting is in scope**, owned by `store` — the per-request cost ledger plus a lookup endpoint. It was the largest genuine parity gap.
2. **Perf persistence is out of scope** — cold start accepted as a deliberate divergence. Peak-EWMA converges in tens of requests, and a persisted score after a long idle period asserts something about hosts that may have changed.
3. **OpenAPI/Swagger is dropped** — one commit ever, zero tests, CDN-dependent, drifted, and unreachable.
4. **The ten top-level `/v1/{finalize,state,debug/*}` routes are dropped**; their per-escrow equivalents remain.
5. **The suspicious-hosts list is in scope** — without it `EscalationPolicy`'s `primarySuspicious` branch is unreachable, and shipping an unreachable branch violates the project's own no-dead-code rule.
6. **`Capacity.Snapshot` is required scope, not optional.** Legacy uses observed-key counters to disable the per-weight cap when chain data is missing. Without it a missed poll fails **closed**.

## Corrections to earlier phases' assumptions — verified, do not re-derive

- **`chain.PhaseSnapshot.Admission()` does not exist.** The Phase 3 plan promised it; `PhaseSnapshot` has no methods at all. Phase 9 writes the predicate, and it must fold in relaxed mode itself (`RequestsBlocked && PoCMode != relaxed`), because the observer deliberately mirrors the raw chain value while legacy folded relaxed mode in at `phase_gate.go:928`.
- **`limits.Capacity.SetEscrowMembership` has zero non-test callers.** Until this phase pushes membership, `EscrowWeight` returns 0, `loadScore` sees a non-positive weight and returns `+Inf`, every candidate is skipped, and `pickEscrow` returns `ErrNoEscrowCapacity`. **The gateway would boot green and serve nothing.** This is the single most important wiring step in the phase.
- **Legacy already solved the boot RPC herd, in two coupled parts.** `buildRuntimes` (`gateway.go:4210`) is dead code with zero callers. Production boot is `main.go:418-430`: a semaphore bounds concurrent builders **and** `buildRuntimeBridgeClient` sizes `MaxIdleConnsPerHost` to the same limit, because the default is 2. **Reproduce the pairing, not just the semaphore** — bounding concurrency while leaving the default idle pool trades a request flood for connection churn.
- **The chat body is already bounded** at 10 MiB (`request_filters.go:158`). The unbounded-ingest hole is **admin**: bare `io.ReadAll(r.Body)` at `gateway.go:3332` plus six unbounded decoders.
- **`normalizeMetricsPath`'s label domain is not closed.** Three of eight branches emit unbounded strings, including `default: return path` on unauthenticated traffic — a cardinality bomb, with zero test coverage anywhere. Reproduce the *matched* labels verbatim; fold everything unmatched into one `other` constant rather than copying the bomb.

---

## Task 1: `registry` — live escrow state

**Files:** create `cmd/gateway/registry/{registry,escrow,session,membership}.go` + tests.

The keystone. It owns escrow records and their `user.Session` handles, and satisfies `scheduler.escrowSource` and `scheduler.session`, `engine.targets` and `engine.dispatchTarget`, and `escrow.SettlementSource`. It pushes outward through **declared outbound interfaces** (not direct imports of `limits`): membership, escrow removal, nonce exhaustion, and the in-flight counter.

The stated dependency rule breaks in both directions here — `Candidates` returns `[]scheduler.Escrow` while membership calls into `limits`. Resolve with a small adapter in `main` plus a registry-side outbound interface; do not let `registry` import `limits`.

Rehydration for settlement uses `user.NewLocalSession`, which is chain-less and client-less and **is not interchangeable with `NewHTTPSession`** — keep the two construction paths distinct and named.

**Tests:** membership pushed on add and removed on retire; `Candidates` filtered to active-and-accepting; a rotated-out escrow disappears between `Candidates` and `Target` (the exact race that stranded a nonce in Phase 8); nonce exhaustion reaches the outbound interface.

## Task 2: `user.Session` adapters

**Files:** create `cmd/gateway/api/dispatch.go`, `cmd/gateway/api/timeouts.go` + tests.

**The `Send` sequence is verified and load-bearing.** `finished` is written in exactly one place — `processResponse` (`user/session.go:534-536`) — the engine reads it on `AttemptDone`, and the engine never calls `ProcessResponse` itself. Order: type-assert `*user.PreparedInference` → `SendOnly(ctx, prepared, stream, onReceipt)` → wrap a state-root mismatch in `engine.ErrStateRootDivergence` → **`ProcessResponse` if `resp != nil`, even on error** → return. `Confirmed()` is `ConfirmedAt > 0`; `StreamBytes()` is `StreamBytesRead`.

Skipping `ProcessResponse` breaks three things, not one: crowning (a 502 on a paid success), timeout votes (posted for every nonce, early, against the wrong deadline), and escalation (never stops). It also drops state-root verification, signature persistence and the `MsgConfirmStart`.

**The timeouts adapter normalizes a legacy wart.** `Session.HandleTimeout` returns a non-nil error on its success path (`:1805`). Rule: `errors.Unwrap(err) == nil && result.Reason != ""` → posted. **A known collision is unresolvable at this layer:** `insufficient timeout votes` (`:1809`) returns a populated result with an unwrapped error, structurally identical to success. Pin all five returns with tests and leave the collision named, not hidden. The one-line source fix (a sentinel at `:1809`) is out of this branch's scope.

**Tests:** the full `Send` order asserted by a recording session, including `ProcessResponse` on the error path; `Flush` reaches the client (Phase 8's blocker was exactly this assertion missing); all five `HandleTimeout` returns; `NewSessionTimeouts` covered — it is the only way `api` builds the adapter and had no test.

## Task 3: `api` — routes, middleware, admission, and the three P0 items

**Files:** create `cmd/gateway/api/{server,routes,admission,middleware,errors}.go` + tests.

21 routes survive (research §1.3 has the table with method, auth, body type and every status code). Client-facing: `/v1/models`, `/v1/chat/completions`, `/v1/status`, `/metrics`, six per-escrow. The rest are operator-facing.

**Write the admission predicate here**, folding in relaxed mode as noted above. It is a pre-queue check on the snapshot — cheap reject before queueing, not a per-escrow gate call.

**The three P0 items, each with its verified insertion point:**
- **Ingest bound.** `http.MaxBytesReader` on admin bodies (the typed error and connection close are the point of using it over `LimitReader`), and reproduce the chat 10 MiB bound. Set `ReadHeaderTimeout`; **never** `WriteTimeout`.
- **Admin key.** Hash both sides, then `subtle.ConstantTimeCompare`, and **evaluate it only on admin paths** — legacy compares on every request before checking whether the path is an admin one, making the timing probeable from a cheap unauthenticated route while gating tx-signing and `os.RemoveAll`. Preserve the admin-flag propagation into `filters`, and resolve the `TrimSpace` disagreement with `gateway.go:1014` deliberately.
- **Unroutable model.** Reject after `filters` and before the cache, the limiter slot and the token budget — legacy's `validatePooledRequestedModel` returns nil when the registry is empty (`gateway.go:1496`), so a cold gateway lets any model string consume all three before failing 502. Fail **closed** (503) on an empty registry.

**Also port verbatim:** `removeDevshardStorage`'s path-traversal guard. It guards `os.RemoveAll` on a client-supplied id.

**Tests:** every route's status codes; admission rejects during PoC and admits under relaxed mode; each P0 item with a test that fails without it; the traversal guard against `../` and absolute-path ids.

## Task 4: request accounting in `store`

**Files:** modify `cmd/gateway/store/`, add `accounting.go` + tests; add the lookup route to `api`.

Per-request ledger: request id, model, escrow, host, nonce, tokens, cost, outcome, timings. Written from the engine's single recording point, not from call sites. Add the lookup endpoint keyed on `X-Request-Id` — legacy hands clients that header with no endpoint to resolve it, so this closes a gap legacy also had.

**Tests:** a completed race writes exactly one row with the outcome's own numbers; a failed race writes a row too; lookup returns it; the write does not block the client's response.

## Task 5: response cache and request capture

**Files:** create `cmd/gateway/api/{cache,capture}.go` + tests.

Port the cache **with its three legacy bugs fixed**: key it on the caller, key it on the escrow (legacy does not, so per-escrow pinning is silently violated and the `X-Devshard-ID` header lies), and store a streaming reply as replayable chunks rather than one blob. Carry forward the first-call-wins status guard — the correct pattern already exists in-tree at `response_cache.go:208-213`.

Request capture: add the `filter_rejected` bucket legacy lacks, plus **sampling and a size cap** — an uncapped capture path is a disk-fill vector.

**Tests:** a second identical request from a different caller is a miss; the same caller on a different escrow is a miss; a streamed reply replays chunk-for-chunk; the capture cap trips and sampling honours its rate.

## Task 6: metrics — collectors, labels, and the race families

**Files:** modify `cmd/gateway/metrics/`, add per-owner collectors; wire `engine.raceMetrics`.

Register one `prometheus.Collector` per package that owns gauge state — `limits`, `perf`, `registry`, `chain` — mirroring legacy's shape but split by owner rather than reaching into a god-struct. Implement `raceMetrics` (the engine runs with `Metrics: nil` today) and emit the race families from the outcome's single recording point.

**Reproduce the matched route labels verbatim** (research §2.3 lists all 31). Fold unmatched paths into one `other` constant rather than copying legacy's unbounded `default: return path`. Do not reproduce raw `request_id` or raw pprof paths. **Exclude `/metrics` from instrumentation.**

**Tests:** every label value asserted against the research table; an unmatched path yields `other`; `/metrics` is not counted; each collector's numbers match its package's readers.

## Task 7: `main` — the composition root

**Files:** modify `cmd/gateway/main.go`.

Wire everything, in the order that makes the dependency graph acyclic, with the `limits`↔`registry` adapter from Task 1.

**Bounded boot, reproducing legacy's pairing:** a semaphore bounding concurrent escrow builds **and** an HTTP client whose `MaxIdleConnsPerHost` matches that bound. Port legacy's three-arm error ladder: inactive → skip, escrow-missing or no-key → mark inactive in the store, anything else → fatal. Lazy construction cannot reproduce that ladder, which is why boot stays eager.

**Shutdown order:** HTTP server first (stop accepting), then the engine's `Stop` barrier, then the escrow manager, then the observer, then the store. State it once, in code, where a reader will hit it.

Decide explicitly whether to keep panic recovery on the request path, and say why in the report.

**Tests:** `run(ctx)` boots and shuts down cleanly with a fake chain; the build ladder's three arms; shutdown order asserted by a recording sequence; the idle pool is sized to the semaphore.

## Task 8: `Capacity.Snapshot` and the suspicious-hosts list

**Files:** modify `cmd/gateway/limits/capacity.go`, `cmd/gateway/perf/`; wire in `main`.

`Capacity.Snapshot` exposes the observed-key counters so a missing-chain-data condition disables the per-weight cap instead of failing closed. The suspicious-hosts list feeds `EscalationPolicy.primarySuspicious`, which is currently unreachable.

**Tests:** a missed poll does not zero capacity; a suspicious host makes `Decide` start two attempts immediately, and the branch is reached from a real wiring path rather than a hand-built policy.

## Task 9: in-process end-to-end

**Files:** create `cmd/gateway/e2e_test.go` (or an `e2e` package) + fixtures.

`user.InProcessClient` is the find: a test can drive a real `user.Session` against real in-process `host.Host` instances — real diffs, state roots, signatures, `MsgFinishInference` — with no network and no chain. Research §8.1 names 13 behaviours this makes genuinely verifiable, including the `Send`/`ProcessResponse` ordering, state-root divergence → permanent host block, flush-before-return, every status code, and metric label parity.

**Write the honest boundary into the test file's own documentation**, so nobody later mistakes the suite for full verification. Not verifiable without a node: that the real chain accepts our transactions (**the largest irreducible risk — one wrong proto field number is a green suite and a rejected tx**), that real hosts behave like scripted ones, any tuning threshold, real-scale concurrency (two named hazards: `ProcessResponse` serialising a wide race on one `Session.mu`, and the registry read lock), settlement money end-to-end, and real-client byte compatibility.

---

## Cross-task notes

- **Ten items are flagged unverified in research §9.** Two must be checked before code depends on them: whether `SetEscrowMembership` wants counts or ratios, and whether `engine.Stop()` covers loser cleanup.
- **Homeless legacy behaviour assigned here:** lazy read-only rehydration with its admin-only DoS gate and metadata-only fallback; the drain barrier's background term; version → host-route-prefix derivation; `attachRuntimeSharedState` as single-constructor discipline. Four dead legacy symbols are dropped, not ported.
- **Two routes legacy should serve and does not:** `GET /v1/requests/{id}` (Task 4 adds it) and a root `/openapi.json` (dropped by decision 3).
- **Parity gaps stay a living document.** Nine divergences are accepted by design; eleven were genuine losses, of which this phase closes request accounting, the suspicious list and `Capacity.Snapshot`. Update `.superpowers/sdd/p9-api-research.md`'s gap table as each closes — it is an input to the final comparison artifact, and a gap silently closed is as misleading as one silently left open.
- **Phase-end:** KISS audit, comment sweep, correctness review, then hand off for the user's commit.
