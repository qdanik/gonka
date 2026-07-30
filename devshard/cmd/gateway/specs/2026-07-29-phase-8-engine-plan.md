# Phase 8 (engine) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `cmd/gateway/engine/` — the race engine that dispatches an assignment to a host, races escalated attempts, crowns exactly one winner, streams its bytes to the client, and emits exactly one `RaceOutcome` — replacing ~5400 tangled lines (`redundancy.go` 4185 + the race half of `proxy.go`).

**Architecture:** Three ownership domains that never share a field. An immutable `AttemptSpec` built once by the coordinator; an `attemptState` owned solely by that attempt's goroutine; and an `AttemptEvent` channel carrying `{Receipt|FirstToken|Content|Done}` to the coordinator. The coordinator is a single event loop with one re-armed timer. Every consumer-facing fact leaves through one `RaceOutcome`, built by a pure function.

**Tech Stack:** Go 1.24, module `devshard`. Consumes `cmd/gateway/{scheduler,perf,limits,metrics,chain,filters,config}` and `devshard/transport` (typed upstream errors). Backing research: `.superpowers/sdd/p8-engine-research.md` — implementers read the cited sections per task. The `RaceOutcome` contract also appears in `specs/2026-07-25-runtime-registry-and-outcome.md`.

## Decisions taken before planning (the eleven open questions)

**Reaching the host (Q1).** `engine` declares a consumer-side `dispatcher` interface covering the eight `user.Session` methods the race needs, satisfied by an adapter that `main` wires from the runtime registry. `scheduler.Assignment` does NOT grow a dispatch handle — that would leak the session across a boundary both packages were built to keep.

**Upstream status (Q2).** Recovered from `transport.UpstreamStatusError` via `errors.As`, self-contained, no admission-controller hook. **This is load-bearing:** `Overload` is the only verdict that contracts the AIMD window, so mapping every error to `TransportFault` yields a gateway whose backoff never engages while its breaker trips on ordinary overload. `engine → devshard/transport` is a new, sanctioned edge.

**Admission at dispatch (Q3).** The engine owns the mutating `Acquire`/`Release` pair; selection used only the non-mutating peek, so a host can pass selection and fail `Acquire`. A failed `Acquire` is **not** a host fault: the engine re-`Pick`s with that participant added to `Exclude`. Dispatching anyway would make the window advisory.

**Empty stream (Q4).** No fifth verdict. `limits` receives `ModelOutcome` (the window is untouched), and the engine keeps its own short-lived non-crowning set: such a host still receives attempts but cannot be crowned. This reproduces legacy shadow-quarantine semantics exactly where crowning already lives, without widening a committed package for a signal only the engine consumes.

**PoC facts (Q5, Q11).** The engine reads `chain.PhaseSnapshot` directly — design §10 already lists it as a `PhaseObserver` consumer for speculative-attempt policy. `phaseTransitionAborted` and the PoC clamp on speculative attempts both live here rather than being threaded through the assignment.

**Escalation inputs (Q6).** Rule 1 (unresponsive primary → immediate parallel) is **dropped**. Host ejection is now a scheduler filter and is recomputed on state change, so an unresponsive host is not picked as primary in the first place; keeping a perf-reading rule would make escalation timing depend on a second, slower-moving signal for a window that no longer meaningfully exists. Escalation is pure over elapsed time and attempt states.

**Winner hold (Q7).** **Dropped.** It existed to let a pairwise-preferred host catch up, and pairwise is gone; with no preference signal it is a 500 ms delay buying nothing, and it currently blocks the winner's own byte pump. Crowning is: first content chunk wins.

**Non-streaming reduced-max-tokens fallback (Q8).** **Dropped** — 60 lines that rewrite the client's body and mutate `min_tokens` to re-issue at half `max_tokens`. Hosts are now ejected on failure rate and the no-content timeout still applies. This is a deliberate behaviour-surface reduction; flag it in the phase hand-off so it can be reversed if operators depended on it.

**Config (Q9).** New `config.Engine` carries the five tunables (receipt timeout, first-token floor, inter-chunk stall, loser grace, max speculative attempts), admin-overridable. The four safety backstops (hard timeout, no-content timeout, max wait, long-response exemption) are package constants, like the drain timeout.

**Exemption ladder (Q10).** Captured into `RaceOutcome` at construction, making the outcome a genuinely pure value the ladder can be tested against. The ladder is written **once**, on the outcome.

**Two distinct PoC facts, not one.** An earlier draft of this plan said "pocGenerationActive" and conflated two separate legacy predicates: the *relaxed-PoC bypass* being active, which is what suppresses empty-stream accounting, and *PoC generation* being active, which is what a phase-transition abort compares against. They answer different questions and change at different times. Both are captured into the outcome; neither substitutes for the other.

## Global Constraints

- **Dependency boundary:** `engine → scheduler, perf, limits, metrics, chain, filters, config, devshard/transport, devshard/user (behind the dispatcher interface)`. NEVER `escrow` or `api`. Lifecycle signals (escrow-missing, balance-exhausted) are **reported in the outcome, never acted on**.
- **No field crosses a goroutine except through the event channel.** The legacy `inflight` struct is 57 fields shared across three goroutines with eight `sync.Once`, five atomics, one mutex, one channel — and several plain fields read cross-goroutine with none of them. That is the defect class this phase exists to remove; a mutex-guarded shared struct is not an acceptable substitute.
- **Exactly one outcome per race, exactly one winner.** Both must hold on every path including panic-free early return, client disconnect, and hard timeout.
- **A committed nonce is always settled.** Every attempt either completes or has its timeout posted; the legacy path that reads an attempt's fields while its goroutine still writes them is a **bug, not a contract** (research §8 item 3) — do not port it.
- No `panic`/`Must*`; no `os.Getenv`; no mutable package-level state (the legacy engine has nine timing globals plus a process-global classify map — none survive). Determinism: injected clock, no wall-clock in logic.
- **Comments — HARD RULE:** default ZERO; only where a name and signature cannot carry a constraint. NEVER restate a name; NEVER reference tasks, phases, plan docs, audits or old files.
- **Tests:** table-driven, deterministic, `-race`, goleak-clean. Every task ships mutation evidence: confirm the mutant COMPILES, confirm the intended test fails, restore byte-identical. A mutant that fails to build proves nothing.

---

## Task 1: `outcome.go` — the single recording point

**Written before any racing code.** `RaceOutcome`, `AttemptOutcome`, `Terminal`, `Lifecycle`, and the three-vocabulary translation: `Terminal → limits.Verdict`, `Terminal → perf.Sample`, `Terminal → metric labels`. The verdict mapping table in research §6.3 is the spec — reproduce it exactly, including that HTTP 400/500 are telemetry-only and move nothing, that non-inference-path failures are ignored, and that a stalled winner after content is a `TransportFault` gated on failure rate.

The exemption ladder (research §8 item 10) is written **once** here as an ordered predicate list. Legacy had six sample-recording entry points applying different subsets of the exemptions; that divergence is unauditable and is the thing being fixed.

`RaceOutcome` carries the captured facts the ladder needs (`nonceFinished`, `pocGenerationActive`) so it stays a pure value. Include `completedAt` — legacy's `TotalTime` is the sole source of two Prometheus families and has no equivalent in `perf.Sample` today.

**Tests:** the full verdict table as a table test, one row per upstream condition, asserting verdict AND whether the window moves; the exemption ladder with each exemption in isolation and in combination; `Terminal → perf.Sample` field-by-field.

## Task 2: `classify.go` — SSE verdicts, pure

`contentSource`, `errorPayload`, `usageCompletionTokens`, the retriable-capability carve-out, `isEmptyStream`, `isModelBurnEmpty` (research §4). Pure over `[]byte`. The legacy `sse_fixtures_test.go` corpus moves here as testdata — it is a real oracle and must not be regenerated.

**Critical distinction to preserve:** an SSE error event increments the chunk count but is **not** content and must never crown. This is what makes an empty-stream host lose to a slower honest one.

**Tests:** every fixture in the ported corpus; chunk-boundary splitting (the legacy `AllChunkSizes`/`RandomChunking` cases); an error event that must not read as content; burn-empty distinguished from plain empty by `usage.completion_tokens > 0`.

## Task 3: `escalation.go` — policy, pure

`EscalationPolicy` over `(clock, attempts, params)` — no perf reads (Q6). The up-front `Decide` plus the per-attempt ladder (research §3.2), with the timing constants from `config.Engine` and the four backstops as constants. Escalation must be **re-validated when its timer fires**, not only when armed: skipping the re-check fires a needless secondary on every healthy request (research §0.7).

**Tests:** each ladder rule in isolation; the re-validation (a timer that fires after the condition cleared must not escalate); the PoC clamp on speculative attempts; boundary values on every threshold.

## Task 4: `attempt.go` — one attempt, one goroutine

The immutable `AttemptSpec`, the goroutine-local `attemptState`, the `AttemptEvent` union, dispatch through the `dispatcher` interface, terminal classification, and the deferred classify-buffer release. `Acquire`/`Release` bracket the real dispatch here; a failed `Acquire` returns the re-pick signal from Q3.

**Tests:** every event emitted in order for a healthy attempt; each terminal classification; a failed `Acquire` produces a re-pick, not a fault; `Release` happens on every exit path including panic-free early return.

## Task 5: `race.go` — the coordinator

The event loop: start attempts, fan in events, crown on first content, early-return once the winner is streaming, spawn background finalize, emit exactly one outcome. One `time.Timer` re-armed to `nextDeadline(attempts, params, winner) (time.Time, trigger)` — a **pure function** (research §8 item 2), because the escalation-vs-stall-vs-hard-timeout precedence is currently **undefined**, not merely implicit. An earlier draft of this plan said "implicit in select-arm ordering", which misstates Go: when several cases of a `select` are ready, the spec chooses one by uniform pseudo-random selection, so arm order carries no precedence at all. The legacy `select` (`redundancy.go:2417`) has four arms — done, escalation, reduced-fallback and non-stream-timeout — that can be ready simultaneously, so which one wins is genuinely unspecified rather than ordered. State it that way: undefined, not "observed to misbehave".

**Tests:** exactly one winner under concurrent first-content; exactly one outcome on every exit path; `nextDeadline` precedence as a table; a losing attempt's bytes never reach the client; termination with all attempts pending.

## Task 6: `stream.go` + `carry.go` — winner bytes and bounded reassembly

The winner writer with the three-way winner/suppressed/buffer branch and ordered prefix flush; the bounded carry buffer with per-attempt/participant/global accounting and rollback on global trip. **`pendingBuf` gets a cap** (research §8 item 6): today it is unbounded while the classify buffer beside it is capped three ways.

**Tests:** prefix flushed in order on crowning; a capped attempt still wins but starts the client stream at the current chunk; global trip rolls back; loser bytes discarded.

## Task 7: `drain.go` — two contexts

The design's model, replacing hand-rolled flags: a client context and a race context (`context.WithoutCancel` + `DrainTimeout` armed on client-done). A client disconnect must not kill the race — receipts, accounting and nonce bookkeeping have to complete (research §5.2). The legacy `streamCtx.Done()` arm is currently unreachable because both callers pass `context.Background()`; its **logic** is ported even though the branch never fires today, because the two-context model makes it live.

**Tests:** client disconnect mid-stream leaves the race running to completion; the drain timeout bounds it; the outcome is still emitted exactly once; goleak-clean after disconnect.

## Task 8: `settle.go` + `capability.go` + `errors.go`

One `HandleTimeout` skip ladder (research §8 item 4 — legacy has two copies that already disagree; **pick one deliberately and say which**). Capability-error parsing feeding `perf.RecordContextLimit`/`RecordToolUnsupported` and the `ContextHint` growth that feeds the next `Pick`. The error types and the error→HTTP-status mapping api needs.

**Tests:** the skip ladder per condition; capability parse for each known upstream phrasing; context-hint growth across a re-pick.

## Task 9: `engine.go` + the simulator harness

`Engine`, `Deps`, `Run(ctx, profile, body, clientW) (RaceOutcome, error)`, and every consumer-side interface (`picker`, `dispatcher`, `hostPerf`, `hostLimiter`, `raceMetrics`). The only file `api` imports.

**`simulator_test.go`** is the deliverable that makes the rest trustworthy: a scripted mock-host harness with receipt delay, inter-chunk stall, empty stream, reasoning-burn, disconnect, garbage SSE, and an injectable clock. Every named legacy regression listed in research §9 is ported as a case.

## Cross-task notes

- Dead code to delete rather than port (~180 lines, research §8 item 12): `detachClient`/`isClientDetached` and its three guard sites, `finishStalledWinnerAfterClientTimeout`, `waitForClientTimedOutAttempts`, `recordStalledWinnerFailureOnce`, `writeStreamReset`, `werrOrNil`. **Exception:** port the `streamCtx.Done()` logic (Task 7).
- Four config knobs are dead or log-only and are dropped, not ported: `InterChunkStallTimeout`, `PerInputTokenFirstTokenLag`, `MinSamplesForDecision`, and the `NonStreamResponseFloor`/`PerInputTokenResponseLag` pair.
- Carry-forwards owed here, all verified: completion→chunk SSE synthesis (never ported); `IsCacheableUpstreamError` is missing the write-time SSE-embedded scan, the read-time re-scan-and-evict, and the 2xx branch; the first-call-wins status guard (the correct pattern already exists in-tree at `response_cache.go:208-213`).
- Metrics: the race families are the largest blocked group in the migration plan. Emit them from the outcome's single recording point, not from call sites.
- **`Session.HandleTimeout` returns a non-nil error on its SUCCESS path** (`session.go:1805`, immediately after the `timeout_completed` log) — the error carries "the inference timed out" upward to fail the request, it does not mean the vote failed. Legacy's `completed` metric branch is therefore unreachable and every posted vote is currently labelled `failed`. The engine's contract is the sane one (non-nil error = the vote failed), so the **adapter must normalize**, and it cannot do so by `Reason != ""` alone: the send-diff failure at `:1802` also returns a populated `result`. **Correction to an earlier draft of this note:** it claimed there are two genuine failure returns and that all of them wrap with `%w`. There are **three**, and the third does not wrap. `insufficient timeout votes` (`user/session.go:1809`) returns a populated `TimeoutResult` with an unwrapped error — structurally identical to the success return at `:1805`, differing only in message text and log stage, and message text is not a legal discriminator. So **no rule over `(result, err)` can separate success from insufficient-votes.** The adapter's rule is `errors.Unwrap(err) == nil && result.Reason != ""` → posted, which fixes the success case and two of the three failures (legacy mislabelled all four as `failed`), and the remaining collision is pinned by a test rather than hidden. The real fix is one line at the source: wrap `:1809` with a new `ErrInsufficientTimeoutVotes` sentinel, after which the adapter classifies correctly with no change. That line is out of this branch's scope. `user/session.go` is merged, shared with devshardctl, and out of this branch's scope, so the wart is absorbed at the boundary rather than fixed at the source. Owner: Task 9's adapter, with a test pinning both returns.
- Phase-end: KISS audit, comment sweep, comprehensive review, hand off for the user's commit.
