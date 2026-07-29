# Phase 7 (scheduler) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `cmd/gateway/scheduler/` — the routing core `Pick(ctx, profile) → Assignment{escrow, host, nonce}` — replacing the old target-selection logic (`reserveRuntimeForModel` + `sessionPicker.run`, ~1400 tangled lines) with a pure decision function + a minimal per-escrow actor, plus the small `limits/` and `chain/` extensions it needs.

**Architecture:** The hard, correctness-critical part (which host serves which request, and the nonce-liveness invariant) is a **pure total function** `match`. The concurrency (serialize one escrow's sequential nonce stream, match concurrent requests to nonces, conserve nonces via cross-request matching) is a **lock-free per-escrow actor** that owns the escrow's `user.Session` and a waiting queue. `Pick` picks an escrow by spare weight W(e), routes the request to that escrow's actor, and awaits its assignment. The engine (later phase) calls `Pick` again per escalation attempt with a grown `Exclude`.

**Tech Stack:** Go 1.24, `devshard` module. Consumes `cmd/gateway/{chain,perf,limits,config}` + `devshard/user` (Session, behind a narrow interface) + `devshard/types` (MaxActiveNonce). Backing research (old-code cites, the state machine, the invariants): `.superpowers/sdd/p7-scheduler-research.md` — implementers read the referenced sections per task.

## Global Constraints

Every task's requirements implicitly include this section.

- **Dependency boundary:** `scheduler → chain, perf, limits, config, devshard/user (behind a narrow interface), devshard/types`. NEVER import `engine`, `api`, `escrow`, `store`, or `devshard/bridge`. Runtime-owned facts (the candidate-escrow registry, each escrow's Session, active/phase status, the state-divergence block signal) enter through consumer-side interfaces defined here and wired by api/engine later.
- **Parity FLOOR — the nonce liveness invariant (research §1.1, §4):** every nonce the session *commits* is dispatched to exactly one party — a real request (`Serve`) or a ghost probe (`Burn`). `Hold`/`Empty` decline BEFORE commit (the nonce is untouched, recomputed identically next time). There is no fourth outcome; that exhaustiveness IS the invariant. `match` returning a `Decision` sum type makes it compiler-checked.
- **Parity FLOOR — multi-slot participant keying (research §4):** a validator can hold multiple slots (`len(group) > distinct participants`). Exclude sets, availability, and `ErrAllHostsExcluded`'s bound are keyed by **participant**, never by slot — a host excluded once must never be re-servable through a sibling slot. Preserve the regression intent of `TestPicker_MultiSlotParticipantTreatedAsOne` / `TestPicker_AllSlotsOneParticipantExhaustsImmediately`.
- **Parity FLOOR — nonce conservation (research §1.2):** a nonce bound to host X is `Burn`ed only when NO waiting request can use X. When request A excludes X but queued request B can use it, the nonce serves B (cross-request match), not a ghost. The 200ms `Hold` grace catches co-arriving traffic before burning. Ghost commit is **local/in-process, no network I/O** (research §1.3) — never an eager chain/HTTP sync.
- **Ghost commit is free + local:** composing the diff + advancing the nonce happens inside `session.Advance` under the Session lock, for real and ghost alike; a ghost additionally records a metric + log, nothing else. The diff reaches the chain only later (catch-up replay on the host's next real dispatch, or at Finalize — both out of scope here).
- **Escrow pinned per request (research §3, Q1):** escrow pick happens once (attempt 1); escalation reuses the same escrow via `profile.Escrow`. Never re-pick the escrow mid-race.
- **No `panic`/`Must*`; no `os.Getenv`; no mutable package-level state (sentinel `error`/`var` sum-type singletons excepted).** Errors wrapped `%w`; sentinel errors for `ErrNoAvailableHost`/`ErrAllHostsExcluded`/the all-zero escrow rate-limit.
- **Determinism:** iterate the waiting queue and group in order; the tie-break is an explicit shared-counter pseudo-round-robin (document it as such, research §6-7 — do NOT "fix" it into a fairer rotation). All time via an injected `now func() time.Time`; the hold uses an injected timer/clock, never `time.Now()`/`time.After` directly in logic.
- **Comments — HARD RULE (calibrate against `cmd/gateway/limits/weights.go` and the Phase 6 escrow package):** default ZERO. Only load-bearing constraints (the liveness invariant, multi-slot keying, ghost-is-local, why `Hold` doesn't commit). NEVER restate a name or reference tasks/phases/plan-docs/old files.
- **Tests:** table-driven, `t.Run` subtests, deterministic (injected clock, fake session/escrowSource/limits/perf — no real network/goroutine-timing races), pass under `-race`, goleak-clean on the actor. `match` is exhaustively unit-tested (every filter kind, multi-slot, conservation, hold, liveness). Test fixtures use abstract names (`host-a`, `model-a`), never invented real-looking model IDs.

## Shared interfaces (defined in `scheduler/`, wired by api/engine later, faked in tests)

```go
// escrowSource is the candidate-escrow registry; api wires it over the live runtime map.
type escrowSource interface {
	// Candidates returns the escrows that could serve model, each with its Session handle,
	// in a stable order, with active/phase already filtered to accepts-new-inferences==true.
	Candidates(model string) []Escrow
}
type Escrow struct {
	ID          string
	Model       string
	Session     session           // the per-escrow nonce stream (below)
	ActiveUsers int               // in-flight user requests, for the W(e) load score
}

// session is the narrow view of devshard/user.Session the scheduler needs; api wires an adapter.
// The adapter lives OUTSIDE this package, so everything it must branch on is exported: it sees
// NonceIntent, never the internal Decision sum type.
type session interface {
	// Advance computes the next candidate binding, calls decide(binding), and commits the nonce
	// (composing the real-or-ghost diff) IFF the returned intent commits. A declined nonce is left
	// untouched and yields a nil Prepared. This is the one atomic peek→decide→commit unit.
	Advance(decide func(HostBinding) NonceIntent) (Prepared, error)
	ParticipantKeys() []string   // distinct participants (slots deduped) — the exclusion universe
	GroupSize() int              // len(group); nonce % GroupSize == hostIdx
	LatestNonce() uint64         // for the nonce-cap gate
}
type HostBinding struct {
	Nonce       uint64
	HostIdx     int
	Participant string // participant key for HostIdx (slot-deduped)
}

// NonceIntent is what the scheduler tells the session to do with the nonce it is offering.
// Params carries the real-dispatch payload and is set only when committing a non-ghost nonce.
type NonceIntent struct {
	Commit bool
	Ghost  bool
	Params any
}

// Prepared is a committed nonce ready for dispatch. *user.PreparedInference already satisfies this
// verbatim, so the api adapter needs no conversion code.
type Prepared interface {
	Nonce() uint64
	HostIdx() int
}
```

The dispatcher keeps the internal `Decision` by capturing it in the closure it passes to `Advance`, and returns only a `NonceIntent` from that closure — so the sum type stays unexported and the adapter stays trivial.

`limits` extension (Task 1): `func (l *ParticipantLimiter) Available(participant, model string) bool` — non-mutating peek (breaker-closed AND window not full), for `match`'s window/breaker filter. `chain` extension (Task 2): `PhaseSnapshot.MaxNonce uint64` (governance `devshard_escrow_params.max_nonce`). **Type seam:** `types.MaxActiveNonce(maxNonce uint32, groupSize int) uint64` takes a `uint32`, so Task 5 must range-check before converting — a `MaxNonce` above `math.MaxUint32` must clamp to `math.MaxUint32`, never wrap, because a wrapped value that lands on 0 silently disables the cap (`MaxActiveNonce` returns `^uint64(0)` for 0). Treat "cap unknown" as a conservative fixed ceiling rather than "unlimited": the legacy gateway ran on a hardcoded 19 800, so falling back to that constant is strictly better than both fail-open and refusing to serve.

## Mandatory hazard controls (found by review of this design before implementation — these are requirements, not suggestions)

**H1 — the drain-termination proof requires frozen predicates.** "Burns are bounded by GroupSize per waiter" only holds if availability cannot change *during* a drain. It can: `throttled` reads `limits.Available`, which every concurrent `Acquire`/`Release` moves. Interleaving: the sweep keeps a waiter because host B looks usable, B's window fills before `Advance` binds to it, the nonce burns, B frees, repeat — an unbounded spin in which **every iteration commits a nonce**. Nonces are a capped, money-backed resource (a fresh escrow costs `model.Amount` on chain), so this drains an escrow's budget in seconds. Requirement: evaluate every availability predicate **once per `drain()`** into an immutable snapshot that `match` reads, and additionally enforce a hard burn budget of `GroupSize * (len(waiting) + 1)` per drain, with a metric when it trips. Note the bound is in **slots**, not participants: a validator holding 6 of 8 slots multiplies nonce spend.

**H2 — the liveness invariant has a real hole, and it is not in `match`.** The sum type makes the *decision* total; it does not make the *dispatch* total. Between the nonce committing inside `Advance` and the assignment reaching `replyCh`, the waiter's context can cancel. Two failure modes: an unbuffered `replyCh` blocks the escrow's actor goroutine **forever** (one cancelled client kills every request on that escrow), and a fire-and-forget send loses a committed nonce that is neither served nor burned — the fourth outcome the invariant forbids. Requirement: `replyCh` is buffered with capacity 1, the send is non-blocking, and a send that finds no receiver is **reclassified as a ghost burn** (`ghostAbandoned`) so the nonce is still accounted to exactly one party.

**H3 — cancellation must not message the actor.** `waiting` is owned by the actor goroutine, so a "deregister me" message can itself block. Requirement: the waiter carries an `abandoned atomic.Bool` that `Pick` sets on ctx-cancel; `match` skips abandoned waiters and `sweepExhausted` reaps them lazily. `Pick` never blocks on the actor to leave.

**H3a — adding `abandoned` changes `match` in TWO places, not one.** The obvious change is skipping abandoned waiters during the scan. The one that is easy to miss: the `hold` deadline is derived from the *oldest* waiter, and it must become the oldest **non-abandoned** waiter. Otherwise a queue whose head has been abandoned holds the nonce open on behalf of a caller that will never be served — a nonce parked for a ghost, which is the liveness failure in a subtler costume. Whoever implements the dispatcher must revisit `match` for both, and pin the second with a test whose queue is entirely abandoned.

**H4 — draining the submit channel on stop.** With a buffered `submit`, `select` picks randomly when both `submit` and `stop` are ready, so waiters can be left in the buffer with no reader while `submitWaiter` reported success. Requirement: after `loop()` exits, drain `submit` and fail every waiter found there, not just those already in `waiting`.

**H5 — the idle-reap protocol.** "Re-check emptiness under the map mutex" is itself a race (`waiting` belongs to the goroutine) and does not order what matters: `Pick` can take a dispatcher, the reaper can stop it, and `Pick` then submits to a dead actor. Requirement: the actor decides its own reap; `Pick` increments a `pendingSubmits atomic.Int64` **before** releasing the registry mutex; the reaper refuses to stop while that is non-zero; `Pick` retries get-or-create if it loses.

**H5a — `submitWaiter` returning false does not mean "stopping".** It also reports a full submit buffer, because blocking there would either hang a submit that arrives before `start` or hold the lifecycle lock indefinitely. `Pick` must therefore distinguish the two: a stopped dispatcher warrants a get-or-create retry, while a full buffer is genuine back-pressure and should surface as a rate-limit-class error rather than a retry loop. Treating every `false` as "stopping" turns a saturated escrow into a spin.

**H5b — the availability predicates are memoised per drain on `{participant, model, requiresTools, contextHint}`,** because `RequestProfile` is not comparable and cannot key a map. Any predicate that reads a profile field outside that tuple will see a stale answer for the rest of the drain. If a future filter needs another field, it must join the memo key — silently reading it is a correctness bug that only appears under load, when drains are long.

**H6 — never close over the phase snapshot.** `chain.PhaseSnapshot` is a value; a dispatcher that captures one at construction serves a hot escrow with an hours-stale view, including straight through a PoC transition. Requirement: `drain()` calls `snapshots.Snapshot()` at its top (which is also where H1's predicate freeze happens).

**H7 — nonce exhaustion has no owner.** Task 5 drops nonce-capped escrows from the candidate set and explicitly must not trigger replacement there; the Phase 6 depletion loop only ever reacted to balance. Nothing schedules a replacement for an escrow that burns through its nonce budget — at 10 rps and ~30% ghost burn a 19 800 budget is gone in roughly 25 minutes, and escrows silently drain into `ErrNoEscrowCapacity`. Requirement: expose `OnNonceExhausted(escrowID)` feeding the same escrow replacement path as `OnBalanceExhausted`, and wire it from the scheduler when the cap gate rejects a candidate.

**H8 — instrument the burn reasons from day one.** `ghost_burns_total{reason}` (including `ghostAbandoned`) and the drain-budget trip counter are the only way to see H1/H2 in production; the `hold` grace stops firing exactly under the load where nonce budget matters most, so its hit rate must be visible too.

**Ordering note:** the `perf.Ejected` full-scan cost (O(hosts) per call, taken under `Capacity`'s read lock) must be fixed before Task 6, otherwise the "lock-free" actor spends its time blocked on `perf.Tracker`'s global mutex.

---

## Task 1: limits/ — non-mutating `Available` peek

**Files:** Modify `cmd/gateway/limits/participant.go`; extend `cmd/gateway/limits/participant_test.go`.

**Interfaces produced:** `func (l *ParticipantLimiter) Available(participant, model string) bool`.

**Behavior (research §5.4):** returns true iff a subsequent `Acquire(participant, model)` *would* succeed right now — breaker not Open (`!now.Before(state.openUntil)`, honoring half-open) AND `inflight < effectiveWindow` — WITHOUT mutating `inflight` or any state. A missing host (no state yet) is available (mirrors Acquire creating a fresh full-window state). Share the window/breaker math with `Acquire` (extract a `wouldAdmitLocked` helper both call — do NOT duplicate the AIMD/breaker logic).

**Tests:** available on a fresh key; not available when breaker Open; not available when `inflight >= window`; available again after Release drops inflight below window; half-open probe availability matches Acquire's; `Available` does NOT change `inflight` (call it N times, then assert Acquire still admits exactly the window count).

**Steps:** failing test → extract `wouldAdmitLocked` + `Available` → green → `-race` → commit.

---

## Task 2: chain/ — governance max_nonce in the snapshot

**Files:** Modify `cmd/gateway/chain/observer_fetch.go` (decode escrow-params) + `cmd/gateway/chain/snapshot.go` (`PhaseSnapshot.MaxNonce uint64`); tests.

**Interfaces produced:** `PhaseSnapshot.MaxNonce uint64` (the governance `devshard_escrow_params.max_nonce`; 0 = not yet fetched).

**Behavior (research §4, §6-4):** the observer fetches `devshard_escrow_params.max_nonce` (the governance-votable param the host enforces via `types.MaxActiveNonce`). Find the REST path for `devshard_escrow_params` (grep `devshard/host` + the proto for the query route). Poll it alongside the existing epoch/participants fetch, publish it on the snapshot. `MaxNonce == 0` (not loaded) means the nonce-cap gate is disabled (cold-start-safe — never deactivate an escrow on missing data). Do NOT compute the cutoff here — the scheduler derives `types.MaxActiveNonce(snapshot.MaxNonce, groupSize)` per escrow (group size is per-escrow).

**Tests:** observer decodes `max_nonce` from a canned escrow-params JSON into `Snapshot().MaxNonce`; a fetch failure leaves the rest of the snapshot intact + `MaxNonce` at its prior value (fail-open); `MaxNonce==0` before first fetch.

**Steps:** failing decode test → add field + fetch + publish → green → `-race` → commit.

---

## Task 3: scheduler/ — types, interfaces, errors, ghost kinds

**Files:** Create `cmd/gateway/scheduler/scheduler.go` (types + interface only, no logic yet), `errors.go`, `ghost.go`; `types_test.go` if any pure helper needs it.

**Interfaces produced:**
```go
type Scheduler interface {
	Pick(ctx context.Context, profile RequestProfile) (Assignment, error)
	BlockHost(escrowID, participant string) // engine-fed 6th filter: permanent state-divergence block
}
type RequestProfile struct {
	Model         string
	Escrow        string   // pinned escrow for escalation; "" = pick one
	InputTokens   int
	RequiresTools bool
	ContextHint   uint64
	Exclude       []string // participant keys already raced in this request
	Params        any      // opaque real-dispatch payload forwarded to session.Advance
	AffinityHint  *AffinityHint
}
type Assignment struct { Escrow string; Host string; Nonce Prepared }
type AffinityHint struct{ /* stub, Task 8 */ }

type Decision interface{ isDecision() }
type serve struct{ waiter *waiter }
type burn  struct{ kind GhostKind }
type hold  struct{ until time.Time }
type GhostKind int
const ( ghostPoC GhostKind = iota; ghostThrottled; ghostCapability; ghostExclude )
```
plus `escrowSource`, `Escrow`, `session`, `HostBinding`, `Prepared` from the Shared-interfaces block. Errors: `var ErrNoAvailableHost`, `ErrAllHostsExcluded`, `ErrNoEscrowCapacity` (the all-zero-W(e) 429-class; does NOT name a host — research §3).

**Tests:** `GhostKind.reason()` label mapping (the four `poc_unavailable_host`/`participant_throttled_no_send`/`participant_capability_no_send`/`no_compatible_request_after_stale` strings, research §1.4); the `Decision` types satisfy the interface (compile-level). No behavior yet.

---

## Task 4: scheduler/match.go — the pure decision core

**Files:** Create `cmd/gateway/scheduler/match.go`, `match_test.go`.

**Interfaces produced:**
```go
type waiter struct {
	profile  RequestProfile
	exclude  map[string]bool // profile.Exclude as a set, participant-keyed
	replyCh  chan result
	enqueued time.Time
}
type Availability struct {
	pocRequired  func(participant string) bool // chain snapshot Preserved/PreservedByModel
	throttled    func(participant string) bool // !limits.Available (breaker/window)
	capability   func(participant string, p RequestProfile) bool // perf.CannotServe OR state-block
	stateBlocked func(participant string) bool // the 6th filter (per-escrow, from BlockHost)
}
func match(b HostBinding, waiting []*waiter, avail Availability, now time.Time, stale time.Duration) Decision
```

**Behavior (research §1, §2 — the ordered filter table; this is THE parity-critical core):**
1. **Host-level filters (burn regardless of waiters), in this exact order (research §1.4 — order is log-label fidelity, pinned by a test):** `avail.pocRequired(b.Participant)` → `burn{ghostPoC}`; else `avail.throttled(b.Participant)` → `burn{ghostThrottled}`.
2. **Waiter match:** scan `waiting` in order (oldest first); the first `w` where `!w.exclude[b.Participant]` AND `!avail.capability(b.Participant, w.profile)` AND `!avail.stateBlocked(b.Participant)` → `serve{w}`.
3. **No compatible waiter:** if `now.Before(oldest.enqueued + stale)` → `hold{until: oldest.enqueued+stale}` (co-arrival grace; nonce NOT committed). Else if some scanned waiter was capability/state-blocked on this host → `burn{ghostCapability}`; else → `burn{ghostExclude}`.

`match` is PURE and TOTAL — no I/O, no lock, no `time.Now`; every path returns a `Decision`. The exhaustiveness is the liveness invariant. Availability predicates are injected (the dispatcher builds them from chain/limits/perf each drain).

**Tests (exhaustive — each pins an invariant/filter):** PoC-required host → ghostPoC; throttled → ghostThrottled; PoC beats throttle (label order); a compatible waiter → serve (oldest of several); waiter excludes the host but a later waiter doesn't → serve the later one (CONSERVATION); all waiters exclude the host, still within stale → hold (no commit); past stale with a capability block → ghostCapability; past stale plain → ghostExclude; multi-slot: two slots of one participant, waiter excluded it → neither slot serves (participant keying); liveness: property-style check that every (binding, waiting, avail) yields exactly one Decision kind.

---

## Task 5: scheduler/escrow_pick.go — W(e) escrow selection

**Files:** Create `cmd/gateway/scheduler/escrow_pick.go`, `escrow_pick_test.go`.

**Interfaces consumed:** `escrowSource.Candidates`, `limits.Capacity.EscrowWeight(escrowID, model)`, `chain.PhaseSnapshot` (for `MaxNonce`), `types.MaxActiveNonce`.

**Interfaces produced:** `func (s *Scheduler) pickEscrow(profile RequestProfile, snapshot chain.PhaseSnapshot) (Escrow, error)`.

**Behavior (research §3):** if `profile.Escrow != ""` → return that pinned escrow from Candidates (escalation path; error if it's gone). Else enumerate `Candidates(profile.Model)` (already active/phase-filtered by the source); drop any at the nonce cap (`snapshot.MaxNonce > 0 && e.Session.LatestNonce() >= types.MaxActiveNonce(snapshot.MaxNonce, e.Session.GroupSize())` — cap disabled when MaxNonce==0); if none remain → `ErrNoEscrowCapacity`. Score each by the load ratio `float64(e.ActiveUsers) / W(e)` where `W(e) = limits.Capacity.EscrowWeight(e.ID, model)`; `W(e) <= 0` → score `+Inf`. Pick the min-score set; if all `+Inf` → `ErrNoEscrowCapacity` (429-class, no host named). Ties → a shared `atomic.Int64` counter modulo the tie-set size (document: shared-counter pseudo-round-robin, NOT fair per-model rotation — research §6-7). **Do NOT** trigger rotation replacement here (that side-effect is the escrow Manager's depletion loop — research §6-6).

**Tests:** pinned escrow returned directly; nonce-capped escrow dropped (and MaxNonce==0 disables the cap); lowest load-ratio wins; equal ratio → tie-break advances the shared counter; all-zero-W(e) → ErrNoEscrowCapacity; model filter; empty candidates → error.

---

## Task 6: scheduler/dispatcher.go — the per-escrow actor

**Files:** Create `cmd/gateway/scheduler/dispatcher.go`, `dispatcher_test.go`.

**Interfaces produced:**
```go
type dispatcher struct { /* owns: session, waiting []*waiter, submit chan, stop chan, avail builders, now, stale, clock */ }
func newDispatcher(...) *dispatcher
func (d *dispatcher) start()                       // launches loop() goroutine
func (d *dispatcher) stop()                          // idempotent; blocks until loop exits
func (d *dispatcher) submitWaiter(w *waiter) bool    // enqueue; false if stopping
```

**Behavior (research §1 — the actor; lock-free, the goroutine owns all state):**
- `loop()`: `select` on `submit` (append waiter, `drain()`), the hold-timer firing (`drain()`), or `stop` (return). No mutex — the goroutine is the sole owner of `waiting`/`session`.
- `drain()`: loop — (1) `sweepExhausted()`: reply `ErrNoAvailableHost` to any waiter whose exclude set covers every currently-available (non-PoC, non-throttled, non-capability/state-blocked) participant, dequeue it (research §1.2 — instant, no wait, no ghost); (2) if `waiting` empty → return; (3) build the `Availability` predicates (from the injected chain snapshot / `limits.Available` / `perf.CannotServe`+`stateBlocked`); (4) `prepared, decision, err := session.Advance(func(b) Decision { return match(b, waiting, avail, now(), stale) })`; (5) apply: `serve` → reply `Assignment{escrow, host: participant, nonce: prepared}` to that waiter + dequeue + continue; `burn` → record ghost metric/log (no I/O) + continue; `hold` → arm the hold-timer for `until` + return (yield until co-arrival or timer).
- Termination: burns are bounded by `GroupSize` per waiter (nonce % group cycles hosts), so a viable waiter is served within ≤ group advances; a non-viable waiter is swept. `drain` always terminates.
- Lifecycle: `stop()` closes `stop`, waits for `loop` to exit (goleak-clean); any still-waiting waiters get `ErrNoAvailableHost` (or a shutdown error) on stop.

**Tests (deterministic — fake `session` returns a scripted binding sequence, injected clock for hold):** single waiter, first nonce usable → served, no burn; waiter excludes host of nonce N, host of N+1 usable → one ghost then served; two waiters, nonce binds host A excludes-for-w1 → serves w2 (CONSERVATION); sweep: waiter excluding all available → ErrNoAvailableHost instantly, nonce untouched; hold: no waiter matches within stale → nonce held (not committed), a co-arriving compatible waiter within stale → served on the held nonce; hold expiry → ghost burn; `start`/`stop` goleak-clean; stop replies pending waiters.

---

## Task 7: scheduler/scheduler.go — Pick orchestration + BlockHost + lifecycle

**Files:** Create the `Scheduler` struct + methods in `scheduler.go` (extend Task 3's file), `scheduler_test.go`; `affinity.go` (stub).

**Interfaces produced:** `func NewScheduler(deps Deps) *Scheduler`; `Pick`; `BlockHost`. `Deps{ Escrows escrowSource; Capacity *limits.Capacity; Limiter *limits.ParticipantLimiter; Perf *perf.Tracker; Snapshots snapshotSource; Config *config.Holder; Now func() time.Time }`.

**Behavior:**
- `Pick(ctx, profile)`: `snapshot := snapshots.Snapshot()`; `escrow, err := pickEscrow(profile, snapshot)` (Task 5); get-or-lazily-create that escrow's `dispatcher` (a `map[escrowID]*dispatcher` guarded by a small lifecycle mutex; the dispatcher captures the escrow's Session + the availability builders closing over snapshot/limits/perf/the state-block set); `w := &waiter{profile, exclude: set(profile.Exclude), replyCh}`; `submitWaiter(w)`; `select { <-w.replyCh → Assignment/err ; <-ctx.Done() → deregister + ctx.Err() }`.
- `BlockHost(escrowID, participant)`: add to a per-escrow `map[escrowID]set[participant]` (own mutex) that the dispatcher's `stateBlocked` predicate reads (research §2 row 6, §6-8) — **permanent, never cleared, and the doc comment on the method must say so.** This is a correctness safety valve, not a performance signal: a host that returned a mismatched post-state-root for an escrow must never receive another real dispatch on it for the process's lifetime. The contract previously lived on a placeholder interface that Task 5 removed; restating it on the method is not optional, because "never cleared" looks like an oversight to anyone who later adds eviction to that map.
- Dispatcher lifecycle: lazy-create on first Pick for an escrow; idle-reap (a dispatcher whose `waiting` has been empty for an idle grace stops itself + removes from the map, re-checking emptiness under the map mutex to avoid racing a fresh submit).
- `affinity.go`: `AffinityHint` type + a no-op `applyAffinity` hook the dispatcher's drain calls (returns "no preference" today) — the extension point only (design decision #2), no perf ranking.

**Tests:** Pick routes to the right escrow's dispatcher and returns its Assignment; escalation (profile.Escrow set + grown Exclude) reuses the same dispatcher; ctx-cancel mid-wait returns ctx.Err() + deregisters (no leaked waiter); BlockHost makes a host state-blocked for that escrow only (the dispatcher's next drain ghosts it); two escrows run independent dispatchers concurrently (-race); idle-reap stops an idle dispatcher (goleak); a Pick after reap re-creates it.

---

## Cross-task notes

- Old-code cites live in `.superpowers/sdd/p7-scheduler-research.md` (§1 state machine, §2 filter taxonomy, §3 W(e), §4 nonce mechanics, §5 the boundary, §6 the improvements-as-requirements). The improvements are REQUIREMENTS: sum-typed `Decision` (§6-1), ordered filter table (§6-2), config-tunable `stale` (§6-3), governance max_nonce (§6-4, Task 2), non-mutating `Available` (§6-5, Task 1), no rotation side-effect in escrow-pick (§6-6), documented pseudo-RR tie-break (§6-7), the 6th filter preserved (§6-8).
- `stale` (the hold grace, default 200ms) is a config value: add `config.Scheduler{ HoldGraceMS int64 }` (or reuse an existing queue-wait knob) — admin-tunable, not a Go const.
- **NOT wired into main** (P9/api wires `escrowSource`, the `session` adapter over `user.Session`, and calls `Pick`/`BlockHost`) — consistent with limits/perf/escrow. Built + fully tested against fakes.
- KV-affinity is stub-only this phase (`affinity.go` extension point, no ranking).
- Phase-end: KISS/over-engineering audit + comprehensive review (extra scrutiny on the liveness invariant + the actor's goroutine lifecycle/goleak + conservation) + comment sweep, then hand off for the user's commit.
