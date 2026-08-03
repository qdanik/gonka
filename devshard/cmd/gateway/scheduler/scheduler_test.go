package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/perf"

	"devshard/cmd/gateway/internal/leakcheck"
)

const escrowB = "escrow-b"

// fakeLimiter keeps the peek and the authority separately settable: unavailable turns the pre-filter
// off, refused lets a host pass the peek and still fail admission, and window is honoured by both.
type fakeLimiter struct {
	mu          sync.Mutex
	unavailable map[string]bool
	refused     map[string]bool
	window      int
	inflight    map[string]int
	admitted    int
	models      []string
}

func newFakeLimiter() *fakeLimiter {
	return &fakeLimiter{
		unavailable: map[string]bool{},
		refused:     map[string]bool{},
		inflight:    map[string]int{},
	}
}

func (f *fakeLimiter) Available(participant, model string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.models = append(f.models, model)
	return !f.unavailable[participant] && f.hasRoomLocked(participant)
}

func (f *fakeLimiter) Acquire(participant, model string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.refused[participant] || !f.hasRoomLocked(participant) {
		return false
	}
	f.inflight[participant]++
	f.admitted++
	return true
}

func (f *fakeLimiter) Release(participant, model string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inflight[participant]--
}

func (f *fakeLimiter) hasRoomLocked(participant string) bool {
	return f.window <= 0 || f.inflight[participant] < f.window
}

func (f *fakeLimiter) block(participant string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unavailable[participant] = true
}

// refuse leaves the peek saying yes, which is the window filling between selection and the commit.
func (f *fakeLimiter) refuse(participant string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refused[participant] = true
}

func (f *fakeLimiter) askedModels() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.models...)
}

// slots reports how many admissions have not been given back; a release without an acquire drives it
// negative rather than clamping, so a double release is visible.
func (f *fakeLimiter) slots() (held, admitted int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, count := range f.inflight {
		held += count
	}
	return held, f.admitted
}

type capabilityQuery struct {
	participant   string
	requiresTools bool
	contextHint   uint64
}

type fakePerf struct {
	mu      sync.Mutex
	blocked map[string]string
	ejected map[string]bool
	queries []capabilityQuery
}

func (f *fakePerf) Ejected(participant, _ string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ejected[participant]
}

func (f *fakePerf) CannotServe(participant string, requiresTools bool, contextHint uint64) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, capabilityQuery{participant: participant, requiresTools: requiresTools, contextHint: contextHint})
	reason, blocked := f.blocked[participant]
	return reason, blocked
}

func (f *fakePerf) eject(participant string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ejected[participant] = true
}

func (f *fakePerf) block(participant, reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked[participant] = reason
}

func (f *fakePerf) asked() []capabilityQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]capabilityQuery(nil), f.queries...)
}

type schedulerConfig struct {
	escrows      []string
	slots        []string
	holdGraceMS  int64
	submitBuffer int
	hostWindow   int
	gate         chan struct{}
	health       hostHealth
}

// escrowHolds counts the in-flight holds the commit takes, so an assignment dropped without giving one
// back shows up as a hold that never came home.
type escrowHolds struct {
	mu   sync.Mutex
	open int
}

func (h *escrowHolds) source() func() (func(), bool) {
	return func() (func(), bool) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.open++
		var once sync.Once
		return func() {
			once.Do(func() {
				h.mu.Lock()
				defer h.mu.Unlock()
				h.open--
			})
		}, true
	}
}

func (h *escrowHolds) outstanding() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.open
}

type schedulerHarness struct {
	scheduler *Scheduler
	escrows   *fakeEscrows
	holds     *escrowHolds
	weights   *fakeWeights
	sessions  map[string]*scriptedSession
	limiter   *fakeLimiter
	perf      *fakePerf
	snapshots *fakeSnapshots
	observer  *recordingObserver
	clock     *testClock
}

func newSchedulerHarness(t *testing.T, cfg schedulerConfig) *schedulerHarness {
	t.Helper()
	if len(cfg.escrows) == 0 {
		cfg.escrows = []string{escrowA}
	}
	if len(cfg.slots) == 0 {
		cfg.slots = []string{hostA, hostB}
	}

	escrows := &fakeEscrows{byModel: map[string][]Escrow{}}
	weights := &fakeWeights{byEscrow: map[string]float64{}}
	sessions := map[string]*scriptedSession{}
	holds := &escrowHolds{}
	for _, escrowID := range cfg.escrows {
		session := &scriptedSession{slots: cfg.slots, gate: cfg.gate, entered: make(chan struct{}, 1)}
		sessions[escrowID] = session
		escrows.byModel[modelA] = append(escrows.byModel[modelA], Escrow{ID: escrowID, Model: modelA, Session: session, Hold: holds.source()})
		weights.byEscrow[escrowID] = 10
	}

	settings := config.Defaults()
	settings.Scheduler.HoldGraceMS = cfg.holdGraceMS

	limiter := newFakeLimiter()
	limiter.window = cfg.hostWindow
	test := &schedulerHarness{
		escrows:   escrows,
		holds:     holds,
		weights:   weights,
		sessions:  sessions,
		limiter:   limiter,
		perf:      &fakePerf{blocked: map[string]string{}, ejected: map[string]bool{}},
		snapshots: &fakeSnapshots{},
		observer:  &recordingObserver{},
		clock:     newTestClock(),
	}
	health := cfg.health
	if health == nil {
		health = test.perf
	}
	test.scheduler = NewScheduler(Deps{
		Escrows:      escrows,
		Capacity:     weights,
		Limiter:      test.limiter,
		Perf:         health,
		Snapshots:    test.snapshots,
		Config:       config.NewHolder(&settings),
		Observer:     test.observer,
		Now:          test.clock.Now,
		SubmitBuffer: cfg.submitBuffer,
	})
	test.scheduler.newTimer = test.clock.newTimer
	t.Cleanup(test.scheduler.Stop)
	return test
}

func (h *schedulerHarness) session(t *testing.T, escrowID string) *scriptedSession {
	t.Helper()
	session, ok := h.sessions[escrowID]
	if !ok {
		t.Fatalf("no session for escrow %q", escrowID)
	}
	return session
}

func (h *schedulerHarness) escrow(t *testing.T, escrowID string) Escrow {
	t.Helper()
	for _, candidate := range h.escrows.byModel[modelA] {
		if candidate.ID == escrowID {
			return candidate
		}
	}
	t.Fatalf("no escrow %q", escrowID)
	return Escrow{}
}

func (h *schedulerHarness) liveDispatchers() map[string]*dispatcher {
	h.scheduler.registryMu.Lock()
	defer h.scheduler.registryMu.Unlock()
	live := make(map[string]*dispatcher, len(h.scheduler.dispatchers))
	for escrowID, active := range h.scheduler.dispatchers {
		live[escrowID] = active
	}
	return live
}

func (h *schedulerHarness) queueDepth(escrowID string) int {
	h.scheduler.registryMu.Lock()
	defer h.scheduler.registryMu.Unlock()
	active, ok := h.scheduler.dispatchers[escrowID]
	if !ok {
		return 0
	}
	return len(active.submit)
}

func eventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func wantHost(t *testing.T, assignment Assignment, err error, escrowID, host string, nonce uint64) {
	t.Helper()
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if assignment.Escrow != escrowID {
		t.Fatalf("assignment escrow = %q, want %q", assignment.Escrow, escrowID)
	}
	if assignment.Host != host {
		t.Fatalf("assignment host = %q, want %q", assignment.Host, host)
	}
	if assignment.Nonce == nil || assignment.Nonce.Nonce() != nonce {
		t.Fatalf("assignment nonce = %v, want %d", assignment.Nonce, nonce)
	}
}

func TestPickAssignsFromTheEscrowItChose(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{})

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, Params: "payload"})

	wantHost(t, assignment, err, escrowA, hostB, 1)
	_, _, commits := test.session(t, escrowA).report()
	if len(commits) != 1 || commits[0].ghost || commits[0].params != "payload" {
		t.Fatalf("commits = %+v, want one real dispatch carrying the request params", commits)
	}
}

func TestPickRoutesToTheLeastLoadedEscrow(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{escrows: []string{escrowA, escrowB}})
	test.escrows.byModel[modelA][0].ActiveUsers = 9

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	wantHost(t, assignment, err, escrowB, hostB, 1)
	if advances, _, _ := test.session(t, escrowA).report(); advances != 0 {
		t.Fatalf("the busier escrow advanced %d nonces, want 0", advances)
	}
}

func TestPickEscalationReusesTheEscrowsDispatcher(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{})

	first, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})
	wantHost(t, first, err, escrowA, hostB, 1)
	created := test.liveDispatchers()
	weighed := len(test.weights.lookups)

	escalated, err := test.scheduler.Pick(context.Background(), RequestProfile{
		Model:   modelA,
		Escrow:  first.Escrow,
		Exclude: []string{first.Host},
	})

	wantHost(t, escalated, err, escrowA, hostA, 2)
	live := test.liveDispatchers()
	if len(live) != 1 || live[escrowA] != created[escrowA] {
		t.Fatalf("escalation ran on %d dispatcher(s) and replaced the original: %v", len(live), live)
	}
	if len(test.weights.lookups) != weighed {
		t.Fatalf("escalation re-derived the escrow: weighed %v", test.weights.lookups[weighed:])
	}
}

func TestPickReturnsTheContextErrorWhenCancelledWhileQueued(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{holdGraceMS: 200})
	ctx, cancel := context.WithCancel(context.Background())

	// The nonce binds the one host this request excludes, so it is held rather than served or burned.
	picked := make(chan error, 1)
	go func() {
		_, err := test.scheduler.Pick(ctx, RequestProfile{Model: modelA, Exclude: []string{hostB}})
		picked <- err
	}()
	if delay := test.clock.awaitArmed(t); delay != 200*time.Millisecond {
		t.Fatalf("hold delay = %v, want the configured grace", delay)
	}

	cancel()

	select {
	case err := <-picked:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Pick err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pick did not return after its context was cancelled")
	}

	drains := test.snapshots.fetches()
	test.clock.advance(200 * time.Millisecond)
	test.clock.fireTimer()
	eventually(t, "the drain that follows the expired hold", func() bool { return test.snapshots.fetches() > drains })

	advances, declines, commits := test.session(t, escrowA).report()
	if advances != 1 || declines != 1 || len(commits) != 0 {
		t.Fatalf("advances=%d declines=%d commits=%+v, want the abandoned request to cost no nonce", advances, declines, commits)
	}
	if burns := test.observer.burns(); len(burns) != 0 {
		t.Fatalf("ghost burns = %v, want none", burns)
	}
	test.scheduler.Stop()
}

// Every admission and every escrow hold ends up either with the caller that will release it or given
// back here; a cancellation racing the handoff must not be able to strand one in between.
func TestPickReleasesTheSlotWhenCancellationRacesTheAssignment(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{})
	const races = 200

	takenByCallers := 0
	for range races {
		ctx, cancel := context.WithCancel(context.Background())
		go cancel()
		assignment, err := test.scheduler.Pick(ctx, RequestProfile{Model: modelA})
		if err == nil {
			takenByCallers++
			test.limiter.Release(assignment.Host, modelA)
			assignment.ReleaseEscrow()
		}
	}
	test.scheduler.Stop()

	held, admitted := test.limiter.slots()
	if held != 0 {
		t.Fatalf("slots held = %d after %d races, want every admission accounted for", held, races)
	}
	if outstanding := test.holds.outstanding(); outstanding != 0 {
		t.Fatalf("escrow holds still out = %d after %d races, want every hold accounted for", outstanding, races)
	}
	dropped := 0
	for _, reason := range test.observer.burns() {
		if reason == ghostAbandoned.reason() {
			dropped++
		}
	}
	if admitted != takenByCallers+dropped {
		t.Fatalf("admitted = %d, want the %d taken by callers plus the %d recorded as abandoned",
			admitted, takenByCallers, dropped)
	}
}

func TestBlockHostAppliesToOneEscrowOnly(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{escrows: []string{escrowA, escrowB}})

	test.scheduler.BlockHost(escrowA, hostB)

	blocked, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, Escrow: escrowA})
	wantHost(t, blocked, err, escrowA, hostA, 2)
	unaffected, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, Escrow: escrowB})
	wantHost(t, unaffected, err, escrowB, hostB, 1)

	_, _, blockedCommits := test.session(t, escrowA).report()
	if len(blockedCommits) != 2 || !blockedCommits[0].ghost || blockedCommits[0].participant != hostB {
		t.Fatalf("blocked escrow commits = %+v, want the blocked host ghosted then a real dispatch", blockedCommits)
	}
	_, _, otherCommits := test.session(t, escrowB).report()
	if len(otherCommits) != 1 || otherCommits[0].ghost {
		t.Fatalf("unblocked escrow commits = %+v, want a single real dispatch on the same host", otherCommits)
	}
}

func TestPickWiresEachAvailabilityPredicateToItsSource(t *testing.T) {
	testCases := []struct {
		name       string
		arrange    func(test *schedulerHarness)
		profile    RequestProfile
		wantReason string
	}{
		{
			name:       "a participant outside the PoC-preserved set",
			arrange:    func(test *schedulerHarness) { test.snapshots.set(chain.PhaseSnapshot{Preserved: []string{hostA}}) },
			profile:    RequestProfile{Model: modelA},
			wantReason: ghostPoC.reason(),
		},
		{
			name:       "a participant the limiter reports unavailable",
			arrange:    func(test *schedulerHarness) { test.limiter.block(hostB) },
			profile:    RequestProfile{Model: modelA},
			wantReason: ghostThrottled.reason(),
		},
		{
			name:       "a participant that cannot serve the request",
			arrange:    func(test *schedulerHarness) { test.perf.block(hostB, "context_limit_exceeded") },
			profile:    RequestProfile{Model: modelA},
			wantReason: ghostCapability.reason(),
		},
		{
			name:       "a participant the outlier detector ejected",
			arrange:    func(test *schedulerHarness) { test.perf.eject(hostB) },
			profile:    RequestProfile{Model: modelA},
			wantReason: ghostEjected.reason(),
		},
		{
			name:       "a participant the request already raced",
			arrange:    func(*schedulerHarness) {},
			profile:    RequestProfile{Model: modelA, Exclude: []string{hostB}},
			wantReason: ghostExclude.reason(),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			test := newSchedulerHarness(t, schedulerConfig{})
			testCase.arrange(test)

			assignment, err := test.scheduler.Pick(context.Background(), testCase.profile)

			wantHost(t, assignment, err, escrowA, hostA, 2)
			if burns := test.observer.burns(); len(burns) != 1 || burns[0] != testCase.wantReason {
				t.Fatalf("ghost burns = %v, want exactly one %q", burns, testCase.wantReason)
			}
		})
	}
}

// failUntilEjected feeds one host enough consecutive failures to trip the detector, and every other host
// one success, so the model's known membership is what the pool-wide cap is computed over.
func failUntilEjected(tracker *perf.Tracker, healthy []string, failing ...string) {
	for _, participant := range healthy {
		tracker.RecordSample(perf.Sample{ParticipantKey: participant, Model: modelA, Responsive: true})
	}
	for range config.Defaults().Perf.ConsecutiveFailThreshold {
		for _, participant := range failing {
			tracker.RecordSample(perf.Sample{ParticipantKey: participant, Model: modelA})
		}
	}
}

// The routing gate reads the detector itself, not a predicate a test wrote: samples go into a real
// tracker and the ejected host is simply never handed the request.
func TestPickWithholdsAHostTheOutlierDetectorEjected(t *testing.T) {
	leakcheck.VerifyNone(t)
	settings := config.Defaults()
	tracker := perf.NewTracker(config.NewHolder(&settings), time.Now)
	failUntilEjected(tracker, []string{hostA}, hostB)
	test := newSchedulerHarness(t, schedulerConfig{health: tracker})

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	wantHost(t, assignment, err, escrowA, hostA, 2)
	if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostEjected.reason() {
		t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostEjected.reason())
	}
	_, _, commits := test.session(t, escrowA).report()
	for _, commit := range commits {
		if commit.participant == hostB && !commit.ghost {
			t.Fatalf("commits = %+v, want no real dispatch to the ejected host", commits)
		}
	}
}

// The cap inside the tracker is what makes the gate safe to honour: when every host fails at once, the
// gate must still leave the fleet servable rather than turning an outage into a total refusal.
func TestPickStillServesWhenEveryHostIsFailingAtOnce(t *testing.T) {
	leakcheck.VerifyNone(t)
	settings := config.Defaults()
	tracker := perf.NewTracker(config.NewHolder(&settings), time.Now)
	failUntilEjected(tracker, nil, hostA, hostB)
	test := newSchedulerHarness(t, schedulerConfig{health: tracker})

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	if err != nil {
		t.Fatalf("Pick: %v, want a host the cap kept in rotation", err)
	}
	if tracker.Ejected(assignment.Host, modelA) {
		t.Fatalf("assignment host = %q, which the gate reports as withheld from routing", assignment.Host)
	}
	if !tracker.Degraded(assignment.Host, modelA) {
		t.Fatalf("assignment host = %q, want one the detector wanted out but the cap kept", assignment.Host)
	}
}

// Admission is decided inside the same step that commits the nonce, so a host that passes the peek and
// then fails the window costs a ghost the accounting can see -- never a live nonce nobody settles.
func TestPickGhostsTheNonceWhenAdmissionRefusesTheBoundHost(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{})
	test.limiter.refuse(hostB)

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	wantHost(t, assignment, err, escrowA, hostA, 2)
	_, _, commits := test.session(t, escrowA).report()
	if len(commits) != 2 || !commits[0].ghost || commits[0].participant != hostB {
		t.Fatalf("commits = %+v, want the refused host's nonce committed as a ghost", commits)
	}
	if commits[1].ghost || commits[1].participant != hostA {
		t.Fatalf("commits = %+v, want the admitted host's nonce dispatched for real", commits)
	}
	if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostThrottled.reason() {
		t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostThrottled.reason())
	}
	if held, admitted := test.limiter.slots(); held != 1 || admitted != 1 {
		t.Fatalf("slots held/admitted = %d/%d, want only the served host's slot taken", held, admitted)
	}
}

func TestPickAdmitsExactlyOneCallerThroughAWindowOfOne(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{slots: []string{hostA}, hostWindow: 1})
	const callers = 8

	var group sync.WaitGroup
	served := make(chan Assignment, callers)
	rejected := make(chan error, callers)
	for range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})
			if err != nil {
				rejected <- err
				return
			}
			served <- assignment
		}()
	}
	group.Wait()
	close(served)
	close(rejected)

	if len(served) != 1 {
		t.Fatalf("served = %d callers, want exactly one through a window of one", len(served))
	}
	for err := range rejected {
		if !errors.Is(err, ErrNoAvailableHost) {
			t.Fatalf("rejected caller err = %v, want ErrNoAvailableHost", err)
		}
	}
	_, _, commits := test.session(t, escrowA).report()
	ghosts := 0
	for _, commit := range commits {
		if commit.ghost {
			ghosts++
		}
	}
	if burns := test.observer.burns(); ghosts != len(commits)-1 || len(burns) != ghosts {
		t.Fatalf("commits = %+v with burns %v, want every nonce but the served one ghosted and recorded", commits, burns)
	}
	if held, admitted := test.limiter.slots(); held != 1 || admitted != 1 {
		t.Fatalf("slots held/admitted = %d/%d, want the one admission still held by its caller", held, admitted)
	}

	test.limiter.Release((<-served).Host, modelA)

	if held, _ := test.limiter.slots(); held != 0 {
		t.Fatalf("slots held after the caller released = %d, want 0", held)
	}
}

func TestPickAsksTheHostSourcesWithTheRequestsOwnKeys(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{})

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, RequiresTools: true, ContextHint: 4096})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}

	for _, model := range test.limiter.askedModels() {
		if model != modelA {
			t.Fatalf("limiter asked about model %q, want %q", model, modelA)
		}
	}
	queries := test.perf.asked()
	if len(queries) == 0 {
		t.Fatal("the capability source was never asked")
	}
	for _, query := range queries {
		if !query.requiresTools || query.contextHint != 4096 {
			t.Fatalf("capability query = %+v, want the request's own tool and context keys", query)
		}
	}
}

func TestPickTreatsAnUnloadedPreservedSetAsAllPreserved(t *testing.T) {
	t.Run("a nil set serves every participant", func(t *testing.T) {
		test := newSchedulerHarness(t, schedulerConfig{})
		test.snapshots.set(chain.PhaseSnapshot{Preserved: nil, PreservedByModel: nil})

		assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

		wantHost(t, assignment, err, escrowA, hostB, 1)
		if burns := test.observer.burns(); len(burns) != 0 {
			t.Fatalf("ghost burns = %v, want none: an unloaded set must not ghost the whole group", burns)
		}
	})

	t.Run("a loaded but empty set preserves nobody", func(t *testing.T) {
		test := newSchedulerHarness(t, schedulerConfig{})
		test.snapshots.set(chain.PhaseSnapshot{Preserved: []string{}})

		_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

		if !errors.Is(err, ErrNoAvailableHost) {
			t.Fatalf("err = %v, want ErrNoAvailableHost", err)
		}
	})

	t.Run("the per-model set wins over the global one", func(t *testing.T) {
		test := newSchedulerHarness(t, schedulerConfig{})
		test.snapshots.set(chain.PhaseSnapshot{
			Preserved:        []string{hostA, hostB},
			PreservedByModel: map[string][]string{modelA: {hostA}},
		})

		assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

		wantHost(t, assignment, err, escrowA, hostA, 2)
		if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostPoC.reason() {
			t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostPoC.reason())
		}
	})

	t.Run("another model's set does not shadow the global one", func(t *testing.T) {
		test := newSchedulerHarness(t, schedulerConfig{})
		test.snapshots.set(chain.PhaseSnapshot{
			Preserved:        []string{hostA},
			PreservedByModel: map[string][]string{"model-other": {hostA, hostB}},
		})

		assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

		wantHost(t, assignment, err, escrowA, hostA, 2)
	})
}

func TestPickRunsEscrowsOnIndependentDispatchers(t *testing.T) {
	test := newSchedulerHarness(t, schedulerConfig{escrows: []string{escrowA, escrowB}})
	const perEscrow = 8

	var group sync.WaitGroup
	results := make(chan Assignment, 2*perEscrow)
	failures := make(chan error, 2*perEscrow)
	for _, escrowID := range []string{escrowA, escrowB} {
		for range perEscrow {
			group.Add(1)
			go func() {
				defer group.Done()
				assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA, Escrow: escrowID})
				if err != nil {
					failures <- err
					return
				}
				results <- assignment
			}()
		}
	}
	group.Wait()
	close(results)
	close(failures)

	if err := <-failures; err != nil {
		t.Fatalf("Pick: %v", err)
	}
	served := map[string]int{}
	for assignment := range results {
		served[assignment.Escrow]++
	}
	if served[escrowA] != perEscrow || served[escrowB] != perEscrow {
		t.Fatalf("served = %v, want %d per escrow", served, perEscrow)
	}
	if live := test.liveDispatchers(); len(live) != 2 {
		t.Fatalf("live dispatchers = %d, want one per escrow", len(live))
	}
	for _, escrowID := range []string{escrowA, escrowB} {
		if advances, _, commits := test.session(t, escrowID).report(); advances != perEscrow || len(commits) != perEscrow {
			t.Fatalf("escrow %q: advances=%d commits=%d, want %d each", escrowID, advances, len(commits), perEscrow)
		}
	}
}

func TestPickSurfacesBackPressureWhenTheQueueIsFull(t *testing.T) {
	gate := make(chan struct{})
	test := newSchedulerHarness(t, schedulerConfig{submitBuffer: 1, gate: gate})
	session := test.session(t, escrowA)

	var inFlight sync.WaitGroup
	inFlight.Add(1)
	go func() {
		defer inFlight.Done()
		if _, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA}); err != nil {
			t.Errorf("Pick: %v", err)
		}
	}()
	select {
	case <-session.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the dispatcher never reached the session")
	}

	inFlight.Add(1)
	go func() {
		defer inFlight.Done()
		if _, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA}); err != nil {
			t.Errorf("Pick: %v", err)
		}
	}()
	eventually(t, "the submit queue to fill", func() bool { return test.queueDepth(escrowA) == 1 })

	_, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	if !errors.Is(err, ErrEscrowBusy) {
		t.Fatalf("err = %v, want ErrEscrowBusy", err)
	}
	close(gate)
	inFlight.Wait()
}

// A caller claims its escrow's dispatcher under the registry lock and submits after releasing it, so
// the window this covers is the one an idle actor could otherwise retire underneath the caller.
func TestClaimedDispatcherOutlivesTheIdleReaper(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{})
	escrow := test.escrow(t, escrowA)

	claimed, err := test.scheduler.dispatcherFor(escrow)
	if err != nil {
		t.Fatalf("dispatcherFor: %v", err)
	}

	if test.scheduler.retire(claimed) {
		t.Fatal("retired a dispatcher a caller had already claimed")
	}
	claimed.pendingSubmits.Add(-1)
	if !test.scheduler.retire(claimed) {
		t.Fatal("refused to retire an unclaimed idle dispatcher")
	}

	if live := test.liveDispatchers(); len(live) != 0 {
		t.Fatalf("live dispatchers = %d, want the retired one gone", len(live))
	}
	test.scheduler.Stop()
}

func TestPickReplacesAStoppedDispatcher(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{})

	stale, err := test.scheduler.dispatcherFor(test.escrow(t, escrowA))
	if err != nil {
		t.Fatalf("dispatcherFor: %v", err)
	}
	stale.pendingSubmits.Add(-1)
	if !stale.markStopped() {
		t.Fatal("markStopped refused an idle dispatcher")
	}

	assignment, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	wantHost(t, assignment, err, escrowA, hostB, 1)
	if live := test.liveDispatchers(); live[escrowA] == stale {
		t.Fatal("Pick submitted to the stopped dispatcher still in the registry")
	}
	test.scheduler.Stop()
}

func TestSchedulerStopIsIdempotent(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{})

	if _, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA}); err != nil {
		t.Fatalf("Pick: %v", err)
	}

	test.scheduler.Stop()
	test.scheduler.Stop()

	if _, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA}); !errors.Is(err, ErrDispatcherStopped) {
		t.Fatalf("err = %v, want ErrDispatcherStopped", err)
	}
}

func TestSchedulerReapsAnIdleDispatcherAndRecreatesItOnDemand(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{})

	first, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})
	wantHost(t, first, err, escrowA, hostB, 1)

	if delay := test.clock.awaitArmed(t); delay != idleDispatcherGrace {
		t.Fatalf("idle delay = %v, want %v", delay, idleDispatcherGrace)
	}
	test.clock.fireTimer()
	eventually(t, "the idle dispatcher to retire", func() bool { return len(test.liveDispatchers()) == 0 })

	recreated, err := test.scheduler.Pick(context.Background(), RequestProfile{Model: modelA})

	wantHost(t, recreated, err, escrowA, hostA, 2)
	if live := test.liveDispatchers(); len(live) != 1 {
		t.Fatalf("live dispatchers = %d, want the escrow's dispatcher back", len(live))
	}
	test.scheduler.Stop()
}

// blockedHosts is keyed by an escrow id the chain never reuses, so an entry that outlives its escrow is
// never read again and never freed. One per escrow that saw a divergent host, for the process lifetime.
func TestRetiringAnEscrowForgetsItsBlockedHosts(t *testing.T) {
	leakcheck.VerifyNone(t)
	test := newSchedulerHarness(t, schedulerConfig{})
	escrow := test.escrow(t, escrowA)
	claimed, err := test.scheduler.dispatcherFor(escrow)
	if err != nil {
		t.Fatalf("dispatcherFor: %v", err)
	}
	claimed.pendingSubmits.Add(-1)
	test.scheduler.BlockHost(escrowA, "host-a")

	if !test.scheduler.retire(claimed) {
		t.Fatal("refused to retire an unclaimed idle dispatcher")
	}

	test.scheduler.blocksMu.RLock()
	defer test.scheduler.blocksMu.RUnlock()
	if _, held := test.scheduler.blockedHosts[escrowA]; held {
		t.Fatal("the retired escrow's block list is still held")
	}
}
