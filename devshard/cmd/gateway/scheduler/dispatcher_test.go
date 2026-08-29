package scheduler

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

const escrowA = "escrow-a"

type committedNonce struct {
	nonce       uint64
	participant string
	ghost       bool
	params      any
}

// scriptedSession binds nonce N to slot N%len(slots), the same rule the real session uses, so a test
// picks the host sequence by choosing the slot list.
type scriptedSession struct {
	balance         uint64
	mu              sync.Mutex
	slots           []string
	nonce           uint64
	advances        int
	declines        int
	commits         []committedNonce
	failWith        error
	failAfterDecide error
	swallowCommit   bool
	gate            chan struct{}
	entered         chan struct{}
	afterDecide     func(HostBinding)
}

func (s *scriptedSession) Advance(decide func(HostBinding) NonceIntent) (Prepared, error) {
	if s.gate != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.advances++
	if s.failWith != nil {
		return nil, s.failWith
	}
	nonce := s.nonce + 1
	hostIdx := int(nonce % uint64(len(s.slots)))
	binding := HostBinding{Nonce: nonce, HostIdx: hostIdx, Participant: s.slots[hostIdx]}

	intent := decide(binding)
	if s.afterDecide != nil {
		s.afterDecide(binding)
	}
	if s.failAfterDecide != nil {
		return nil, s.failAfterDecide
	}
	if !intent.Commit {
		s.declines++
		return nil, nil
	}
	if s.swallowCommit {
		return nil, nil
	}
	s.nonce = nonce
	s.commits = append(s.commits, committedNonce{
		nonce:       nonce,
		participant: binding.Participant,
		ghost:       intent.Ghost,
		params:      intent.Params,
	})
	return preparedNonce{nonce: nonce, hostIdx: hostIdx}, nil
}

func (s *scriptedSession) ParticipantKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]bool, len(s.slots))
	keys := make([]string, 0, len(s.slots))
	for _, slot := range s.slots {
		if seen[slot] {
			continue
		}
		seen[slot] = true
		keys = append(keys, slot)
	}
	return keys
}

func (s *scriptedSession) GroupSize() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slots)
}

func (s *scriptedSession) LatestNonce() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nonce
}

func (s *scriptedSession) stopFailing() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failAfterDecide = nil
}

func (s *scriptedSession) report() (advances, declines int, commits []committedNonce) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.advances, s.declines, append([]committedNonce(nil), s.commits...)
}

type preparedNonce struct {
	nonce   uint64
	hostIdx int
}

func (p preparedNonce) Nonce() uint64 { return p.nonce }
func (p preparedNonce) HostIdx() int  { return p.hostIdx }

type recordingObserver struct {
	mu          sync.Mutex
	retired     []string
	ghosts      []string
	holds       int
	trips       int
	ghostNonces []uint64
}

func (o *recordingObserver) GhostBurned(_ string, burned Burn) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ghosts = append(o.ghosts, burned.Reason)
	o.ghostNonces = append(o.ghostNonces, burned.Nonce)
}

func (o *recordingObserver) NonceHeld(string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.holds++
}

func (o *recordingObserver) BurnBudgetExhausted(string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.trips++
}

func (o *recordingObserver) EscrowRetired(escrowID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retired = append(o.retired, escrowID)
}

func (o *recordingObserver) burns() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.ghosts...)
}

func (o *recordingObserver) burnedNonces() []uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint64(nil), o.ghostNonces...)
}

func (o *recordingObserver) counts() (holds, trips int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.holds, o.trips
}

type testClock struct {
	mu    sync.Mutex
	now   time.Time
	armed chan time.Duration
	fires chan time.Time
}

func newTestClock() *testClock {
	return &testClock{now: baseTime, armed: make(chan time.Duration, 16), fires: make(chan time.Time, 1)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

func (c *testClock) newTimer(delay time.Duration) (<-chan time.Time, func()) {
	select {
	case c.armed <- delay:
	default:
	}
	return c.fires, func() {}
}

func (c *testClock) awaitArmed(t *testing.T) time.Duration {
	t.Helper()
	select {
	case delay := <-c.armed:
		return delay
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for an armed timer")
		return 0
	}
}

func (c *testClock) fireTimer() { c.fires <- c.Now() }

type fakeSnapshots struct {
	mu       sync.Mutex
	snapshot chain.PhaseSnapshot
	calls    int
}

func (f *fakeSnapshots) Snapshot() chain.PhaseSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.snapshot
}

func (f *fakeSnapshots) set(snapshot chain.PhaseSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot = snapshot
}

func (f *fakeSnapshots) fetches() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

type harnessConfig struct {
	slots           []string
	pocRequired     func(string) bool
	throttled       func(string) bool
	ejected         func(string) bool
	stateBlocked    func(string) bool
	afterDecide     func(HostBinding)
	failWith        error
	failAfterDecide error
	swallowCommit   bool
	refused         []string
	gate            chan struct{}
	submitBuffer    int
	holdStart       bool
}

type harness struct {
	dispatcher *dispatcher
	session    *scriptedSession
	clock      *testClock
	observer   *recordingObserver
	snapshots  *fakeSnapshots
	limiter    *fakeLimiter
	exhausted  *atomic.Pointer[string]
}

func newHarness(t *testing.T, cfg harnessConfig) *harness {
	t.Helper()
	if len(cfg.slots) == 0 {
		cfg.slots = []string{hostA, hostB}
	}
	session := &scriptedSession{
		slots:           cfg.slots,
		failWith:        cfg.failWith,
		failAfterDecide: cfg.failAfterDecide,
		swallowCommit:   cfg.swallowCommit,
		gate:            cfg.gate,
		afterDecide:     cfg.afterDecide,
	}
	clock := newTestClock()
	observer := &recordingObserver{}
	snapshots := &fakeSnapshots{}
	limiter := newFakeLimiter()
	for _, participant := range cfg.refused {
		limiter.refuse(participant)
	}

	var exhausted atomic.Pointer[string]

	predicates := func(snapshot chain.PhaseSnapshot) availability {
		return availability{
			pocRequired: func(participant string) bool {
				return snapshot.RequestsBlocked || (cfg.pocRequired != nil && cfg.pocRequired(participant))
			},
			throttled: func(participant string) bool {
				return cfg.throttled != nil && cfg.throttled(participant)
			},
			ejected: func(participant string) bool {
				return cfg.ejected != nil && cfg.ejected(participant)
			},
			stateBlocked: func(participant string) bool {
				return cfg.stateBlocked != nil && cfg.stateBlocked(participant)
			},
		}
	}

	dispatcher := newDispatcher(dispatcherDeps{
		escrowID:     escrowA,
		session:      session,
		snapshots:    snapshots,
		predicates:   predicates,
		acquireSlot:  func(participant string) bool { return limiter.Acquire(participant, modelA) },
		releaseSlot:  func(participant string) { limiter.Release(participant, modelA) },
		observer:     observer,
		now:          clock.Now,
		matchWait:    matchWaitWindow,
		newTimer:     clock.newTimer,
		submitBuffer: cfg.submitBuffer,
		onExhausted:  func(escrowID, reason string) { exhausted.Store(&escrowID) },
	})
	t.Cleanup(dispatcher.stop)
	if !cfg.holdStart {
		dispatcher.start()
	}
	return &harness{
		dispatcher: dispatcher,
		session:    session,
		clock:      clock,
		observer:   observer,
		exhausted:  &exhausted,
		snapshots:  snapshots,
		limiter:    limiter,
	}
}

// wantSlots asserts the limiter's books: every admission that did not reach a caller was given back.
func (h *harness) wantSlots(t *testing.T, held, admitted int) {
	t.Helper()
	gotHeld, gotAdmitted := h.limiter.slots()
	if gotHeld != held || gotAdmitted != admitted {
		t.Fatalf("slots held/admitted = %d/%d, want %d/%d", gotHeld, gotAdmitted, held, admitted)
	}
}

func (h *harness) submit(t *testing.T, enqueued time.Time, excluded ...string) *waiter {
	t.Helper()
	queued := newWaiter(RequestProfile{Model: modelA, Exclude: excluded, Params: "payload"}, enqueued)
	if outcome := h.dispatcher.submitWaiter(queued); outcome != submitAccepted {
		t.Fatalf("submitWaiter on a running dispatcher = %v, want submitAccepted", outcome)
	}
	return queued
}

func awaitReply(t *testing.T, queued *waiter) pickResult {
	t.Helper()
	select {
	case result := <-queued.replyCh:
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a dispatcher reply")
		return pickResult{}
	}
}

func wantAssignment(t *testing.T, result pickResult, host string, nonce uint64) {
	t.Helper()
	if result.err != nil {
		t.Fatalf("reply err = %v, want an assignment", result.err)
	}
	if result.assignment.Escrow != escrowA {
		t.Fatalf("assignment escrow = %q, want %q", result.assignment.Escrow, escrowA)
	}
	if result.assignment.Host != host {
		t.Fatalf("assignment host = %q, want %q", result.assignment.Host, host)
	}
	if result.assignment.Nonce == nil || result.assignment.Nonce.Nonce() != nonce {
		t.Fatalf("assignment nonce = %v, want %d", result.assignment.Nonce, nonce)
	}
}

func wantNoReply(t *testing.T, queued *waiter) {
	t.Helper()
	select {
	case result := <-queued.replyCh:
		t.Fatalf("unexpected reply %+v", result)
	default:
	}
}

func TestDispatcherServesFirstUsableNonce(t *testing.T) {
	test := newHarness(t, harnessConfig{})

	queued := test.submit(t, test.clock.Now())

	wantAssignment(t, awaitReply(t, queued), hostB, 1)
	test.dispatcher.stop()

	advances, declines, commits := test.session.report()
	if advances != 1 || declines != 0 || len(commits) != 1 {
		t.Fatalf("advances=%d declines=%d commits=%d, want 1/0/1", advances, declines, len(commits))
	}
	if commits[0].ghost || commits[0].params != "payload" {
		t.Fatalf("commit = %+v, want a real dispatch carrying the request params", commits[0])
	}
	if burns := test.observer.burns(); len(burns) != 0 {
		t.Fatalf("ghost burns = %v, want none", burns)
	}
}

func TestDispatcherBurnsUntilACompatibleHostIsBound(t *testing.T) {
	test := newHarness(t, harnessConfig{})
	stale := test.clock.Now().Add(-2 * matchWaitWindow)

	queued := test.submit(t, stale, hostB)

	wantAssignment(t, awaitReply(t, queued), hostA, 2)
	test.dispatcher.stop()

	_, _, commits := test.session.report()
	if len(commits) != 2 || !commits[0].ghost || commits[1].ghost {
		t.Fatalf("commits = %+v, want one ghost then one real dispatch", commits)
	}
	if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostExclude.reason() {
		t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostExclude.reason())
	}
}

func TestDispatcherConservesNonceAcrossWaiters(t *testing.T) {
	test := newHarness(t, harnessConfig{})
	now := test.clock.Now()

	excluding := test.submit(t, now, hostB)
	compatible := test.submit(t, now)

	wantAssignment(t, awaitReply(t, compatible), hostB, 1)
	wantAssignment(t, awaitReply(t, excluding), hostA, 2)
	test.dispatcher.stop()

	if burns := test.observer.burns(); len(burns) != 0 {
		t.Fatalf("ghost burns = %v, want none: the nonce was usable by a queued request", burns)
	}
}

// A host no participant can reach is a dead end, and answering it costs no nonce at all.
func TestDispatcherSweepsAnUnreachableWaiterWithoutAdvancing(t *testing.T) {
	test := newHarness(t, harnessConfig{ejected: func(string) bool { return true }})

	queued := test.submit(t, test.clock.Now())

	if result := awaitReply(t, queued); !errors.Is(result.err, ErrNoAvailableHost) {
		t.Fatalf("err = %v, want ErrNoAvailableHost", result.err)
	}
	test.dispatcher.stop()

	advances, _, commits := test.session.report()
	if advances != 0 || len(commits) != 0 {
		t.Fatalf("advances=%d commits=%d, want the nonce untouched", advances, len(commits))
	}
	if test.session.LatestNonce() != 0 {
		t.Fatalf("nonce = %d, want 0", test.session.LatestNonce())
	}
}

// A host the request itself excluded is not a dead end. Sweeping the waiter out here fails the one
// request the exclusion rescue exists for, which is why that rescue was unreachable from the day it
// was written: the sweep judged the waiter one statement earlier, on the same ladder.
func TestAWaiterEveryHostExcludedIsServedRatherThanFailed(t *testing.T) {
	test := newHarness(t, harnessConfig{})

	queued := test.submit(t, test.clock.Now().Add(-2*matchWaitWindow), hostA, hostB)

	result := awaitReply(t, queued)
	if result.err != nil {
		t.Fatalf("err = %v, want the nonce spent on an attempt rather than on nobody", result.err)
	}
	if result.assignment.Nonce == nil {
		t.Fatal("the waiter was answered without a nonce to dispatch")
	}
	test.dispatcher.stop()
}

func TestDispatcherHoldsNonceForCoArrivingWaiter(t *testing.T) {
	t.Run("a compatible co-arrival is served on the held nonce", func(t *testing.T) {
		test := newHarness(t, harnessConfig{})
		now := test.clock.Now()

		holding := test.submit(t, now, hostB)
		if delay := test.clock.awaitArmed(t); delay != matchWaitWindow {
			t.Fatalf("hold delay = %v, want %v", delay, matchWaitWindow)
		}
		advances, declines, commits := test.session.report()
		if advances != 1 || declines != 1 || len(commits) != 0 {
			t.Fatalf("advances=%d declines=%d commits=%d, want the held nonce uncommitted", advances, declines, len(commits))
		}

		coArriving := test.submit(t, now.Add(10*time.Millisecond))

		wantAssignment(t, awaitReply(t, coArriving), hostB, 1)
		wantAssignment(t, awaitReply(t, holding), hostA, 2)
		if burns := test.observer.burns(); len(burns) != 0 {
			t.Fatalf("ghost burns = %v, want none: the held nonce went to the co-arrival", burns)
		}
		if holds, _ := test.observer.counts(); holds != 1 {
			t.Fatalf("recorded holds = %d, want 1", holds)
		}
	})

	t.Run("an expired hold burns the nonce", func(t *testing.T) {
		test := newHarness(t, harnessConfig{})

		holding := test.submit(t, test.clock.Now(), hostB)
		test.clock.awaitArmed(t)
		test.clock.advance(matchWaitWindow)
		test.clock.fireTimer()

		wantAssignment(t, awaitReply(t, holding), hostA, 2)
		test.dispatcher.stop()

		if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostExclude.reason() {
			t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostExclude.reason())
		}
	})
}

func TestDispatcherFreezesAvailabilityWithinADrain(t *testing.T) {
	var calls int
	var mu sync.Mutex
	flipping := func(string) bool {
		mu.Lock()
		defer mu.Unlock()
		calls++
		return calls%2 == 1
	}
	test := newHarness(t, harnessConfig{throttled: flipping})

	queued := test.submit(t, test.clock.Now().Add(-2*matchWaitWindow))

	result := awaitReply(t, queued)
	if result.err != nil {
		t.Fatalf("err = %v, want an assignment: a flipping predicate must not starve the waiter", result.err)
	}
	test.dispatcher.stop()

	if burns := test.observer.burns(); len(burns) != 0 {
		t.Fatalf("ghost burns = %v, want none: availability was frozen for the drain", burns)
	}
	if _, trips := test.observer.counts(); trips != 0 {
		t.Fatalf("burn budget trips = %d, want 0", trips)
	}
}

func TestDispatcherReclassifiesALostAssignmentAsAGhost(t *testing.T) {
	t.Run("a waiter abandoned after the commit", func(t *testing.T) {
		var abandonOnce sync.Once
		var lost *waiter
		test := newHarness(t, harnessConfig{afterDecide: func(HostBinding) {
			abandonOnce.Do(func() { lost.abandoned.Store(true) })
		}})

		lost = newWaiter(RequestProfile{Model: modelA}, test.clock.Now())
		if outcome := test.dispatcher.submitWaiter(lost); outcome != submitAccepted {
			t.Fatalf("submitWaiter on a running dispatcher = %v, want submitAccepted", outcome)
		}

		next := test.submit(t, test.clock.Now())
		wantAssignment(t, awaitReply(t, next), hostA, 2)
		wantNoReply(t, lost)

		if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostAbandoned.reason() {
			t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostAbandoned.reason())
		}
		test.wantSlots(t, 1, 2)
	})

	t.Run("a waiter whose reply buffer is already full", func(t *testing.T) {
		test := newHarness(t, harnessConfig{})

		lost := newWaiter(RequestProfile{Model: modelA}, test.clock.Now())
		lost.replyCh <- pickResult{err: errors.New("caller already gave up")}
		if outcome := test.dispatcher.submitWaiter(lost); outcome != submitAccepted {
			t.Fatalf("submitWaiter on a running dispatcher = %v, want submitAccepted", outcome)
		}

		next := test.submit(t, test.clock.Now())
		wantAssignment(t, awaitReply(t, next), hostA, 2)

		if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostAbandoned.reason() {
			t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostAbandoned.reason())
		}
		test.wantSlots(t, 1, 2)
	})
}

// Between the admission and the dispatch that gives the slot back sit two paths that never reach a
// caller; each has to hand the slot back itself.
func TestDispatcherReleasesAnAdmissionThatNeverReachesACaller(t *testing.T) {
	t.Run("the session fails after admitting the request", func(t *testing.T) {
		sessionErr := errors.New("commit rejected")
		test := newHarness(t, harnessConfig{failAfterDecide: sessionErr})

		queued := test.submit(t, test.clock.Now())

		if result := awaitReply(t, queued); !errors.Is(result.err, sessionErr) {
			t.Fatalf("err = %v, want the session error", result.err)
		}
		test.wantSlots(t, 0, 1)
	})

	t.Run("the session commits no nonce", func(t *testing.T) {
		test := newHarness(t, harnessConfig{swallowCommit: true})

		queued := test.submit(t, test.clock.Now())

		if result := awaitReply(t, queued); result.err == nil {
			t.Fatalf("result = %+v, want the missing-nonce error", result)
		}
		test.wantSlots(t, 0, 1)
	})
}

// A window that fills between the peek and the commit must cost one ghost, not one per turn: the
// refusal is what tells the sweep to answer the rest of the queue.
func TestDispatcherGhostsOnceWhenAdmissionRefusesEveryHost(t *testing.T) {
	test := newHarness(t, harnessConfig{slots: []string{hostA}, refused: []string{hostA}, holdStart: true})
	first := test.submit(t, test.clock.Now())
	second := test.submit(t, test.clock.Now())

	test.dispatcher.start()

	for _, queued := range []*waiter{first, second} {
		if result := awaitReply(t, queued); !errors.Is(result.err, ErrHostsBusy) {
			t.Fatalf("err = %v, want ErrHostsBusy", result.err)
		}
	}
	test.dispatcher.stop()

	_, _, commits := test.session.report()
	if len(commits) != 1 || !commits[0].ghost {
		t.Fatalf("commits = %+v, want exactly one ghost for the nonce bound when admission refused", commits)
	}
	if burns := test.observer.burns(); len(burns) != 1 || burns[0] != ghostThrottled.reason() {
		t.Fatalf("ghost burns = %v, want exactly one %q", burns, ghostThrottled.reason())
	}
	// The burn leaves an inference record on chain that stays started forever, so the observer has to
	// name the nonce: nothing downstream can tell it apart from work still running without the number.
	if nonces := test.observer.burnedNonces(); len(nonces) != 1 || nonces[0] != commits[0].nonce {
		t.Fatalf("burned nonces = %v, want the committed nonce %d", nonces, commits[0].nonce)
	}
	test.wantSlots(t, 0, 0)
}

func TestDispatcherNeverHoldsForAnAbandonedQueue(t *testing.T) {
	test := newHarness(t, harnessConfig{})
	now := test.clock.Now()

	abandoned := make([]*waiter, 2)
	for index := range abandoned {
		queued := newWaiter(RequestProfile{Model: modelA, Exclude: []string{hostB}}, now)
		queued.abandoned.Store(true)
		abandoned[index] = queued
		if outcome := test.dispatcher.submitWaiter(queued); outcome != submitAccepted {
			t.Fatalf("submitWaiter on a running dispatcher = %v, want submitAccepted", outcome)
		}
	}

	live := test.submit(t, now)
	wantAssignment(t, awaitReply(t, live), hostB, 1)
	test.dispatcher.stop()

	for _, queued := range abandoned {
		wantNoReply(t, queued)
	}
	select {
	case delay := <-test.clock.armed:
		t.Fatalf("armed a %v hold for an abandoned queue", delay)
	default:
	}
	if holds, _ := test.observer.counts(); holds != 0 {
		t.Fatalf("recorded holds = %d, want 0", holds)
	}
}

func TestDispatcherFailsBufferedWaitersOnStop(t *testing.T) {
	t.Run("waiters submitted before the loop ever ran", func(t *testing.T) {
		test := newHarness(t, harnessConfig{holdStart: true})

		first := newWaiter(RequestProfile{Model: modelA}, test.clock.Now())
		second := newWaiter(RequestProfile{Model: modelA}, test.clock.Now())
		for _, queued := range []*waiter{first, second} {
			if outcome := test.dispatcher.submitWaiter(queued); outcome != submitAccepted {
				t.Fatalf("submitWaiter before stop = %v, want submitAccepted", outcome)
			}
		}

		test.dispatcher.stop()

		for _, queued := range []*waiter{first, second} {
			if result := awaitReply(t, queued); !errors.Is(result.err, ErrDispatcherStopped) {
				t.Fatalf("buffered waiter err = %v, want ErrDispatcherStopped", result.err)
			}
		}
		if outcome := test.dispatcher.submitWaiter(newWaiter(RequestProfile{Model: modelA}, test.clock.Now())); outcome != submitStopped {
			t.Fatalf("submitWaiter after stop = %v, want submitStopped", outcome)
		}
	})

	t.Run("waiters buffered while the actor is busy", func(t *testing.T) {
		gate := make(chan struct{})
		test := newHarness(t, harnessConfig{gate: gate})

		queued := make([]*waiter, 4)
		for index := range queued {
			queued[index] = test.submit(t, test.clock.Now())
		}

		close(gate)
		test.dispatcher.stop()

		for index, waiting := range queued {
			select {
			case <-waiting.replyCh:
			default:
				t.Fatalf("waiter %d was stranded with no reply", index)
			}
		}
	})
}

func TestDispatcherRefetchesTheSnapshotEachDrain(t *testing.T) {
	test := newHarness(t, harnessConfig{})

	served := test.submit(t, test.clock.Now())
	wantAssignment(t, awaitReply(t, served), hostB, 1)

	test.snapshots.set(chain.PhaseSnapshot{RequestsBlocked: true})
	blocked := test.submit(t, test.clock.Now())

	if result := awaitReply(t, blocked); !errors.Is(result.err, ErrHostsBusy) {
		t.Fatalf("err = %v, want ErrHostsBusy from the refetched snapshot", result.err)
	}
	test.dispatcher.stop()

	if fetches := test.snapshots.fetches(); fetches < 2 {
		t.Fatalf("snapshot fetches = %d, want one per drain", fetches)
	}
}

// A host that has answered "I do not implement tools" will answer the same way tomorrow, so telling
// the caller to retry wastes their time and a nonce. Every other exhaustion is genuinely transient.
func TestDispatcherReportsASpentDepositForReplacement(t *testing.T) {
	t.Run("an exhausted deposit is reported", func(t *testing.T) {
		test := newHarness(t, harnessConfig{failWith: types.ErrInsufficientBalance})

		awaitReply(t, test.submit(t, test.clock.Now()))

		reported := test.exhausted.Load()
		if reported == nil || *reported != escrowA {
			t.Fatalf("reported escrow = %v, want %q", reported, escrowA)
		}
	})

	t.Run("an unrelated session failure is not", func(t *testing.T) {
		test := newHarness(t, harnessConfig{failWith: errors.New("session broken")})

		awaitReply(t, test.submit(t, test.clock.Now()))

		if reported := test.exhausted.Load(); reported != nil {
			t.Fatalf("reported escrow = %q, want no report", *reported)
		}
	})
}

func TestDispatcherFailsWaitersWhenTheSessionErrors(t *testing.T) {
	sessionErr := errors.New("session broken")
	test := newHarness(t, harnessConfig{failWith: sessionErr})

	queued := test.submit(t, test.clock.Now())

	result := awaitReply(t, queued)
	if !errors.Is(result.err, sessionErr) {
		t.Fatalf("err = %v, want the session error", result.err)
	}
}

// Every drain that ends without holding a nonce must leave the queue empty: a waiter left behind has no
// timer of its own, so its only wake-up would be a later arrival -- which is served ahead of it.
func TestDispatcherLeavesNoWaiterWithoutAWakeUp(t *testing.T) {
	t.Run("an advance that failed on the chosen waiter answers the rest too", func(t *testing.T) {
		test := newHarness(t, harnessConfig{failAfterDecide: types.ErrInsufficientBalance, holdStart: true})
		first := test.submit(t, test.clock.Now())
		second := test.submit(t, test.clock.Now())

		test.dispatcher.start()

		for index, queued := range []*waiter{first, second} {
			if result := awaitReply(t, queued); !errors.Is(result.err, types.ErrInsufficientBalance) {
				t.Fatalf("waiter %d err = %v, want the depleted escrow's error", index, result.err)
			}
		}
		test.wantSlots(t, 0, 1)
	})

	t.Run("a tripped burn budget answers the queue it stopped serving", func(t *testing.T) {
		test := newHarness(t, harnessConfig{swallowCommit: true})

		queued := test.submit(t, test.clock.Now().Add(-2*matchWaitWindow), hostB)

		if result := awaitReply(t, queued); !errors.Is(result.err, ErrNoAvailableHost) {
			t.Fatalf("err = %v, want ErrNoAvailableHost", result.err)
		}
		if _, trips := test.observer.counts(); trips != 1 {
			t.Fatalf("burn budget trips = %d, want 1", trips)
		}
	})
}

// The actor wakes on the next submit, so a queue it left behind is served after the request that woke it.
func TestDispatcherServesNoLaterArrivalAheadOfAFailedQueue(t *testing.T) {
	test := newHarness(t, harnessConfig{failAfterDecide: types.ErrInsufficientBalance, holdStart: true})
	queued := []*waiter{
		test.submit(t, test.clock.Now()),
		test.submit(t, test.clock.Now()),
		test.submit(t, test.clock.Now()),
	}

	test.dispatcher.start()

	for index, waiting := range queued {
		if result := awaitReply(t, waiting); result.err == nil {
			t.Fatalf("waiter %d was served, want the failed advance reported to it", index)
		}
	}
	test.session.stopFailing()
	if result := awaitReply(t, test.submit(t, test.clock.Now())); result.err != nil {
		t.Fatalf("later arrival err = %v, want an assignment once the escrow advances again", result.err)
	}
}

func TestDispatcherLifecycleIsIdempotent(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	test := newHarness(t, harnessConfig{})
	test.dispatcher.start()

	holding := test.submit(t, test.clock.Now(), hostB)
	test.clock.awaitArmed(t)

	test.dispatcher.stop()
	test.dispatcher.stop()

	if result := awaitReply(t, holding); !errors.Is(result.err, ErrDispatcherStopped) {
		t.Fatalf("queued waiter err = %v, want ErrDispatcherStopped", result.err)
	}
	test.dispatcher.start()
	if outcome := test.dispatcher.submitWaiter(newWaiter(RequestProfile{Model: modelA}, test.clock.Now())); outcome != submitStopped {
		t.Fatalf("submitWaiter after stop = %v, want submitStopped", outcome)
	}
}

func (s *scriptedSession) Balance() uint64    { return s.balance }
func (s *scriptedSession) TokenPrice() uint64 { return 1 }
