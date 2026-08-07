package registry

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/scheduler"
	"devshard/types"
	"devshard/user"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// desertbit/timer starts its wheel from a package-level init, so it exists for the life of any
	// binary that links the chain client and is never ours to stop.
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction("github.com/desertbit/timer.timerRoutine"))
}

// settlementSource mirrors escrow.SettlementSource so a signature drift in this package fails here
// rather than in the composition root.
type settlementSource interface {
	IsBusy(escrowID string) bool
	Finalize(ctx context.Context, escrowID string) error
	BuildSettlement(ctx context.Context, escrowID string) (chain.SettlementInput, error)
}

var (
	_ settlementSource = (*Registry)(nil)
	// Assigning the registry into the scheduler's own dependency struct is the only way to assert its
	// unexported escrowSource interface is satisfied.
	_ = func() scheduler.Deps { return scheduler.Deps{Escrows: (*Registry)(nil)} }
	_ = func() scheduler.Escrow { return scheduler.Escrow{Session: nonceStream{}} }
)

type fakeSession struct {
	sealed       int
	perSlotKeys  []string
	participants []string

	phase         atomic.Int32
	nonce         atomic.Uint64
	signatures    map[uint64]map[uint32][]byte
	escrowState   types.EscrowState
	finalizeErr   error
	finalizeCalls atomic.Int64
	flushErr      error
	flushCalls    atomic.Int64
	closeErr      error
	closeCalls    atomic.Int64
	onFlush       func()
	prepare       func(user.ParamsForHost) (*user.PreparedInference, error)
}

func newFakeSession(perSlotKeys ...string) *fakeSession {
	session := &fakeSession{perSlotKeys: perSlotKeys}
	seen := map[string]bool{}
	for _, participant := range perSlotKeys {
		if seen[participant] {
			continue
		}
		seen[participant] = true
		session.participants = append(session.participants, participant)
	}
	return session
}

func (f *fakeSession) ParticipantKeys() []string        { return f.participants }
func (f *fakeSession) HostParticipantKeyList() []string { return f.perSlotKeys }
func (f *fakeSession) Nonce() uint64                    { return f.nonce.Load() }
func (f *fakeSession) Balance() uint64                  { return 1 << 40 }
func (f *fakeSession) Phase() types.SessionPhase        { return types.SessionPhase(f.phase.Load()) }

func (f *fakeSession) PrepareInferenceFn(chooser user.ParamsForHost) (*user.PreparedInference, error) {
	if f.prepare == nil {
		return nil, errors.New("fake session cannot prepare")
	}
	return f.prepare(chooser)
}

func (f *fakeSession) Signatures() map[uint64]map[uint32][]byte { return f.signatures }
func (f *fakeSession) SnapshotState() types.EscrowState         { return f.escrowState }
func (f *fakeSession) SealedInferences() int                    { return f.sealed }

func (f *fakeSession) Finalize(context.Context) error {
	f.finalizeCalls.Add(1)
	return f.finalizeErr
}

func (f *fakeSession) FlushSnapshot() error {
	f.flushCalls.Add(1)
	if f.onFlush != nil {
		f.onFlush()
	}
	return f.flushErr
}

func (f *fakeSession) Close() error {
	f.closeCalls.Add(1)
	return f.closeErr
}

func (f *fakeSession) UserSession() *user.Session { return nil }

func (f *fakeSession) setPhase(phase types.SessionPhase) { f.phase.Store(int32(phase)) }

type recordingMembership struct {
	mu       sync.Mutex
	shares   map[string]map[string]float64
	removals []string
}

func newRecordingMembership() *recordingMembership {
	return &recordingMembership{shares: map[string]map[string]float64{}}
}

func (m *recordingMembership) SetEscrowMembership(escrowID string, hostShares map[string]float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shares[escrowID] = hostShares
}

func (m *recordingMembership) RemoveEscrow(escrowID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.shares, escrowID)
	m.removals = append(m.removals, escrowID)
}

func (m *recordingMembership) sharesFor(escrowID string) map[string]float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.shares[escrowID]
}

func (m *recordingMembership) removed() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.removals...)
}

type recordingExhaustion struct {
	mu        sync.Mutex
	exhausted []string
}

func (e *recordingExhaustion) OnBalanceExhausted(escrowID, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.exhausted = append(e.exhausted, escrowID)
}

func (e *recordingExhaustion) seen() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.exhausted...)
}

// sessions hands out a pre-registered session per escrow and counts how often each factory was asked.
type sessions struct {
	byEscrow             map[string]*fakeSession
	err                  error
	calls                atomic.Int64
	refuseConcurrentOpen bool
	openInFlight         atomic.Bool
}

func newSessions(byEscrow map[string]*fakeSession) *sessions {
	return &sessions{byEscrow: byEscrow}
}

// open stands in for a SQLite session: with refuseConcurrentOpen set, a second open that overlaps the
// first fails the way a locked database file does.
func (s *sessions) open(_ context.Context, escrowID string) (EscrowSession, error) {
	s.calls.Add(1)
	if s.refuseConcurrentOpen {
		if !s.openInFlight.CompareAndSwap(false, true) {
			return nil, errors.New("database is locked (5) (SQLITE_BUSY)")
		}
		defer s.openInFlight.Store(false)
		time.Sleep(5 * time.Millisecond)
	}
	if s.err != nil {
		return nil, s.err
	}
	session, known := s.byEscrow[escrowID]
	if !known {
		return nil, fmt.Errorf("no session for escrow %s", escrowID)
	}
	return session, nil
}

func fixedClock() func() time.Time {
	moment := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return moment }
}

func TestAddPushesSharesSplitAcrossSharedParticipants(t *testing.T) {
	t.Parallel()
	// Participant A holds two slots in escrow 1 and one in escrow 2; B holds one slot in escrow 1 only.
	shared := newSessions(map[string]*fakeSession{
		"1": newFakeSession("hostA", "hostA", "hostB"),
		"2": newFakeSession("hostA"),
	})
	capacity := newRecordingMembership()
	registry := New(Deps{ServingSessions: shared.open, Membership: capacity, Now: fixedClock()})

	if err := registry.Add(context.Background(), "1", "qwen"); err != nil {
		t.Fatalf("Add(1) = %v, want nil", err)
	}
	if err := registry.Add(context.Background(), "2", "qwen"); err != nil {
		t.Fatalf("Add(2) = %v, want nil", err)
	}

	// Slot counts would be {hostA:2, hostB:1} and {hostA:1}; escrow-internal ratios would be
	// {hostA:0.667, hostB:0.333} and {hostA:1}. Both are wrong for limits.escrowWeight.
	wantFirst := map[string]float64{"hostA": 2.0 / 3.0, "hostB": 1}
	wantSecond := map[string]float64{"hostA": 1.0 / 3.0}
	if got := capacity.sharesFor("1"); !reflect.DeepEqual(got, wantFirst) {
		t.Errorf("membership for escrow 1 = %v, want %v", got, wantFirst)
	}
	if got := capacity.sharesFor("2"); !reflect.DeepEqual(got, wantSecond) {
		t.Errorf("membership for escrow 2 = %v, want %v", got, wantSecond)
	}
}

func TestRetireRemovesMembershipAndRepublishesTheRest(t *testing.T) {
	t.Parallel()
	shared := newSessions(map[string]*fakeSession{
		"1": newFakeSession("hostA", "hostA", "hostB"),
		"2": newFakeSession("hostA"),
	})
	capacity := newRecordingMembership()
	registry := New(Deps{ServingSessions: shared.open, Membership: capacity, Now: fixedClock()})
	mustAdd(t, registry, "1", "qwen")
	mustAdd(t, registry, "2", "qwen")

	if err := registry.Retire("2"); err != nil {
		t.Fatalf("Retire(2) = %v, want nil", err)
	}

	if got, want := capacity.removed(), []string{"2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("RemoveEscrow calls = %v, want %v", got, want)
	}
	wantFirst := map[string]float64{"hostA": 1, "hostB": 1}
	if got := capacity.sharesFor("1"); !reflect.DeepEqual(got, wantFirst) {
		t.Errorf("membership for escrow 1 after retire = %v, want %v", got, wantFirst)
	}
	if got := capacity.sharesFor("2"); got != nil {
		t.Errorf("membership for escrow 2 after retire = %v, want none", got)
	}
}

func TestCandidatesReturnOnlyAcceptingEscrows(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		phase types.SessionPhase
		want  int
	}{
		{name: "active escrow is a candidate", phase: types.PhaseActive, want: 1},
		{name: "finalizing escrow is not", phase: types.PhaseFinalizing, want: 0},
		{name: "settling escrow is not", phase: types.PhaseSettlement, want: 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			session := newFakeSession("hostA")
			session.setPhase(testCase.phase)
			registry := New(Deps{
				ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
				Now:             fixedClock(),
			})
			mustAdd(t, registry, "1", "qwen")

			if got := len(registry.Candidates("qwen")); got != testCase.want {
				t.Errorf("len(Candidates(qwen)) = %d, want %d", got, testCase.want)
			}
			if got := registry.Serves("qwen"); got != (testCase.want > 0) {
				t.Errorf("Serves(qwen) = %v, want %v", got, testCase.want > 0)
			}
		})
	}
}

func TestSnapshotKeepsEscrowsCandidatesDrop(t *testing.T) {
	t.Parallel()
	accepting := newFakeSession("hostA", "hostA", "hostB")
	settling := newFakeSession("hostC")
	settling.setPhase(types.PhaseSettlement)
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": accepting, "2": settling}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	mustAdd(t, registry, "2", "qwen")
	_, release, held := registry.Acquire("1")
	if !held {
		t.Fatal("Acquire(1) was refused")
	}
	t.Cleanup(release)

	want := []EscrowState{
		{ID: "1", Model: "qwen", Accepting: true, InFlight: 1, Participants: []string{"hostA", "hostB"}},
		{ID: "2", Model: "qwen", Accepting: false, InFlight: 0, Participants: []string{"hostC"}},
	}
	if got := registry.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Snapshot() = %+v, want %+v", got, want)
	}
	if got := len(registry.Candidates("qwen")); got != 1 {
		t.Fatalf("len(Candidates(qwen)) = %d, want 1", got)
	}
}

func TestSnapshotOfAnEmptyRegistryIsEmpty(t *testing.T) {
	t.Parallel()
	if got := New(Deps{Now: fixedClock()}).Snapshot(); len(got) != 0 {
		t.Fatalf("Snapshot() = %+v, want none", got)
	}
}

func TestCandidatesSkipOtherModels(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{
			"1": newFakeSession("hostA"),
			"2": newFakeSession("hostB"),
		}).open,
		Now: fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	mustAdd(t, registry, "2", "kimi")

	candidates := registry.Candidates("qwen")
	if len(candidates) != 1 || candidates[0].ID != "1" || candidates[0].Model != "qwen" {
		t.Fatalf("Candidates(qwen) = %+v, want only escrow 1 on qwen", candidates)
	}
	if got, want := registry.Models(), []string{"kimi", "qwen"}; !reflect.DeepEqual(got, want) {
		t.Errorf("Models() = %v, want %v", got, want)
	}
}

func TestCandidatesAreOrderedByEscrowID(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{
			"3": newFakeSession("hostC"),
			"1": newFakeSession("hostA"),
			"2": newFakeSession("hostB"),
		}).open,
		Now: fixedClock(),
	})
	mustAdd(t, registry, "3", "qwen")
	mustAdd(t, registry, "1", "qwen")
	mustAdd(t, registry, "2", "qwen")

	var ordered []string
	for _, candidate := range registry.Candidates("qwen") {
		ordered = append(ordered, candidate.ID)
	}
	if want := []string{"1", "2", "3"}; !reflect.DeepEqual(ordered, want) {
		t.Errorf("candidate order = %v, want %v", ordered, want)
	}
}

// An escrow can be retired between the moment Candidates named it and the moment anything asks for it,
// so the routable lookup must report it gone rather than hand back a session nothing will settle.
func TestRetiredEscrowVanishesBetweenCandidatesAndTheRoutableLookup(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	candidates := registry.Candidates("qwen")
	if len(candidates) != 1 {
		t.Fatalf("len(Candidates(qwen)) = %d, want 1", len(candidates))
	}
	if _, found := registry.RoutableSession("1"); !found {
		t.Fatal("RoutableSession(1) before retire = not found, want found")
	}

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	if _, found := registry.RoutableSession("1"); found {
		t.Error("RoutableSession(1) after retire = found, want gone")
	}
	if got := len(registry.Candidates("qwen")); got != 0 {
		t.Errorf("len(Candidates(qwen)) after retire = %d, want 0", got)
	}
	// The slice handed out before the retire keeps naming the escrow, which is exactly why the engine
	// must re-resolve the target instead of trusting the assignment.
	if candidates[0].ID != "1" {
		t.Errorf("previously returned candidate = %q, want 1", candidates[0].ID)
	}
}

func TestNonceExhaustionReachesTheRotationSink(t *testing.T) {
	t.Parallel()
	rotation := &recordingExhaustion{}
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Exhaustion:      rotation,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	registry.Exhausted("1", "test")

	if got, want := rotation.seen(), []string{"1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("OnBalanceExhausted calls = %v, want %v", got, want)
	}
}

func TestRetireDefersCloseUntilInFlightRequestsDrain(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	held, release, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	if held != EscrowSession(session) {
		t.Errorf("Acquire(1) = %v, want the escrow's own session", held)
	}
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	if got := session.closeCalls.Load(); got != 0 {
		t.Fatalf("Close calls while a request is in flight = %d, want 0", got)
	}
	if !registry.IsBusy("1") {
		t.Error("IsBusy(1) with one in-flight request = false, want true")
	}
	if _, found := registry.RoutableSession("1"); found {
		t.Error("RoutableSession(1) while draining = found; a retired escrow must not take another race")
	}
	if _, _, routable := registry.Acquire("1"); routable {
		t.Error("Acquire(1) while draining = true; draining finishes existing work, it never takes new work")
	}

	release()

	if got := session.closeCalls.Load(); got != 1 {
		t.Errorf("Close calls after the last release = %d, want 1", got)
	}
	if got := session.flushCalls.Load(); got != 1 {
		t.Errorf("FlushSnapshot calls after the last release = %d, want 1", got)
	}
	if registry.IsBusy("1") {
		t.Error("IsBusy(1) after the last release = true, want false")
	}
}

// The last release closes a drained escrow with nobody left to hand a failure to: the request that
// held it open has already been answered. An unflushed escrow replays its whole diff tail when it is
// rehydrated, so the failure is counted rather than lost.
func TestADrainedEscrowThatFailsToCloseIsCounted(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.flushErr = errors.New("disk full")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	release()

	if got := registry.DrainCloseFailures(); got != 1 {
		t.Fatalf("DrainCloseFailures() = %d, want 1", got)
	}
}

// A retired escrow's committed nonces still owe their votes and their settlement, so the two lookups
// must disagree about it: one serving both would either route to a retired escrow or strand those nonces.
func TestRoutingLosesARetiredEscrowWhileSettlementKeepsIt(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	t.Cleanup(release)

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	if _, routable := registry.RoutableSession("1"); routable {
		t.Error("RoutableSession(1) after retire = found, want gone: a retired escrow must take no new request")
	}
	held, settling := registry.SettlementSession("1")
	if !settling {
		t.Fatal("SettlementSession(1) after retire = not found: every vote the escrow still owed is dropped")
	}
	if held != EscrowSession(session) {
		t.Errorf("SettlementSession(1) = %v, want the draining escrow's own session", held)
	}
}

// Re-publishing an id an earlier entry is still draining would resolve that entry's committed nonces
// to a session that never saw them, and hold a second handle on the storage they settle through.
func TestAddRefusesAnIdAnEarlierEntryIsStillDraining(t *testing.T) {
	t.Parallel()
	draining := newFakeSession("hostA")
	replacement := newFakeSession("hostA")
	var opened atomic.Int64
	registry := New(Deps{
		ServingSessions: func(context.Context, string) (EscrowSession, error) {
			if opened.Add(1) == 1 {
				return draining, nil
			}
			return replacement, nil
		},
		Now: fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	t.Cleanup(release)
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	err := registry.Add(context.Background(), "1", "qwen")

	if !errors.Is(err, ErrDraining) {
		t.Fatalf("Add(1) while it drains = %v, want ErrDraining", err)
	}
	held, settling := registry.SettlementSession("1")
	if !settling || held != EscrowSession(draining) {
		t.Fatalf("SettlementSession(1) = %v (found = %v), want the draining entry that owns the committed nonces", held, settling)
	}
	if got := opened.Load(); got != 1 {
		t.Errorf("serving sessions opened = %d, want 1: a second handle over the storage the draining entry still holds", got)
	}
	if _, routable := registry.RoutableSession("1"); routable {
		t.Error("RoutableSession(1) = found, want gone: the refused escrow was published anyway")
	}
}

// The last release closes the draining session, so the settlement lookup outlives retirement but not the
// drain: whatever counts a request in flight must keep counting it until that request's votes are posted.
func TestSettlementLookupEndsWithTheDrain(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, release, acquired := registry.Acquire("1")
	if !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	release()

	if _, settling := registry.SettlementSession("1"); settling {
		t.Error("SettlementSession(1) after the last release = found, want gone: its storage is closed")
	}
}

func TestRetireClosesAnIdleEscrowImmediately(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	if got := session.closeCalls.Load(); got != 1 {
		t.Errorf("Close calls = %d, want 1", got)
	}
}

// The whole point of holding a retired entry in draining is that Add refuses its id until the storage
// is actually released. A close that failed released nothing, so the refusal has to outlive it --
// otherwise a second session opens over storage the first still holds, and two state machines drive one
// escrow's nonces.
func TestAddRefusesAnEscrowWhoseCloseFailed(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	session.closeErr = errors.New("storage refused to close")
	sessions := newSessions(map[string]*fakeSession{"1": session})
	registry := New(Deps{ServingSessions: sessions.open, Now: fixedClock()})
	mustAdd(t, registry, "1", "qwen")

	if err := registry.Retire("1"); err == nil {
		t.Fatal("Retire(1) = nil, want the close failure")
	}

	err := registry.Add(context.Background(), "1", "qwen")
	if !errors.Is(err, ErrDraining) {
		t.Fatalf("Add(1) after a failed close = %v, want ErrDraining", err)
	}
	if got := session.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls = %d, want the failed close not to be retried by Add", got)
	}
}

func TestAcquireRefusesARetiredEscrow(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	if _, _, acquired := registry.Acquire("1"); acquired {
		t.Error("Acquire(1) after retire = true, want false")
	}
}

// The scheduler takes Hold in the step that commits a nonce, holding a candidate published before the
// retire. Refusing it there is what makes IsBusy monotone for the settlement that follows.
func TestRetireRefusesTheNonceCommitHoldTakenBeforeIt(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	candidate := registry.Candidates("qwen")[0]

	if release, held := candidate.Hold(); !held {
		t.Fatal("Hold() before retire = false, want true")
	} else {
		release()
	}
	if err := registry.Retire("1"); err != nil {
		t.Fatalf("Retire(1) = %v, want nil", err)
	}

	if _, held := candidate.Hold(); held {
		t.Error("Hold() after retire = true, want false: a nonce was committed on a retired escrow")
	}
	if registry.IsBusy("1") {
		t.Error("IsBusy(1) = true, want false: nothing is in flight after the refused commit")
	}
}

func TestActiveUsersFeedsTheLoadScore(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")

	_, releaseFirst, acquiredFirst := registry.Acquire("1")
	_, _, acquiredSecond := registry.Acquire("1")
	if !acquiredFirst || !acquiredSecond {
		t.Fatal("Acquire(1) = false, want true")
	}

	if got := registry.Candidates("qwen")[0].ActiveUsers; got != 2 {
		t.Errorf("ActiveUsers = %d, want 2", got)
	}
	releaseFirst()
	if got := registry.Candidates("qwen")[0].ActiveUsers; got != 1 {
		t.Errorf("ActiveUsers after one release = %d, want 1", got)
	}
}

func TestReleasingTwiceCountsOnce(t *testing.T) {
	t.Parallel()
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	_, releaseFirst, _ := registry.Acquire("1")
	if _, _, acquired := registry.Acquire("1"); !acquired {
		t.Fatal("Acquire(1) = false, want true")
	}

	releaseFirst()
	releaseFirst()

	if got := registry.Candidates("qwen")[0].ActiveUsers; got != 1 {
		t.Errorf("ActiveUsers after a doubled release = %d, want 1", got)
	}
}

// A published escrow is not opened a second time: the session is a SQLite file, and the open would
// fail against the live one rather than produce a handle to discard.
func TestAddIsIdempotentAndOpensNoSecondSession(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	factory := newSessions(map[string]*fakeSession{"1": session})
	registry := New(Deps{ServingSessions: factory.open, Now: fixedClock()})
	mustAdd(t, registry, "1", "qwen")

	mustAdd(t, registry, "1", "qwen")

	if got := factory.calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	if got := session.closeCalls.Load(); got != 0 {
		t.Errorf("Close calls on the live session = %d, want 0", got)
	}
	if got := len(registry.Candidates("qwen")); got != 1 {
		t.Errorf("len(Candidates(qwen)) = %d, want 1", got)
	}
}

// Two callers publish the same escrow at once whenever an operator creates one: the create path adds
// it, and the devshard row it writes wakes the republish watcher, which adds it again. A session
// factory that refuses a concurrent open -- which is what SQLite does to a second writer -- must not
// turn that into a failure for either caller.
func TestConcurrentAddsOfOneEscrowOpenItOnce(t *testing.T) {
	t.Parallel()
	factory := newSessions(map[string]*fakeSession{"1": newFakeSession("hostA")})
	factory.refuseConcurrentOpen = true
	registry := New(Deps{ServingSessions: factory.open, Now: fixedClock()})

	var adding sync.WaitGroup
	failures := make([]error, 2)
	for caller := range failures {
		adding.Add(1)
		go func() {
			defer adding.Done()
			failures[caller] = registry.Add(context.Background(), "1", "qwen")
		}()
	}
	adding.Wait()

	for caller, err := range failures {
		if err != nil {
			t.Fatalf("caller %d: Add = %v, want both callers to succeed", caller, err)
		}
	}
	if got := factory.calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	if got := len(registry.Candidates("qwen")); got != 1 {
		t.Fatalf("len(Candidates(qwen)) = %d, want 1", got)
	}
}

func TestAddReportsAFailedSessionOpen(t *testing.T) {
	t.Parallel()
	factory := newSessions(nil)
	factory.err = errors.New("chain unreachable")
	registry := New(Deps{ServingSessions: factory.open, Now: fixedClock()})

	err := registry.Add(context.Background(), "1", "qwen")

	if err == nil || !errors.Is(err, factory.err) {
		t.Fatalf("Add = %v, want it to wrap %v", err, factory.err)
	}
	if got := len(registry.Candidates("qwen")); got != 0 {
		t.Errorf("len(Candidates(qwen)) = %d, want 0", got)
	}
}

func TestCloseReleasesLiveAndDrainingSessions(t *testing.T) {
	t.Parallel()
	live := newFakeSession("hostA")
	drained := newFakeSession("hostB")
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"1": live, "2": drained}).open,
		Now:             fixedClock(),
	})
	mustAdd(t, registry, "1", "qwen")
	mustAdd(t, registry, "2", "qwen")
	if _, _, acquired := registry.Acquire("2"); !acquired {
		t.Fatal("Acquire(2) = false, want true")
	}
	if err := registry.Retire("2"); err != nil {
		t.Fatalf("Retire(2) = %v, want nil", err)
	}

	if err := registry.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if got := live.closeCalls.Load(); got != 1 {
		t.Errorf("Close calls on the live session = %d, want 1", got)
	}
	if got := drained.closeCalls.Load(); got != 1 {
		t.Errorf("Close calls on the draining session = %d, want 1", got)
	}
	if err := registry.Add(context.Background(), "1", "qwen"); !errors.Is(err, ErrClosed) {
		t.Errorf("Add after Close = %v, want ErrClosed", err)
	}
}

func TestConcurrentPicksRotationsAndRequestsStayConsistent(t *testing.T) {
	t.Parallel()
	byEscrow := map[string]*fakeSession{}
	for index := range 4 {
		byEscrow[fmt.Sprint(index)] = newFakeSession("hostA", "hostB")
	}
	registry := New(Deps{
		ServingSessions: newSessions(byEscrow).open,
		Membership:      newRecordingMembership(),
		Now:             fixedClock(),
	})
	t.Cleanup(func() { _ = registry.Close() })

	var workers sync.WaitGroup
	for index := range 4 {
		escrowID := fmt.Sprint(index)
		workers.Add(3)
		go func() {
			defer workers.Done()
			for range 200 {
				_ = registry.Add(context.Background(), escrowID, "qwen")
				_ = registry.Retire(escrowID)
			}
		}()
		go func() {
			defer workers.Done()
			for range 200 {
				if _, release, acquired := registry.Acquire(escrowID); acquired {
					release()
				}
			}
		}()
		go func() {
			defer workers.Done()
			for range 200 {
				for _, candidate := range registry.Candidates("qwen") {
					_, _ = registry.RoutableSession(candidate.ID)
				}
				_ = registry.Serves("qwen")
			}
		}()
	}
	workers.Wait()

	for escrowID, session := range byEscrow {
		if registry.IsBusy(escrowID) {
			t.Errorf("escrow %s is still busy after every request finished", escrowID)
		}
		if got := session.closeCalls.Load(); got == 0 {
			t.Errorf("escrow %s was never closed across %d rotations", escrowID, got)
		}
	}
}

func mustAdd(t *testing.T, registry *Registry, escrowID, model string) {
	t.Helper()
	if err := registry.Add(context.Background(), escrowID, model); err != nil {
		t.Fatalf("Add(%s, %s) = %v, want nil", escrowID, model, err)
	}
}

// A real session flushes its snapshot under its own lock, and a dispatch holds that lock while it asks
// the registry for a hold -- session lock, then registry lock. Retiring under the registry lock takes
// the same two in the opposite order, and the two orders together freeze every later route and every
// settlement of an already-committed nonce, process-wide, until a restart.
func TestRetireClosesTheSessionOutsideTheRegistryLock(t *testing.T) {
	t.Parallel()
	session := newFakeSession("hostA")
	var sessionLock sync.Mutex
	flushing := make(chan struct{})
	session.onFlush = func() {
		close(flushing)
		sessionLock.Lock()
		defer sessionLock.Unlock()
	}
	registry := New(Deps{ServingSessions: newSessions(map[string]*fakeSession{"1": session}).open, Now: fixedClock()})
	mustAdd(t, registry, "1", "qwen")

	sessionLock.Lock()
	defer sessionLock.Unlock()
	retired := make(chan error, 1)
	go func() { retired <- registry.Retire("1") }()
	<-flushing

	// The dispatch side: any registry call needing the registry lock while the retire waits on the
	// session lock. It returns promptly or the two locks are held in opposite orders.
	answered := make(chan bool, 1)
	go func() { answered <- registry.IsBusy("1") }()

	select {
	case <-answered:
	case <-time.After(5 * time.Second):
		t.Fatal("the registry lock is held across the session flush: routing and settlement are wedged behind one retirement")
	}
	sessionLock.Unlock()
	if err := <-retired; err != nil {
		t.Fatalf("Retire = %v", err)
	}
	sessionLock.Lock()
}
