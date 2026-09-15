package engine

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
)

// stubPicker's hold parks every pick its queue cannot answer, which is the scheduler's own queue holding a
// waiter nothing will wake; parked reports each pick that got that far.
type stubPicker struct {
	mu       sync.Mutex
	queue    []scheduler.Assignment
	blocked  [][2]string
	diverged [][2]string
	replays  int
	profiles []scheduler.RequestProfile
	hold     chan struct{}
	parked   chan struct{}
}

func (p *stubPicker) Pick(ctx context.Context, profile scheduler.RequestProfile) (scheduler.Assignment, error) {
	p.mu.Lock()
	p.profiles = append(p.profiles, profile)
	if len(p.queue) > 0 {
		next := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()
		return next, nil
	}
	hold, parked := p.hold, p.parked
	p.mu.Unlock()

	if hold == nil {
		return scheduler.Assignment{}, scheduler.ErrNoAvailableHost
	}
	if parked != nil {
		parked <- struct{}{}
	}
	select {
	case <-hold:
		return p.next()
	case <-ctx.Done():
		return scheduler.Assignment{}, ctx.Err()
	}
}

// next answers a released pick with whatever was queued while it was parked, which is how a test puts a
// replacement host in flight at a chosen moment rather than at whatever moment the race asked for one.
func (p *stubPicker) next() (scheduler.Assignment, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.queue) == 0 {
		return scheduler.Assignment{}, scheduler.ErrNoAvailableHost
	}
	next := p.queue[0]
	p.queue = p.queue[1:]
	return next, nil
}

// Mirrors the scheduler's one-replay-per-participant-per-escrow policy.
func (p *stubPicker) HostDiverged(escrowID, participant string, _ time.Time) bool {
	p.mu.Lock()
	seen := 0
	for _, entry := range p.diverged {
		if entry == [2]string{escrowID, participant} {
			seen++
		}
	}
	p.diverged = append(p.diverged, [2]string{escrowID, participant})
	p.mu.Unlock()
	if seen == 0 {
		p.replays++
		return true
	}
	p.blockHost(escrowID, participant)
	return false
}

func (p *stubPicker) HostServed(string, string, time.Time) {}

func (p *stubPicker) blockHost(escrowID, participant string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = append(p.blocked, [2]string{escrowID, participant})
}

// hostScript is one nonce's scripted host. arrive/release let several attempts be held at the same point so
// their first content chunks contend for the crown; streaming/resume hold one mid-stream, after its first
// chunk and before the rest. flush makes the script assert the transport's own contract on the writer it
// was handed and flush per chunk, which is how a client learns a token arrived before the response is over.
type hostScript struct {
	arrive    chan<- uint64
	release   <-chan struct{}
	streaming chan<- uint64
	resume    <-chan struct{}
	receipt   bool
	chunks    []string
	flush     bool
	hold      bool
	confirmed bool
	finished  bool
	err       error
}

type scriptedTarget struct {
	mu      sync.Mutex
	scripts map[uint64]*hostScript
	hosts   int
	labels  map[int]string
	rewound []int
}

func (t *scriptedTarget) script(nonce uint64) *hostScript {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.scripts[nonce]
}

func (t *scriptedTarget) Send(ctx context.Context, nonce scheduler.Prepared, stream io.Writer, onReceipt func()) (Response, error) {
	script := t.script(nonce.Nonce())
	if script == nil {
		return nil, errors.New("unscripted nonce")
	}
	if script.arrive != nil {
		script.arrive <- nonce.Nonce()
	}
	if script.release != nil {
		select {
		case <-script.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if script.receipt {
		onReceipt()
	}
	for index, chunk := range script.chunks {
		if _, err := stream.Write([]byte(chunk)); err != nil {
			return nil, err
		}
		if script.flush {
			if flusher, ok := stream.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if index > 0 || script.streaming == nil {
			continue
		}
		script.streaming <- nonce.Nonce()
		select {
		case <-script.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if script.hold {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return fakeResponse{confirmed: script.confirmed}, script.err
}

func (t *scriptedTarget) HostCount() int { return t.hosts }

func (t *scriptedTarget) HostLabel(hostIdx int) string {
	if label, ok := t.labels[hostIdx]; ok {
		return label
	}
	return fmt.Sprintf("host-%d", hostIdx)
}

func (t *scriptedTarget) RewindHostCatchUp(hostIdx int, _ string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rewound = append(t.rewound, hostIdx)
	return true
}

func (t *scriptedTarget) NonceFinished(nonce uint64) bool {
	script := t.script(nonce)
	return script != nil && script.finished
}

func (t *scriptedTarget) Target(string) (DispatchTarget, bool) { return t, true }

// simTargets is the boundary main wires: a resolved target arrives holding its escrow, and the engine
// gives that hold back only once the race's vote is posted.
type simTargets struct {
	*scriptedTarget
	panicking atomic.Bool
	holds     atomic.Int64
	releases  atomic.Int64
}

func (t *simTargets) Target(string) (DispatchTarget, func(), bool) {
	t.holds.Add(1)
	release := func() { t.releases.Add(1) }
	if t.panicking.Load() {
		return unusableTarget{DispatchTarget: t.scriptedTarget}, release, true
	}
	return t.scriptedTarget, release, true
}

func (t *simTargets) held() (holds, releases int64) { return t.holds.Load(), t.releases.Load() }

// unusableTarget fails the race after the hold has been taken, on the goroutine that called Run.
type unusableTarget struct{ DispatchTarget }

func (unusableTarget) HostCount() int { panic("dispatch target is unusable") }

// markerClassifier reads its verdict out of the chunk itself so a script's bytes and their
// classification cannot drift apart.
type markerClassifier struct{}

func (markerClassifier) Classify(chunk []byte) chunkFacts {
	switch {
	case bytes.Contains(chunk, []byte("content")):
		return chunkFacts{Content: true, ContentSource: "delta.content"}
	case bytes.Contains(chunk, []byte("too-long")):
		return chunkFacts{
			ErrorSource:       "error.BadRequestError",
			ErrorType:         "BadRequestError",
			ErrorMessage:      vllmContextTotalMessage,
			CapabilityRefused: true,
		}
	case bytes.Contains(chunk, []byte("malformed-request")):
		return chunkFacts{
			Error:        true,
			ErrorSource:  "error.BadRequestError",
			ErrorCode:    "400",
			ErrorType:    "BadRequestError",
			ErrorMessage: malformedToolCallMessage,
			ErrorPayload: malformedToolCallRejection,
		}
	}
	return chunkFacts{}
}

func (markerClassifier) Flush() chunkFacts { return chunkFacts{} }
func (markerClassifier) Release()          {}

// claimWatch reports the content chunk an attempt is about to hand to its sink, which is the last point a
// test can observe before that write claims the crown.
type claimWatch struct {
	streamClassifier
	claiming chan<- struct{}
}

func (w claimWatch) Classify(chunk []byte) chunkFacts {
	facts := w.streamClassifier.Classify(chunk)
	if facts.Content {
		select {
		case w.claiming <- struct{}{}:
		default:
		}
	}
	return facts
}

type stubPerf struct {
	mu           sync.Mutex
	acquired     []string
	released     []string
	ejected      map[string]bool
	degraded     map[string]bool
	toolCalls    []string
	versionCalls []string
	limits       []contextLimitCall
	observed     map[string]time.Duration

	contextLimitRecorded chan struct{}
}

func (p *stubPerf) FirstContentP75(participant, _ string) (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	observed, known := p.observed[participant]
	return observed, known
}

func (p *stubPerf) Acquire(participant string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acquired = append(p.acquired, participant)
}

func (p *stubPerf) Release(participant string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = append(p.released, participant)
}

func (p *stubPerf) Ejected(participant, _ string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ejected[participant]
}

func (p *stubPerf) Degraded(participant, _ string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.degraded[participant]
}

func (p *stubPerf) RecordContextLimit(participant, model string, maxTokens uint64) {
	p.mu.Lock()
	p.limits = append(p.limits, contextLimitCall{participant: participant, maxTokens: maxTokens})
	recorded := p.contextLimitRecorded
	p.mu.Unlock()
	if recorded != nil {
		recorded <- struct{}{}
	}
}

func (p *stubPerf) RecordVersionUnsupported(participant string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.versionCalls = append(p.versionCalls, participant)
}

func (p *stubPerf) RecordToolUnsupported(participant, model string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.toolCalls = append(p.toolCalls, participant)
}

func (p *stubPerf) counts() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.acquired), len(p.released)
}

type stubSnapshots struct{ snapshot chain.PhaseSnapshot }

func (s stubSnapshots) Snapshot() chain.PhaseSnapshot { return s.snapshot }

type stubCrown struct {
	mu       sync.Mutex
	denied   map[string]bool
	observed map[string]bool
}

func (g *stubCrown) Denied(participant, _ string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.denied[participant]
}

func (g *stubCrown) Observe(participant, _ string, contentless bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.observed == nil {
		g.observed = map[string]bool{}
	}
	g.observed[participant] = contentless
}

// slotLedger stands in for the host windows the scheduler drew each attempt's slot from. Releases are
// reported on a channel so a test waits for the attempt goroutines instead of polling behind them.
type slotLedger struct{ releases chan string }

func newSlotLedger() *slotLedger { return &slotLedger{releases: make(chan string, 64)} }

func (l *slotLedger) Release(participant, model string) { l.releases <- participant }

type recordingSink struct {
	mu      sync.Mutex
	writes  [][]byte
	flushes int
}

func (s *recordingSink) Write(chunk []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes = append(s.writes, bytes.Clone(chunk))
	return len(chunk), nil
}

func (s *recordingSink) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.flushes++
}

func (s *recordingSink) flushed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes
}

func (s *recordingSink) forwarded() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Join(s.writes, nil)
}

type raceFixture struct {
	deps     raceDeps
	request  raceRequest
	picker   *stubPicker
	target   *scriptedTarget
	perf     *stubPerf
	crown    *stubCrown
	limiter  *slotLedger
	clock    *virtualTime
	client   *recordingSink
	reported chan RaceOutcome
}

func newRaceFixture(policy EscalationPolicy, hosts int) *raceFixture {
	fixture := &raceFixture{
		picker:   &stubPicker{},
		target:   &scriptedTarget{scripts: map[uint64]*hostScript{}, hosts: hosts, labels: map[int]string{}},
		perf:     &stubPerf{ejected: map[string]bool{}, degraded: map[string]bool{}},
		crown:    &stubCrown{denied: map[string]bool{}},
		limiter:  newSlotLedger(),
		clock:    newVirtualTime(),
		client:   &recordingSink{},
		reported: make(chan RaceOutcome, 4),
	}
	fixture.deps = raceDeps{
		Picker:    fixture.picker,
		Targets:   fixture.target,
		Limiter:   fixture.limiter,
		Perf:      fixture.perf,
		Crown:     fixture.crown,
		Snapshots: stubSnapshots{},
		Policy:    policy,
		Modes:     config.Modes{},
		Classify:  func(string) streamClassifier { return markerClassifier{} },
		Now:       fixture.clock.Now,
		Timer:     func() raceTimer { return fixture.clock },
		Report:    func(outcome RaceOutcome) { fixture.reported <- outcome },
	}
	fixture.request = raceRequest{
		Request: Request{
			RequestID:   "request-1",
			Model:       testModel,
			InputTokens: 1_000,
		},
		Client: fixture.client,
	}
	return fixture
}

func (f *raceFixture) host(nonce uint64, hostIdx int, participant string, script *hostScript) {
	f.target.mu.Lock()
	f.target.scripts[nonce] = script
	f.target.labels[hostIdx] = "label-" + participant
	f.target.mu.Unlock()
	f.picker.mu.Lock()
	f.picker.queue = append(f.picker.queue, scheduler.Assignment{
		Escrow: "escrow-1",
		Host:   participant,
		Nonce:  fakePrepared{nonce: nonce, hostIdx: hostIdx},
	})
	f.picker.mu.Unlock()
}

func (f *raceFixture) run(ctx context.Context) (RaceOutcome, error) {
	return runRace(ctx, f.deps, f.request)
}

// racePolicy escalates the moment an attempt is dispatched without a receipt, which is the only way a
// second crownable attempt joins a race that has no suspicious host.
func racePolicy(maxAttempts int) EscalationPolicy {
	return EscalationPolicy{
		ReceiptTimeout:        0,
		FirstTokenFloor:       time.Hour,
		InterChunkStall:       time.Hour,
		LoserGrace:            10 * time.Minute,
		MaxAttemptsPerRequest: maxAttempts,
	}
}

func settledPolicy() EscalationPolicy {
	return EscalationPolicy{
		ReceiptTimeout:        time.Hour,
		FirstTokenFloor:       time.Hour,
		InterChunkStall:       time.Hour,
		LoserGrace:            10 * time.Minute,
		MaxAttemptsPerRequest: 1,
	}
}

// waitForValue bounds a wait on the race, so a regression that blocks it fails the test rather than
// hanging the package.
func waitForValue[T any](t *testing.T, values <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never happened", what)
		var zero T
		return zero
	}
}

func contentChunk(nonce uint64) string {
	return fmt.Sprintf("data: {\"content\":\"%d\"}\n\n", nonce)
}

func TestRunRaceCrownsExactlyOneWinnerUnderConcurrentContent(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 4)
	arrive := make(chan uint64, 2)
	release := make(chan struct{})
	for index, nonce := range []uint64{10, 11} {
		fixture.host(nonce, index, fmt.Sprintf("host-%d", index), &hostScript{
			arrive:    arrive,
			release:   release,
			receipt:   true,
			chunks:    []string{"data: role\n\n", contentChunk(nonce)},
			confirmed: true,
			finished:  true,
		})
	}

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()

	<-arrive
	<-arrive
	close(release)

	reported := <-fixture.reported
	<-outcomes

	if len(reported.Attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(reported.Attempts))
	}
	if reported.WinnerNonce != 10 && reported.WinnerNonce != 11 {
		t.Fatalf("winner nonce = %d, want one of the two attempts", reported.WinnerNonce)
	}
	won := 0
	for _, attempt := range reported.Attempts {
		if attempt.Terminal == TerminalWon {
			won++
		}
	}
	if won != 1 {
		t.Fatalf("attempts with TerminalWon = %d, want 1", won)
	}
	if !reported.Succeeded {
		t.Fatal("Succeeded = false, want true")
	}
	forwarded := string(fixture.client.forwarded())
	if !bytes.Contains([]byte(forwarded), []byte(contentChunk(reported.WinnerNonce))) {
		t.Fatalf("client stream %q missing the winner's chunk", forwarded)
	}
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
}

func TestRunRaceWithholdsLosingAttemptBytes(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 4)
	arrive := make(chan uint64, 2)
	release := make(chan struct{})
	for index, nonce := range []uint64{20, 21} {
		fixture.host(nonce, index, fmt.Sprintf("host-%d", index), &hostScript{
			arrive:    arrive,
			release:   release,
			receipt:   true,
			chunks:    []string{"data: role\n\n", contentChunk(nonce)},
			confirmed: true,
			finished:  true,
		})
	}

	go func() {
		if _, err := fixture.run(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	<-arrive
	<-arrive
	close(release)
	reported := <-fixture.reported

	loser := uint64(20)
	if reported.WinnerNonce == 20 {
		loser = 21
	}
	forwarded := fixture.client.forwarded()
	if bytes.Contains(forwarded, []byte(contentChunk(loser))) {
		t.Fatalf("client stream %q carries the losing attempt's bytes", forwarded)
	}
}

func TestRunRaceReportsOneOutcomeWhenEveryAttemptFails(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.host(30, 0, "host-0", &hostScript{receipt: true, err: errors.New("dial failed")})

	outcome, err := fixture.run(context.Background())
	if err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if outcome.Succeeded || reported.Succeeded {
		t.Fatal("Succeeded = true, want false")
	}
	if len(reported.Attempts) != 1 || reported.Attempts[0].Terminal != TerminalDialFailure {
		t.Fatalf("attempts = %+v, want one dial failure", reported.Attempts)
	}
	if reported.WinnerNonce != 0 {
		t.Fatalf("winner nonce = %d, want 0", reported.WinnerNonce)
	}
	acquired, released := fixture.perf.counts()
	if acquired != 1 || released != 1 {
		t.Fatalf("perf bracket = %d/%d, want 1/1", acquired, released)
	}
}

func TestRunRaceReportsOneOutcomeAfterHandingOffPendingLosers(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 4)
	arrive := make(chan uint64, 2)
	winnerRelease := make(chan struct{})
	loserRelease := make(chan struct{})

	fixture.host(40, 0, "host-0", &hostScript{
		arrive:  arrive,
		release: loserRelease,
		receipt: true,
		chunks:  []string{"data: role\n\n"},
	})
	fixture.host(41, 1, "host-1", &hostScript{
		arrive:    arrive,
		release:   winnerRelease,
		receipt:   true,
		chunks:    []string{contentChunk(41)},
		confirmed: true,
		finished:  true,
	})

	returned := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		returned <- outcome
	}()

	<-arrive
	<-arrive
	close(winnerRelease)
	served := <-returned
	if served.WinnerNonce != 41 {
		t.Fatalf("winner nonce = %d, want 41", served.WinnerNonce)
	}

	close(loserRelease)
	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if len(reported.Attempts) != 2 {
		t.Fatalf("reported attempts = %d, want both", len(reported.Attempts))
	}
	acquired, released := fixture.perf.counts()
	if acquired != 2 || released != 2 {
		t.Fatalf("perf bracket = %d/%d, want 2/2", acquired, released)
	}
}

func TestRunRaceReportsOneOutcomeWhenTerminatedWithEveryAttemptPending(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 4)
	fixture.deps.DrainTimeout = time.Millisecond
	fixture.clock.step = time.Second
	arrive := make(chan uint64, 2)
	for index, nonce := range []uint64{50, 51} {
		fixture.host(nonce, index, fmt.Sprintf("host-%d", index), &hostScript{
			arrive: arrive,
			hold:   true,
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if _, err := fixture.run(ctx); !errors.Is(err, context.Canceled) {
			t.Errorf("runRace error = %v, want context.Canceled", err)
		}
	}()

	<-arrive
	<-arrive
	cancel()

	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if len(reported.Attempts) != 2 {
		t.Fatalf("reported attempts = %d, want both", len(reported.Attempts))
	}
	for _, attempt := range reported.Attempts {
		if attempt.Terminal != TerminalClientCancelled {
			t.Fatalf("attempt %d terminal = %v, want client cancelled", attempt.Nonce, attempt.Terminal)
		}
	}
	acquired, released := fixture.perf.counts()
	if acquired != 2 || released != 2 {
		t.Fatalf("perf bracket = %d/%d, want 2/2", acquired, released)
	}
}

func TestRunRaceReportsOneOutcomeOnHardTimeoutWithPendingAttempts(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.host(60, 0, "host-0", &hostScript{receipt: true, hold: true})
	// A clock that jumps past every backstop on each reading drives the hard timeout without a sleep.
	fixture.clock.step = 30 * time.Minute

	outcome, err := fixture.run(context.Background())
	if err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if len(reported.Attempts) != 1 || len(outcome.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(reported.Attempts))
	}
	// The host receipted and then held until the backstop, with a client still waiting and nobody
	// crowned. Reported as cancelled it would be exempt from the sample ladder like a loser the race
	// outran, and a host that hangs for the whole timeout would cost its own health nothing.
	if reported.Attempts[0].Terminal != TerminalHardTimeout {
		t.Fatalf("terminal = %v, want hard_timeout", reported.Attempts[0].Terminal)
	}
}

func TestRunRaceReturnsPickErrorAndReportsNoAttempts(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)

	_, err := fixture.run(context.Background())
	if !errors.Is(err, scheduler.ErrNoAvailableHost) {
		t.Fatalf("error = %v, want ErrNoAvailableHost", err)
	}
	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if len(reported.Attempts) != 0 {
		t.Fatalf("reported attempts = %+v, want none", reported.Attempts)
	}
}

// A client that leaves while its primary waits in the scheduler is owed no primary, so the pick is given up and nothing is committed for a vote to settle.
func TestRunRaceGivesUpThePrimaryPickWhenTheClientLeavesWhileItWaits(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	hold := make(chan struct{})
	openHold := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(openHold)
	fixture.picker.hold, fixture.picker.parked = hold, make(chan struct{}, 1)
	clientCtx, leave := context.WithCancel(context.Background())
	t.Cleanup(leave)

	returned := make(chan error, 1)
	go func() {
		_, err := fixture.run(clientCtx)
		returned <- err
	}()
	waitForValue(t, fixture.picker.parked, "the primary's pick waiting in the scheduler")
	leave()
	err := waitForValue(t, returned, "the race giving up the primary's pick once the client left")
	openHold()

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runRace error = %v, want the client's own cancellation, neither a scheduler timeout nor a capacity refusal", err)
	}
	reported := <-fixture.reported
	if len(fixture.reported) != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+len(fixture.reported))
	}
	if len(reported.Attempts) != 0 {
		t.Fatalf("reported attempts = %+v, want none", reported.Attempts)
	}
	if plan := reported.TimeoutPlan(); len(plan) != 0 {
		t.Fatalf("timeout plan = %+v, want no vote", plan)
	}
	if reported.Lifecycle != (Lifecycle{ClientGone: true}) {
		t.Fatalf("lifecycle = %+v, want only the client's departure, with neither a missing escrow nor an exhausted balance", reported.Lifecycle)
	}
}

// expiredPicker answers every pick the way the scheduler does once the pick's own deadline has passed.
type expiredPicker struct{ *stubPicker }

func (expiredPicker) Pick(context.Context, scheduler.RequestProfile) (scheduler.Assignment, error) {
	return scheduler.Assignment{}, context.DeadlineExceeded
}

// A pick that runs out its own deadline is the scheduler's failure, so a client still waiting is not recorded as gone.
func TestRunRaceDoesNotReadAPrimaryPickDeadlineAsADeparture(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.deps.Picker = expiredPicker{stubPicker: fixture.picker}

	_, err := fixture.run(context.Background())

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runRace error = %v, want the pick's own deadline", err)
	}
	reported := <-fixture.reported
	if len(reported.Attempts) != 0 {
		t.Fatalf("reported attempts = %+v, want none", reported.Attempts)
	}
	if reported.Lifecycle.ClientGone {
		t.Fatal("a pick that ran out its own deadline was recorded as the client leaving")
	}
}

// The escrow can rotate out between the pick that committed a nonce on it and the registry lookup that
// finds its session, which is why the handle is fetched per race. That assignment is already paid for.
func TestRunRaceGivesBackTheSlotAndVotesForANonceItCannotDispatch(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.host(300, 0, "host-0", &hostScript{receipt: true})
	fixture.deps.Targets = missingTarget{}

	_, err := fixture.run(context.Background())

	if !errors.Is(err, errNoDispatchTarget) {
		t.Fatalf("error = %v, want errNoDispatchTarget", err)
	}
	if released := waitForValue(t, fixture.limiter.releases, "the release of the slot the scheduler took for this assignment"); released != "host-0" {
		t.Fatalf("released slot = %q, want host-0", released)
	}
	reported := <-fixture.reported
	plan := reported.TimeoutPlan()
	if len(plan) != 1 || !plan[0].Post || plan[0].Nonce != 300 {
		t.Fatalf("timeout plan = %+v, want one posted vote for nonce 300", plan)
	}
	// A verifier recomputes the refusal deadline from the committed record, so a zero here posts the
	// vote in year 1: too early to be collectable, and the nonce is stranded for good.
	poster := &stubPoster{vote: "refused"}
	settleEvents(reported, poster)
	if len(poster.posts) != 1 || !poster.posts[0].sentAt.Equal(testEpoch) {
		t.Fatalf("posted votes = %+v, want one carrying the race's start %v", poster.posts, testEpoch)
	}
	if _, exemption := reported.Sample(reported.Attempts[0]); exemption != ExemptNeverDispatched {
		t.Fatalf("sample exemption = %v, want ExemptNeverDispatched", exemption)
	}
}

// missingTarget is the registry after the escrow the pick chose has already rotated out.
type missingTarget struct{}

func (missingTarget) Target(string) (DispatchTarget, bool) { return nil, false }

// Every attempt the race starts arrives holding a slot the scheduler took for it, so the race owes one
// release per attempt however each ended.
func TestRunRaceReleasesOneHostSlotPerAttempt(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	fixture.host(70, 0, "host-0", &hostScript{receipt: true})
	fixture.host(71, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(71)},
		confirmed: true,
		finished:  true,
	})

	if _, err := fixture.run(context.Background()); err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	reported := <-fixture.reported

	if len(reported.Attempts) != 2 {
		t.Fatalf("attempts = %d, want 2", len(reported.Attempts))
	}
	for range reported.Attempts {
		waitForValue(t, fixture.limiter.releases, "a host-slot release for every attempt")
	}
	if extra := len(fixture.limiter.releases); extra != 0 {
		t.Fatalf("host-slot releases = %d more than attempts, want one each", extra)
	}
}

func TestRunRaceRewindsADivergentHostBeforeBlockingIt(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.host(80, 0, "host-0", &hostScript{receipt: true, err: ErrStateRootDivergence})

	if _, err := fixture.run(context.Background()); err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	reported := <-fixture.reported
	if !reported.Attempts[0].StateDivergent {
		t.Fatal("StateDivergent = false, want true")
	}
	fixture.picker.mu.Lock()
	blocked, diverged := fixture.picker.blocked, fixture.picker.diverged
	fixture.picker.mu.Unlock()
	if len(diverged) != 1 || diverged[0] != [2]string{"escrow-1", "host-0"} {
		t.Fatalf("diverged hosts = %v, want the divergent one", diverged)
	}
	if len(blocked) != 0 {
		t.Fatalf("blocked hosts = %v, want none on the first divergence", blocked)
	}
	fixture.target.mu.Lock()
	rewound := fixture.target.rewound
	fixture.target.mu.Unlock()
	if len(rewound) != 1 || rewound[0] != 0 {
		t.Fatalf("rewound host indexes = %v, want the divergent host's", rewound)
	}
}

// Both attempts report arrival before either can produce content, so the rival is provably live for the
// whole window in which the denied host claims the crown, whichever order the two goroutines run in.
func TestRunRaceSuppressesCrownForDeniedHostWhileARivalIsLive(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	fixture.crown.denied["host-0"] = true
	arrive := make(chan uint64, 2)
	rivalRelease := make(chan struct{})
	fixture.host(90, 0, "host-0", &hostScript{
		arrive:    arrive,
		receipt:   true,
		chunks:    []string{contentChunk(90), "data: tail\n\n"},
		confirmed: true,
		finished:  true,
	})
	fixture.host(91, 1, "host-1", &hostScript{
		arrive:    arrive,
		release:   rivalRelease,
		receipt:   true,
		chunks:    []string{contentChunk(91)},
		confirmed: true,
		finished:  true,
	})

	returned := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		returned <- outcome
	}()

	<-arrive
	<-arrive
	close(rivalRelease)
	<-returned
	reported := <-fixture.reported

	if reported.WinnerNonce != 91 || !reported.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want the rival to serve", reported.WinnerNonce, reported.Succeeded)
	}
	if forwarded := fixture.client.forwarded(); bytes.Contains(forwarded, []byte(contentChunk(90))) {
		t.Fatalf("client stream %q carries the denied host's bytes", forwarded)
	}
}

// The replacement a denied primary earns is a rival from the moment the race commits to fetching it, not
// from the moment it launches: crowning inside that window strands the replacement's nonce unspent and
// credits the denied host for the answer.
func TestRunRaceSuppressesCrownForDeniedHostWhileItsReplacementIsBeingPicked(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	fixture.crown.denied["host-0"] = true
	fixture.picker.hold = make(chan struct{})
	fixture.picker.parked = make(chan struct{}, 1)
	claiming := make(chan struct{}, 1)
	fixture.deps.Classify = func(string) streamClassifier {
		return claimWatch{streamClassifier: markerClassifier{}, claiming: claiming}
	}
	fixture.host(90, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(90), "data: tail\n\n"},
		confirmed: true,
		finished:  true,
	})

	returned := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		returned <- outcome
	}()

	<-fixture.picker.parked
	<-claiming
	fixture.host(91, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(91)},
		confirmed: true,
		finished:  true,
	})
	close(fixture.picker.hold)
	<-returned
	reported := <-fixture.reported

	if reported.WinnerNonce != 91 || !reported.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want the replacement to serve", reported.WinnerNonce, reported.Succeeded)
	}
	if forwarded := fixture.client.forwarded(); bytes.Contains(forwarded, []byte(contentChunk(90))) {
		t.Fatalf("client stream %q carries the denied host's bytes", forwarded)
	}
}

// Holding the claim must not become a way to lose a paid-for answer: once the replacement pick comes back
// empty, the denied host is the last one standing and its content is the client's response.
func TestRunRaceCrownsADeniedHostWhenItsReplacementPickFindsNoHost(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	fixture.crown.denied["host-0"] = true
	fixture.picker.hold = make(chan struct{})
	fixture.picker.parked = make(chan struct{}, 1)
	arrive := make(chan uint64, 1)
	fixture.host(90, 0, "host-0", &hostScript{
		arrive:    arrive,
		receipt:   true,
		chunks:    []string{contentChunk(90)},
		confirmed: true,
		finished:  true,
	})

	returned := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		returned <- outcome
	}()

	<-fixture.picker.parked
	<-arrive
	close(fixture.picker.hold)
	served := <-returned
	reported := <-fixture.reported

	if served.WinnerNonce != 90 || !served.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want the denied host's answer", served.WinnerNonce, served.Succeeded)
	}
	if forwarded := string(fixture.client.forwarded()); forwarded != contentChunk(90) {
		t.Fatalf("client stream = %q, want the answer the race paid for", forwarded)
	}
	if len(reported.Attempts) != 1 {
		t.Fatalf("attempts = %d, want only the denied host's", len(reported.Attempts))
	}
}

// A denied host that is the last one standing has already been paid for: refusing its answer hands the
// client an error for a response the race committed a nonce to produce.
func TestRunRaceCrownsADeniedHostThatIsTheLastOneStanding(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.crown.denied["host-0"] = true
	fixture.host(90, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(90)},
		confirmed: true,
		finished:  true,
	})

	outcome, err := fixture.run(context.Background())
	if err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	if outcome.WinnerNonce != 90 || !outcome.Succeeded {
		t.Fatalf("outcome = winner %d succeeded %v, want 90/true", outcome.WinnerNonce, outcome.Succeeded)
	}
	reported := <-fixture.reported
	if forwarded := string(fixture.client.forwarded()); forwarded != contentChunk(90) {
		t.Fatalf("client stream = %q, want the answer the race paid for", forwarded)
	}
	if reported.DeniesCrowning(reported.Attempts[0]) {
		t.Error("a host that produced content was charged as if it had answered with nothing")
	}
}

func TestPhaseAborted(t *testing.T) {
	cases := []struct {
		name               string
		attempt            AttemptOutcome
		startedInInference bool
		generating         bool
		want               bool
	}{
		{"aborted mid stream", AttemptOutcome{Terminal: TerminalStreamTruncated}, true, true, true},
		{"started during generation", AttemptOutcome{Terminal: TerminalStreamTruncated}, false, true, false},
		{"generation over", AttemptOutcome{Terminal: TerminalStreamTruncated}, true, false, false},
		{"nonce finished", AttemptOutcome{Terminal: TerminalStreamTruncated, NonceFinished: true}, true, true, false},
		{"host answered with an error", AttemptOutcome{Terminal: TerminalErrorStream, ErrorSource: "error"}, true, true, false},
		{"never receipted", AttemptOutcome{Terminal: TerminalNoReceipt}, true, true, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := phaseAborted(testCase.attempt, testCase.startedInInference, testCase.generating)
			if got != testCase.want {
				t.Fatalf("phaseAborted = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestPoCFactsAreDistinct(t *testing.T) {
	generating := chain.PhaseSnapshot{
		RequestsBlocked: true,
		EpochPhase:      chain.EpochPhasePoCGenerate,
	}
	validating := chain.PhaseSnapshot{
		RequestsBlocked: true,
		EpochPhase:      chain.EpochPhasePoCValidate,
	}
	relaxed := config.Modes{PoCMode: config.PoCModeRelaxed}
	strict := config.Modes{PoCMode: config.PoCModeOff}

	if !pocBypassActive(validating, relaxed) || pocGenerating(validating, relaxed) {
		t.Fatal("validation must read as a bypass but not as generation")
	}
	if !pocGenerating(generating, relaxed) {
		t.Fatal("generation phase must read as generation")
	}
	if pocBypassActive(generating, strict) || pocGenerating(generating, strict) {
		t.Fatal("strict mode never serves through a PoC phase")
	}
}

func pendingAttempt(sendTime time.Time) EscalationAttempt {
	return EscalationAttempt{SendTime: sendTime}
}

func streamingAttempt(sendTime, lastChunk time.Time) EscalationAttempt {
	return EscalationAttempt{
		SendTime:     sendTime,
		ReceiptTime:  sendTime,
		FirstToken:   sendTime,
		FirstContent: sendTime,
		LastChunk:    lastChunk,
	}
}

func failedAfterEscalating(sendTime time.Time) EscalationAttempt {
	return EscalationAttempt{Done: true, Escalated: true, SendTime: sendTime, ReceiptTime: sendTime}
}

func TestNextDeadlinePrecedence(t *testing.T) {
	base := testEpoch
	const hard = streamingHardTimeout
	const hostGroupSize = 16

	cases := []struct {
		name          string
		receipt       time.Duration
		stall         time.Duration
		loserGrace    time.Duration
		budget        int
		limit         int
		drain         time.Time
		pick          time.Time
		cancelled     bool
		retryRuledOut bool
		attempts      []EscalationAttempt
		wantAt        time.Time
		wantTrigger   deadlineTrigger
	}{
		{
			name: "escalation earliest", receipt: time.Second, stall: 2 * time.Second, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(time.Second),
			wantTrigger: triggerEscalation,
		},
		{
			name: "stall earliest", receipt: 2 * time.Second, stall: time.Second, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(time.Second),
			wantTrigger: triggerStall,
		},
		{
			name: "hard timeout earliest", receipt: 25 * time.Minute, stall: 30 * time.Minute, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(hard),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "hard timeout wins a tie with escalation", receipt: hard, stall: 30 * time.Minute, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(hard),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "hard timeout wins a tie with stall", receipt: 30 * time.Minute, stall: hard, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(hard),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "escalation wins a tie with stall", receipt: 5 * time.Second, stall: 5 * time.Second, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(5 * time.Second),
			wantTrigger: triggerEscalation,
		},
		{
			name: "a running pick is already the escalation", receipt: time.Second, stall: 2 * time.Second, budget: 4,
			pick:        base.Add(schedulerPickTimeout),
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(2 * time.Second),
			wantTrigger: triggerStall,
		},
		{
			name: "a crowned winner ends escalation", receipt: time.Second, stall: 2 * time.Second, budget: 4,
			attempts: []EscalationAttempt{
				pendingAttempt(base),
				func() EscalationAttempt {
					attempt := streamingAttempt(base, base)
					attempt.Crowned = true
					return attempt
				}(),
			},
			wantAt:      base.Add(2 * time.Second),
			wantTrigger: triggerStall,
		},
		{
			name: "an exhausted budget ends escalation", receipt: time.Second, stall: 2 * time.Second, budget: 2,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(2 * time.Second),
			wantTrigger: triggerStall,
		},
		{
			name: "a failed attempt frees its place in the budget", receipt: time.Second, stall: 2 * time.Second, budget: 2,
			attempts:    []EscalationAttempt{failedAfterEscalating(base), pendingAttempt(base)},
			wantAt:      base.Add(time.Second),
			wantTrigger: triggerEscalation,
		},
		{
			name: "the host group bounds the attempts a failure freed", receipt: time.Second, stall: 2 * time.Second, budget: 2, limit: 2,
			attempts:    []EscalationAttempt{failedAfterEscalating(base), pendingAttempt(base)},
			wantAt:      base.Add(hard),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "a refusal that rules out a retry ends escalation", receipt: time.Second, stall: 2 * time.Second, budget: 4,
			retryRuledOut: true,
			attempts:      []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:        base.Add(2 * time.Second),
			wantTrigger:   triggerStall,
		},
		{
			name: "an already stalled attempt is not re-armed", receipt: time.Hour, stall: time.Second, budget: 1,
			attempts: []EscalationAttempt{
				func() EscalationAttempt {
					attempt := streamingAttempt(base, base)
					attempt.Stalled = true
					return attempt
				}(),
			},
			wantAt:      base.Add(hard),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "no stall knob leaves escalation", receipt: time.Second, stall: 0, budget: 4,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(time.Second),
			wantTrigger: triggerEscalation,
		},
		{
			name:       "loser grace bounds a finished winner",
			receipt:    time.Hour,
			stall:      time.Hour,
			loserGrace: time.Minute,
			budget:     4,
			attempts: []EscalationAttempt{
				pendingAttempt(base),
				{
					Done:          true,
					Crowned:       true,
					NonceFinished: true,
					SendTime:      base,
					ReceiptTime:   base,
					Completed:     base,
				},
			},
			wantAt:      base.Add(time.Minute),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "the drain deadline bounds a departed client's race", receipt: time.Second, stall: time.Hour, budget: 4,
			drain:       base.Add(time.Minute),
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(time.Minute),
			wantTrigger: triggerHardTimeout,
		},
		{
			name: "a departed client is owed no escalation", receipt: time.Second, stall: time.Second, budget: 4,
			drain:       base.Add(time.Hour),
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantAt:      base.Add(time.Second),
			wantTrigger: triggerStall,
		},
		{
			name: "cancellation disarms everything", receipt: time.Second, stall: time.Second, budget: 4,
			cancelled:   true,
			attempts:    []EscalationAttempt{pendingAttempt(base), streamingAttempt(base, base)},
			wantTrigger: triggerNone,
		},
		{
			name: "nothing armed before dispatch", receipt: time.Second, stall: time.Second, budget: 4,
			attempts:    []EscalationAttempt{{}},
			wantTrigger: triggerNone,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			plan := deadlinePlan{
				Policy: EscalationPolicy{
					ReceiptTimeout:  testCase.receipt,
					FirstTokenFloor: time.Hour,
					InterChunkStall: testCase.stall,
					LoserGrace:      testCase.loserGrace,
				},
				Attempts:  testCase.attempts,
				Budget:    testCase.budget,
				Limit:     cmp.Or(testCase.limit, hostGroupSize),
				Drain:     testCase.drain,
				Pick:      testCase.pick,
				Cancelled: testCase.cancelled,

				RetryRuledOut: testCase.retryRuledOut,
			}
			arm := nextDeadline(base, plan)
			if arm.Trigger != testCase.wantTrigger {
				t.Fatalf("trigger = %v, want %v", arm.Trigger, testCase.wantTrigger)
			}
			if !arm.At.Equal(testCase.wantAt) {
				t.Fatalf("deadline = %v, want %v", arm.At, testCase.wantAt)
			}
			if testCase.wantTrigger != triggerEscalation && arm.Escalation.Stage != StageNone {
				t.Fatalf("escalation carried on a %v arm: %+v", arm.Trigger, arm.Escalation)
			}
			if testCase.wantTrigger == triggerEscalation && arm.Escalation.Stage != StageReceiptTimeout {
				t.Fatalf("escalation stage = %q, want the receipt timeout", arm.Escalation.Stage)
			}
		})
	}
}

// pausedCoordinator holds attempts the test wrote itself, so one wake-up can be driven in isolation
// instead of being reached through a whole race.
func pausedCoordinator(fixture *raceFixture, budget int, attempts ...*liveAttempt) *raceCoordinator {
	return pausedCoordinatorForClient(context.Background(), fixture, budget, attempts...)
}

// departedClient is the context of a client that has already hung up.
func departedClient() context.Context {
	clientCtx, leave := context.WithCancel(context.Background())
	leave()
	return clientCtx
}

func pausedCoordinatorForClient(clientCtx context.Context, fixture *raceFixture, budget int, attempts ...*liveAttempt) *raceCoordinator {
	coordinator := newCoordinator(clientCtx, fixture.deps, fixture.request)
	coordinator.target = fixture.target
	coordinator.budget = budget
	coordinator.attemptLimit = fixture.deps.Policy.AttemptLimit(fixture.target.HostCount(), false)
	coordinator.attempts = attempts
	coordinator.pending = len(attempts)
	for _, attempt := range attempts {
		coordinator.byNonce[attempt.nonce] = attempt
	}
	return coordinator
}

func stalledFixtureCoordinator(policy EscalationPolicy, attempts ...*liveAttempt) *raceCoordinator {
	return pausedCoordinator(newRaceFixture(policy, 1), 1, attempts...)
}

// A pick's answer and a rejection can be ready at once; the answer is judged after the rejection, so its
// committed nonce is stranded, not dispatched.
func TestAPickAnswerIsJudgedAgainstTheEventsAlreadyDelivered(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 2)
	refused := &liveAttempt{nonce: 100, participant: "host-0", sendTime: testEpoch, receiptTime: testEpoch, cancel: func() {}}
	coordinator := pausedCoordinator(fixture, 2, refused)
	coordinator.pickCancel = func() {}
	coordinator.events <- AttemptEvent{Kind: AttemptDone, Nonce: 100, At: testEpoch, Outcome: &AttemptOutcome{
		Nonce:        100,
		Participant:  "host-0",
		Terminal:     TerminalCapabilityRefused,
		ErrorSource:  "error.BadRequestError",
		ErrorType:    "BadRequestError",
		ErrorMessage: vllmContextTotalMessage,
	}}

	coordinator.applyPick(pickedHost{assignment: scheduler.Assignment{
		Escrow: "escrow-1",
		Host:   "host-1",
		Nonce:  fakePrepared{nonce: 101, hostIdx: 1},
	}})

	if len(coordinator.attempts) != 2 {
		t.Fatalf("attempts = %d, want the refused one and the stranded pick", len(coordinator.attempts))
	}
	if picked := coordinator.attempts[1]; !picked.done || picked.outcome.Terminal != TerminalNoReceipt {
		t.Fatalf("picked attempt done = %v outcome = %+v, want stranded without a receipt", picked.done, picked.outcome)
	}
}

// The rejection ends the search for another host, not the race: a sibling still running keeps its context.
func TestATrustedContextLengthRejectionDisarmsEscalationWithoutCancellingASibling(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 3)
	siblingCancelled := false
	refused := &liveAttempt{nonce: 100, participant: "host-0", sendTime: testEpoch, receiptTime: testEpoch, cancel: func() {}}
	sibling := &liveAttempt{
		nonce:       101,
		participant: "host-1",
		sendTime:    testEpoch,
		receiptTime: testEpoch,
		cancel:      func() { siblingCancelled = true },
	}
	coordinator := pausedCoordinator(fixture, 3, refused, sibling)

	coordinator.apply(AttemptEvent{Kind: AttemptDone, Nonce: 100, At: testEpoch, Outcome: &AttemptOutcome{
		Nonce:        100,
		Participant:  "host-0",
		Terminal:     TerminalCapabilityRefused,
		ErrorSource:  "error.BadRequestError",
		ErrorType:    "BadRequestError",
		ErrorMessage: vllmContextTotalMessage,
	}})

	if arm := nextDeadline(coordinator.deps.Now(), coordinator.plan()); arm.Trigger == triggerEscalation {
		t.Fatalf("arm = %+v, want no escalation once a trusted host rejected the prompt", arm)
	}
	if siblingCancelled {
		t.Fatal("the rejection cancelled an attempt still running")
	}
}

// A timer fire and a queued event are equally ready in the select, so every deadline must be judged
// against the events already delivered. Each case below queues the event that clears its deadline.
func TestADeadlineIsJudgedAgainstTheEventsAlreadyDelivered(t *testing.T) {
	t.Run("a delivered first token withdraws the escalation it would have confirmed", func(t *testing.T) {
		policy := settledPolicy()
		policy.FirstTokenFloor = time.Second
		fixture := newRaceFixture(policy, 2)
		fixture.host(401, 1, "host-1", &hostScript{receipt: true})
		receipted := &liveAttempt{
			nonce:       400,
			participant: "host-0",
			sendTime:    testEpoch.Add(-2 * time.Second),
			receiptTime: testEpoch.Add(-2 * time.Second),
			cancel:      func() {},
		}
		coordinator := pausedCoordinator(fixture, 2, receipted)
		arm := nextDeadline(coordinator.deps.Now(), coordinator.plan())
		if arm.Trigger != triggerEscalation || arm.Escalation.Stage != StageFirstToken {
			t.Fatalf("arm = %+v, want the first-token escalation", arm)
		}
		coordinator.events <- AttemptEvent{Kind: AttemptFirstToken, Nonce: 400, At: testEpoch}

		coordinator.expire(arm)

		if len(coordinator.attempts) != 1 {
			t.Fatalf("attempts = %d, want 1: a host that has started streaming is owed no successor", len(coordinator.attempts))
		}
		fixture.picker.mu.Lock()
		picks := len(fixture.picker.profiles)
		fixture.picker.mu.Unlock()
		if picks != 0 {
			t.Fatalf("picks = %d, want 0: an extra nonce was committed on a cleared deadline", picks)
		}
	})

	t.Run("a delivered context-length rejection withdraws the escalation it would have confirmed", func(t *testing.T) {
		policy := settledPolicy()
		policy.ReceiptTimeout = time.Second
		fixture := newRaceFixture(policy, 3)
		unreceipted := &liveAttempt{
			nonce:       430,
			participant: "host-0",
			sendTime:    testEpoch.Add(-2 * time.Second),
			cancel:      func() {},
		}
		rejecting := &liveAttempt{
			nonce:       431,
			participant: "host-1",
			sendTime:    testEpoch.Add(-2 * time.Second),
			receiptTime: testEpoch.Add(-2 * time.Second),
			cancel:      func() {},
		}
		coordinator := pausedCoordinator(fixture, 3, unreceipted, rejecting)
		arm := nextDeadline(coordinator.deps.Now(), coordinator.plan())
		if arm.Trigger != triggerEscalation || arm.Escalation.Stage != StageReceiptTimeout {
			t.Fatalf("arm = %+v, want the receipt-timeout escalation", arm)
		}
		coordinator.events <- AttemptEvent{Kind: AttemptDone, Nonce: 431, At: testEpoch, Outcome: &AttemptOutcome{
			Nonce:        431,
			Participant:  "host-1",
			Terminal:     TerminalCapabilityRefused,
			ErrorSource:  "error.BadRequestError",
			ErrorType:    "BadRequestError",
			ErrorMessage: vllmContextTotalMessage,
		}}

		coordinator.expire(arm)

		if coordinator.picking() {
			t.Fatal("a pick started after a trusted host rejected the prompt as past the model's context length")
		}
	})

	t.Run("a delivered chunk withdraws the stall it would have flagged", func(t *testing.T) {
		policy := settledPolicy()
		policy.InterChunkStall = time.Minute
		fixture := newRaceFixture(policy, 1)
		streaming := &liveAttempt{
			nonce:        410,
			participant:  "host-0",
			sendTime:     testEpoch.Add(-3 * time.Minute),
			receiptTime:  testEpoch.Add(-3 * time.Minute),
			firstToken:   testEpoch.Add(-3 * time.Minute),
			firstContent: testEpoch.Add(-2 * time.Minute),
			lastChunk:    testEpoch.Add(-2 * time.Minute),
			cancel:       func() {},
		}
		coordinator := pausedCoordinator(fixture, 1, streaming)
		arm := nextDeadline(coordinator.deps.Now(), coordinator.plan())
		if arm.Trigger != triggerStall {
			t.Fatalf("arm = %+v, want the stall", arm)
		}
		coordinator.events <- AttemptEvent{Kind: AttemptChunk, Nonce: 410, At: testEpoch}

		coordinator.expire(arm)

		if streaming.stalled {
			t.Fatal("a host whose chunk was already delivered was flagged as having gone silent")
		}
	})

	t.Run("a delivered completion releases the client instead of cancelling it", func(t *testing.T) {
		fixture := newRaceFixture(settledPolicy(), 1)
		fixture.host(420, 0, "host-0", &hostScript{finished: true})
		winner := &liveAttempt{
			nonce:       420,
			participant: "host-0",
			sendTime:    testEpoch,
			receiptTime: testEpoch,
			cancel:      func() {},
		}
		coordinator := pausedCoordinator(fixture, 1, winner)
		coordinator.winner = winner
		coordinator.events <- AttemptEvent{
			Kind:    AttemptDone,
			Nonce:   420,
			At:      testEpoch,
			Outcome: &AttemptOutcome{Nonce: 420, Terminal: TerminalLost, ContentChunks: 1},
		}

		if exit := coordinator.depart(); exit == exitClientGone {
			t.Fatal("a winner that had already completed was reported to its client as cancelled")
		}
	})
}

// An escalation's pick waits on the scheduler's queue, which can leave it unanswered for as long as that
// queue has nothing to wake it. A coordinator that waited for it inline would answer no crown claim and
// service no deadline, so the winner it is racing to would reach neither its client nor an outcome.
func TestAParkedEscalationPickBlocksNeitherTheWinnerNorTheRace(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	held, parked := make(chan struct{}), make(chan struct{}, 1)
	fixture.picker.hold, fixture.picker.parked = held, parked
	t.Cleanup(func() { close(held) })

	dispatched, streamed := make(chan uint64, 1), make(chan uint64, 1)
	release, resume := make(chan struct{}), make(chan struct{})
	fixture.host(500, 0, "host-0", &hostScript{
		arrive:    dispatched,
		release:   release,
		receipt:   true,
		streaming: streamed,
		resume:    resume,
		chunks:    []string{contentChunk(500), "data: tail\n\n"},
		confirmed: true,
		finished:  true,
	})

	returned := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		returned <- outcome
	}()

	<-dispatched
	waitForValue(t, parked, "the escalation reaching the scheduler")

	close(release)
	waitForValue(t, streamed, "the winner's first content chunk getting past the coordinator's crown answer")
	if forwarded := fixture.client.forwarded(); !bytes.Contains(forwarded, []byte(contentChunk(500))) {
		t.Fatalf("client stream %q is missing the crowned winner's bytes", forwarded)
	}
	close(resume)

	fixture.clock.waitArmed(t, schedulerPickTimeout)
	fixture.clock.advance(schedulerPickTimeout)

	if outcome := waitForValue(t, returned, "the race ending while its escalation pick was parked"); outcome.WinnerNonce != 500 || !outcome.Succeeded {
		t.Fatalf("outcome = winner %d succeeded %v, want 500/true", outcome.WinnerNonce, outcome.Succeeded)
	}
	if reported := <-fixture.reported; len(reported.Attempts) != 1 {
		t.Fatalf("reported attempts = %d, want only the one the race started", len(reported.Attempts))
	}
}

// The departure and a due escalation are separate select arms, so the pick reads the departure itself rather than trusting the select to take it first.
func TestAnEscalationDueAsTheClientLeavesStartsNoPick(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 2)
	failed := &liveAttempt{nonce: 440, participant: "host-0", sendTime: testEpoch, receiptTime: testEpoch, done: true, cancel: func() {}}
	coordinator := pausedCoordinatorForClient(departedClient(), fixture, 2, failed)
	arm := nextDeadline(coordinator.deps.Now(), coordinator.plan())
	if arm.Trigger != triggerEscalation || arm.Escalation.Stage != StageAttemptFailed {
		t.Fatalf("arm = %+v, want the failed attempt's escalation", arm)
	}

	coordinator.expire(arm)

	if coordinator.picking() {
		t.Fatal("a pick started for a client that had already left")
	}
	fixture.picker.mu.Lock()
	picks := len(fixture.picker.profiles)
	fixture.picker.mu.Unlock()
	if picks != 0 {
		t.Fatalf("picks = %d, want 0: a nonce was requested for a client that had already left", picks)
	}
}

func TestAFailedAttemptFreesItsPlaceForAnotherPick(t *testing.T) {
	policy := refusalPolicy()
	policy.ReceiptTimeout = time.Second
	fixture := newRaceFixture(policy, 3)
	sentBeforeTheReceiptTimeout := fixture.deps.Now().Add(-time.Minute)
	failed := &liveAttempt{nonce: 450, participant: "host-0", sendTime: sentBeforeTheReceiptTimeout, receiptTime: sentBeforeTheReceiptTimeout, escalated: true, done: true, cancel: func() {}}
	waiting := &liveAttempt{nonce: 451, participant: "host-1", sendTime: sentBeforeTheReceiptTimeout, cancel: func() {}}
	coordinator := pausedCoordinator(fixture, 2, failed, waiting)
	arm := nextDeadline(coordinator.deps.Now(), coordinator.plan())
	if arm.Trigger != triggerEscalation || arm.Escalation.Stage != StageReceiptTimeout {
		t.Fatalf("arm = %+v, want the waiting attempt's receipt timeout", arm)
	}

	coordinator.expire(arm)
	t.Cleanup(coordinator.stopPicking)

	if !coordinator.picking() {
		t.Fatal("no pick started, although the failed attempt left a place in the budget")
	}
}

// An immediate attempt still owed when a pick answers is no different: a client that has left is owed no further nonce.
func TestAnImmediateAttemptOwedAsTheClientLeavesStartsNoPick(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 3)
	fixture.target.scripts[451] = &hostScript{receipt: true, finished: true}
	primary := &liveAttempt{nonce: 450, participant: "host-0", sendTime: testEpoch, receiptTime: testEpoch, cancel: func() {}}
	coordinator := pausedCoordinatorForClient(departedClient(), fixture, 3, primary)
	coordinator.pickCancel, coordinator.moreImmediate = func() {}, 1

	coordinator.applyPick(pickedHost{assignment: scheduler.Assignment{
		Escrow: "escrow-1",
		Host:   "host-1",
		Nonce:  fakePrepared{nonce: 451, hostIdx: 1},
	}})
	awaitTracedAttemptDone(t, coordinator.events)

	if coordinator.picking() {
		t.Fatal("an immediate attempt's pick started for a client that had already left")
	}
	fixture.picker.mu.Lock()
	picks := len(fixture.picker.profiles)
	fixture.picker.mu.Unlock()
	if picks != 0 {
		t.Fatalf("picks = %d, want 0: a nonce was requested for a client that had already left", picks)
	}
}

// The budget clamp turns on the one fact that makes a nonce scarce, which is not the same fact as the
// phase refusing new inferences: serving through that phase deliberately is what makes it spendable.
func TestBudgetClampsOnlyWhenTheGatewayIsNotServingThroughTheBlock(t *testing.T) {
	blocked := chain.PhaseSnapshot{RequestsBlocked: true, EpochPhase: chain.EpochPhasePoCGenerate}
	cases := []struct {
		name  string
		modes config.Modes
		want  int
	}{
		{name: "strict mode leaves one nonce to spend", modes: config.Modes{}, want: 1},
		{name: "relaxed mode is serving through the block", modes: config.Modes{PoCMode: config.PoCModeRelaxed}, want: 2},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newRaceFixture(racePolicy(2), 4)
			fixture.deps.Snapshots = stubSnapshots{snapshot: blocked}
			fixture.deps.Modes = testCase.modes
			fixture.host(440, 0, "host-0", &hostScript{receipt: true})
			fixture.host(441, 1, "host-1", &hostScript{receipt: true})

			if _, err := fixture.run(context.Background()); err != nil {
				t.Fatalf("runRace error = %v", err)
			}
			reported := <-fixture.reported

			if len(reported.Attempts) != testCase.want {
				t.Fatalf("attempts = %d, want %d", len(reported.Attempts), testCase.want)
			}
		})
	}
}

func TestMarkStallsFlagsOnlySilentContentAttempts(t *testing.T) {
	base := testEpoch
	policy := settledPolicy()
	policy.InterChunkStall = time.Minute

	silent := &liveAttempt{nonce: 1, firstContent: base, lastChunk: base}
	recent := &liveAttempt{nonce: 2, firstContent: base, lastChunk: base.Add(time.Hour)}
	contentless := &liveAttempt{nonce: 3, lastChunk: base}
	finished := &liveAttempt{nonce: 4, firstContent: base, lastChunk: base, done: true}

	coordinator := stalledFixtureCoordinator(policy, silent, recent, contentless, finished)
	coordinator.deps.Now = func() time.Time { return base.Add(2 * time.Minute) }
	coordinator.markStalls()

	for _, expectation := range []struct {
		attempt *liveAttempt
		want    bool
	}{{silent, true}, {recent, false}, {contentless, false}, {finished, false}} {
		if expectation.attempt.stalled != expectation.want {
			t.Fatalf("attempt %d stalled = %v, want %v", expectation.attempt.nonce, expectation.attempt.stalled, expectation.want)
		}
	}
}

func TestOutcomeRewritesTerminalsTheCoordinatorAloneKnows(t *testing.T) {
	base := testEpoch
	stalledOut := &liveAttempt{
		nonce:   1,
		stalled: true,
		done:    true,
		outcome: &AttemptOutcome{Nonce: 1, Terminal: TerminalClientCancelled, ContentChunks: 3, SendTime: base},
	}
	cancelledEmpty := &liveAttempt{
		nonce:   2,
		stalled: true,
		done:    true,
		outcome: &AttemptOutcome{Nonce: 2, Terminal: TerminalClientCancelled, SendTime: base},
	}
	crowned := &liveAttempt{
		nonce:         3,
		done:          true,
		nonceFinished: true,
		outcome:       &AttemptOutcome{Nonce: 3, Terminal: TerminalLost, ContentChunks: 2, SendTime: base},
	}

	coordinator := stalledFixtureCoordinator(settledPolicy(), stalledOut, cancelledEmpty, crowned)
	coordinator.winner = crowned
	outcome := coordinator.outcome()

	wanted := map[uint64]Terminal{1: TerminalStalled, 2: TerminalClientCancelled, 3: TerminalWon}
	for _, attempt := range outcome.Attempts {
		if attempt.Terminal != wanted[attempt.Nonce] {
			t.Fatalf("attempt %d terminal = %v, want %v", attempt.Nonce, attempt.Terminal, wanted[attempt.Nonce])
		}
	}
	if outcome.WinnerNonce != 3 || !outcome.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want 3/true", outcome.WinnerNonce, outcome.Succeeded)
	}
	if !outcome.Attempts[2].NonceFinished {
		t.Fatal("NonceFinished = false on the winner, want true")
	}
}

// refusalPolicy escalates on nothing but a failed attempt or a suspicious primary: no receipt or
// first-token clock starts an attempt.
func refusalPolicy() EscalationPolicy {
	policy := settledPolicy()
	policy.MaxAttemptsPerRequest = 2
	return policy
}

// The race takes a trusted host's rejection as every host's, so a second attempt would only commit a nonce
// and leave its host a timeout vote.
func TestRunRaceStopsEscalatingOnceATrustedHostRejectsTheContextLength(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 3)
	fixture.host(100, 0, "host-0", &hostScript{receipt: true, chunks: []string{"data: too-long\n\n"}})
	fixture.host(101, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(101)},
		confirmed: true,
		finished:  true,
	})

	outcome, err := fixture.run(context.Background())
	if err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	<-fixture.reported
	if len(outcome.Attempts) != 1 || outcome.Attempts[0].Terminal != TerminalCapabilityRefused {
		t.Fatalf("attempts = %+v, want only the refused one", outcome.Attempts)
	}

	fixture.perf.mu.Lock()
	limits := fixture.perf.limits
	fixture.perf.mu.Unlock()
	if len(limits) != 1 || limits[0] != (contextLimitCall{participant: "host-0", maxTokens: 40960}) {
		t.Fatalf("recorded context limits = %+v, want one for host-0", limits)
	}

	fixture.picker.mu.Lock()
	picks := len(fixture.picker.profiles)
	fixture.picker.mu.Unlock()
	if picks != 1 {
		t.Fatalf("picks = %d, want 1: no other host takes a prompt past the model's context length", picks)
	}
}

// The rejection ends only the search for another host: an attempt already running is left to answer.
func TestRunRaceLetsARunningAttemptFinishAfterATrustedContextLengthRejection(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	release := make(chan struct{})
	openRelease := sync.OnceFunc(func() { close(release) })
	t.Cleanup(openRelease)
	fixture.perf.contextLimitRecorded = make(chan struct{}, 1)
	fixture.host(100, 0, "host-0", &hostScript{
		release:   release,
		receipt:   true,
		chunks:    []string{contentChunk(100)},
		confirmed: true,
		finished:  true,
	})
	fixture.host(101, 1, "host-1", &hostScript{receipt: true, chunks: []string{"data: too-long\n\n"}})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	waitForValue(t, fixture.perf.contextLimitRecorded, "the escalated host's rejection reaching the race")
	openRelease()

	outcome := waitForValue(t, outcomes, "the race ending")
	waitForValue(t, fixture.reported, "the race's report")
	if outcome.WinnerNonce != 100 || !outcome.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want 100/true: the rejection cut short the attempt still running",
			outcome.WinnerNonce, outcome.Succeeded)
	}
}

// A suspicious host's word is not taken on trust, so the rival its race is already fetching still runs.
func TestRunRaceKeepsTheRivalOfASuspiciousHostThatRejectsTheContextLength(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 2)
	fixture.crown.denied["host-0"] = true
	hold := make(chan struct{})
	openHold := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(openHold)
	fixture.picker.hold, fixture.picker.parked = hold, make(chan struct{}, 1)
	fixture.perf.contextLimitRecorded = make(chan struct{}, 1)
	fixture.host(100, 0, "host-0", &hostScript{receipt: true, chunks: []string{"data: too-long\n\n"}})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	waitForValue(t, fixture.picker.parked, "the rival's pick reaching the scheduler")
	waitForValue(t, fixture.perf.contextLimitRecorded, "the suspicious host's rejection reaching the race")
	fixture.host(101, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(101)},
		confirmed: true,
		finished:  true,
	})
	openHold()

	outcome := waitForValue(t, outcomes, "the race ending")
	waitForValue(t, fixture.reported, "the race's report")
	if outcome.WinnerNonce != 101 || !outcome.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want 101/true: the suspicious host's rejection stopped its rival",
			outcome.WinnerNonce, outcome.Succeeded)
	}
}

// The pick is given up rather than waited out, so the rejection commits no nonce nobody will answer.
func TestRunRaceGivesUpThePickInFlightWhenATrustedHostRejectsTheContextLength(t *testing.T) {
	fixture := newRaceFixture(racePolicy(2), 2)
	hold, release := make(chan struct{}), make(chan struct{})
	openRelease := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() { close(hold) })
	t.Cleanup(openRelease)
	fixture.picker.hold, fixture.picker.parked = hold, make(chan struct{}, 1)
	fixture.host(100, 0, "host-0", &hostScript{release: release, receipt: true, chunks: []string{"data: too-long\n\n"}})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	waitForValue(t, fixture.picker.parked, "the escalation's pick reaching the scheduler")
	openRelease()

	if outcome := waitForValue(t, outcomes, "the race ending without waiting out its pick"); len(outcome.Attempts) != 1 {
		t.Fatalf("attempts = %d, want only the refused one", len(outcome.Attempts))
	}
	waitForValue(t, fixture.reported, "the race's report")
}

// Every host receives the same body, so a trusted host's rejection of the request is every host's, and a second attempt would only commit a nonce and leave its host a timeout vote.
func TestRunRaceStopsEscalatingOnceATrustedHostRejectsTheRequest(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 3)
	fixture.host(100, 0, "host-0", &hostScript{receipt: true, chunks: []string{"data: malformed-request\n\n"}})
	fixture.host(101, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(101)},
		confirmed: true,
		finished:  true,
	})

	outcome, err := fixture.run(context.Background())
	if err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	<-fixture.reported
	if len(outcome.Attempts) != 1 || outcome.Attempts[0].Terminal != TerminalErrorStream {
		t.Fatalf("attempts = %+v, want only the rejected one", outcome.Attempts)
	}

	fixture.picker.mu.Lock()
	picks := len(fixture.picker.profiles)
	fixture.picker.mu.Unlock()
	if picks != 1 {
		t.Fatalf("picks = %d, want 1: every host would reject the same body", picks)
	}
}

// finishWatch reports each attempt the coordinator has begun to judge, which it finishes before it reads a pick's answer.
type finishWatch struct{ finished chan uint64 }

func (w finishWatch) RecordStep(step RaceStep) {
	if step.Kind == RaceStepAttemptFinished {
		w.finished <- step.Nonce
	}
}

func (finishWatch) HostDeniedCrown(string, string, int) {}

func (finishWatch) HostCrownedAgain(string, string) {}

// A suspicious host's word is not taken on trust, so its rejection of the request still lets the rival's pick through.
func TestRunRaceKeepsTheRivalOfASuspiciousHostThatRejectsTheRequest(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 2)
	fixture.crown.denied["host-0"] = true
	hold := make(chan struct{})
	openHold := sync.OnceFunc(func() { close(hold) })
	t.Cleanup(openHold)
	fixture.picker.hold, fixture.picker.parked = hold, make(chan struct{}, 1)
	watch := finishWatch{finished: make(chan uint64, 2)}
	fixture.deps.Journal = watch
	fixture.host(100, 0, "host-0", &hostScript{receipt: true, chunks: []string{"data: malformed-request\n\n"}})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := fixture.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	waitForValue(t, fixture.picker.parked, "the rival's pick reaching the scheduler")
	if finished := waitForValue(t, watch.finished, "the suspicious host's rejection reaching the race"); finished != 100 {
		t.Fatalf("finished attempt = %d, want the suspicious host's 100", finished)
	}
	fixture.host(101, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(101)},
		confirmed: true,
		finished:  true,
	})
	openHold()

	outcome := waitForValue(t, outcomes, "the race ending")
	waitForValue(t, fixture.reported, "the race's report")
	if outcome.WinnerNonce != 101 || !outcome.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want 101/true: the suspicious host's rejection stopped its rival",
			outcome.WinnerNonce, outcome.Succeeded)
	}
}

func TestRunRaceEscalatesAfterItsLastAttemptFailed(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 3)
	fixture.host(110, 0, "host-0", &hostScript{receipt: true, err: errors.New("connection reset")})
	fixture.host(111, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(111)},
		confirmed: true,
		finished:  true,
	})

	outcome, err := fixture.run(context.Background())
	if err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	<-fixture.reported
	if len(outcome.Attempts) != 2 {
		t.Fatalf("attempts = %d, want the failed one and its escalation", len(outcome.Attempts))
	}
	if outcome.WinnerNonce != 111 || !outcome.Succeeded {
		t.Fatalf("winner = %d succeeded = %v, want 111/true", outcome.WinnerNonce, outcome.Succeeded)
	}
	if outcome.Attempts[1].StartReason != StageAttemptFailed.Reason() {
		t.Fatalf("escalation start reason = %q, want %q", outcome.Attempts[1].StartReason, StageAttemptFailed.Reason())
	}
}

// Redundancy is the reason a second attempt exists, so the host already asked must be off the table
// for the escalation -- including when it failed for a reason that says nothing about its capabilities.
func TestRunRaceExcludesEveryHostItAlreadyDispatchedTo(t *testing.T) {
	fixture := newRaceFixture(refusalPolicy(), 3)
	fixture.host(120, 0, "host-0", &hostScript{receipt: true, err: errors.New("connection reset")})
	fixture.host(121, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(121)},
		confirmed: true,
		finished:  true,
	})

	if _, err := fixture.run(context.Background()); err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	<-fixture.reported

	fixture.picker.mu.Lock()
	profiles := fixture.picker.profiles
	fixture.picker.mu.Unlock()
	if len(profiles) != 2 {
		t.Fatalf("picks = %d, want the primary and its escalation", len(profiles))
	}
	if len(profiles[1].Exclude) != 1 || profiles[1].Exclude[0] != "host-0" {
		t.Fatalf("escalation exclusions = %v, want the host already dispatched to", profiles[1].Exclude)
	}
}

func TestARacePinnedToAnEscrowAsksTheSchedulerForThatOne(t *testing.T) {
	fixture := newRaceFixture(racePolicy(1), 2)
	fixture.request.Escrow = "escrow-pinned"
	fixture.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentChunk(10)},
		confirmed: true,
		finished:  true,
	})

	if _, err := fixture.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}

	fixture.picker.mu.Lock()
	defer fixture.picker.mu.Unlock()
	if len(fixture.picker.profiles) == 0 {
		t.Fatal("the race made no pick")
	}
	if got := fixture.picker.profiles[0].Escrow; got != "escrow-pinned" {
		t.Fatalf("first pick asked for escrow %q, want %q", got, "escrow-pinned")
	}
}

func TestABackstopCancelWithNoWinnerIsNotACancelledLoser(t *testing.T) {
	base := time.Now()
	hung := &liveAttempt{
		nonce:   1,
		done:    true,
		outcome: &AttemptOutcome{Nonce: 1, Terminal: TerminalClientCancelled, SendTime: base},
	}

	coordinator := stalledFixtureCoordinator(settledPolicy(), hung)
	coordinator.cancelled = true
	outcome := coordinator.outcome()

	if got := outcome.Attempts[0].Terminal; got != TerminalStalled {
		t.Fatalf("terminal = %v, want %v: nobody won, so this host answered nothing", got, TerminalStalled)
	}
}

// The same backstop fires as the losers' grace period once a winner is crowned. Those are cancelled
// losers in the proper sense and must stay exempt, or every race penalises the hosts it outran.
func TestABackstopCancelAfterAWinnerLeavesLosersExempt(t *testing.T) {
	base := time.Now()
	loser := &liveAttempt{
		nonce:   1,
		done:    true,
		outcome: &AttemptOutcome{Nonce: 1, Terminal: TerminalClientCancelled, SendTime: base},
	}
	winner := &liveAttempt{
		nonce:         2,
		done:          true,
		nonceFinished: true,
		outcome:       &AttemptOutcome{Nonce: 2, Terminal: TerminalLost, ContentChunks: 2, SendTime: base},
	}

	coordinator := stalledFixtureCoordinator(settledPolicy(), loser, winner)
	coordinator.winner = winner
	coordinator.cancelled = true
	outcome := coordinator.outcome()

	if got := outcome.Attempts[0].Terminal; got != TerminalClientCancelled {
		t.Fatalf("terminal = %v, want %v: this host lost a race, it did not fail one", got, TerminalClientCancelled)
	}
}

// The drain deadline after a client leaves is the same backstop, so a departure must not be charged to
// the hosts still finishing their nonces for it.
func TestABackstopCancelAfterTheClientLeftLeavesHostsExempt(t *testing.T) {
	base := time.Now()
	draining := &liveAttempt{
		nonce:   1,
		done:    true,
		outcome: &AttemptOutcome{Nonce: 1, Terminal: TerminalClientCancelled, SendTime: base},
	}

	coordinator := stalledFixtureCoordinator(settledPolicy(), draining)
	coordinator.handedOff = true
	coordinator.cancelled = true
	outcome := coordinator.outcome()

	if got := outcome.Attempts[0].Terminal; got != TerminalClientCancelled {
		t.Fatalf("terminal = %v, want %v: the client left, the hosts did not fail", got, TerminalClientCancelled)
	}
}

// TerminalStalled is assigned in one place, inside the outcome assembly, so a log line that reads the
// attempt goroutine's own terminal can never show it: a host that hangs for the whole backstop reads as
// an ordinary cancelled loser while the metric, the limiter and the sample ladder all call it stalled.
func TestRacedTerminalPromotesAStalledHostForEveryReader(t *testing.T) {
	t.Parallel()
	coordinator := &raceCoordinator{}
	attempt := &liveAttempt{stalled: true}
	hung := AttemptOutcome{Terminal: TerminalClientCancelled, ContentChunks: 3}

	if got := coordinator.racedTerminal(attempt, hung); got != TerminalStalled {
		t.Fatalf("racedTerminal = %v, want stalled -- the log must name what the ladders name", got)
	}

	cancelled := AttemptOutcome{Terminal: TerminalClientCancelled}
	if got := coordinator.racedTerminal(&liveAttempt{}, cancelled); got != TerminalClientCancelled {
		t.Fatalf("racedTerminal = %v, want the cancellation left alone", got)
	}
}

// An attempt whose goroutine never reported used to vanish from the outcome, taking its committed nonce
// with it: no ledger row and, since TimeoutPlan reads only the outcome, no vote for a nonce already
// spent. Production logs showed 8 of 255 nonces leaving no trace beyond the line that committed them.
func TestAnUnreportedAttemptStaysInTheOutcome(t *testing.T) {
	t.Parallel()
	base := raceStart
	silent := &liveAttempt{nonce: 557, participant: "host-silent", sendTime: base}
	answered := &liveAttempt{
		nonce:         558,
		done:          true,
		nonceFinished: true,
		outcome:       &AttemptOutcome{Nonce: 558, Terminal: TerminalLost, ContentChunks: 2, SendTime: base},
	}

	coordinator := stalledFixtureCoordinator(settledPolicy(), silent, answered)
	coordinator.winner = answered
	outcome := coordinator.outcome()

	if len(outcome.Attempts) != 2 {
		t.Fatalf("attempts = %d, want the silent one kept: its nonce was committed", len(outcome.Attempts))
	}
	var kept AttemptOutcome
	for _, attempt := range outcome.Attempts {
		if attempt.Nonce == 557 {
			kept = attempt
		}
	}
	if kept.Participant != "host-silent" || kept.Terminal != TerminalUnclassified {
		t.Fatalf("silent attempt = %+v, want it named and unclassified", kept)
	}
	if plan := outcome.TimeoutPlan(); len(plan) != 1 || plan[0].Nonce != 557 || !plan[0].Post {
		t.Fatalf("TimeoutPlan() = %+v, want a posted vote for 557", plan)
	}
}

// The race context deliberately never cancels so a departed client still leaves its nonces settling.
// A pick issued on it must carry its own deadline: once the scheduler waits for capacity instead of
// refusing, an unbounded pick hangs the request until the client disconnects.
func TestEveryPickIsBounded(t *testing.T) {
	t.Parallel()
	raceCtx := context.WithoutCancel(t.Context())

	pickCtx, cancel := context.WithTimeout(raceCtx, schedulerPickTimeout)
	defer cancel()

	if _, hasDeadline := raceCtx.Deadline(); hasDeadline {
		t.Fatal("the race context gained a deadline: a departed client would stop nonces from settling")
	}
	deadline, hasDeadline := pickCtx.Deadline()
	if !hasDeadline {
		t.Fatal("the pick context carries no deadline, so a waiting scheduler would hang it forever")
	}
	if remaining := time.Until(deadline); remaining > schedulerPickTimeout {
		t.Fatalf("pick deadline is %v away, want at most %v", remaining, schedulerPickTimeout)
	}
}

// The race outlives the client on purpose, to settle the nonce it committed. What must not outlive the
// client is the claim that the answer reached one: production crowned 64 attempts after telling their
// clients the request had failed, and every one was labelled user-visible.
func TestAWinnerCrownedAfterTheClientLeftIsNotLabelledUserVisible(t *testing.T) {
	fixture := newRaceFixture(racePolicy(1), 1)
	fixture.deps.DrainTimeout = time.Minute
	fixture.clock.step = time.Second
	arrive := make(chan uint64, 1)
	release := make(chan struct{})
	openRelease := sync.OnceFunc(func() { close(release) })
	t.Cleanup(openRelease)
	fixture.host(50, 0, "host-0", &hostScript{
		arrive:    arrive,
		release:   release,
		receipt:   true,
		chunks:    []string{roleEvent, contentEvent("answered anyway")},
		confirmed: true,
		finished:  true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan error, 1)
	go func() {
		_, err := fixture.run(ctx)
		returned <- err
	}()

	<-arrive
	cancel()
	// Released only once the race has answered the departed client, so the departure lands before the answer.
	if err := waitForValue(t, returned, "the race answering the departed client"); !errors.Is(err, context.Canceled) {
		t.Fatalf("runRace error = %v, want context.Canceled", err)
	}
	openRelease()

	reported := <-fixture.reported

	if !reported.Lifecycle.ClientGone {
		t.Fatal("the outcome does not record that the client left, so nothing downstream can know")
	}
	for _, attempt := range reported.Attempts {
		if !reported.IsWinner(attempt) {
			continue
		}
		if got := reported.Labels(attempt).Visibility; got != VisibilityWinnerClientGone {
			t.Fatalf("winner visibility = %q, want %q", got, VisibilityWinnerClientGone)
		}
	}
}

// A winner served to a client that stayed keeps the label that says so.
func TestAWinnerServedToAWaitingClientStaysUserVisible(t *testing.T) {
	fixture := newRaceFixture(settledPolicy(), 1)
	fixture.host(60, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{roleEvent, contentEvent("delivered")},
		confirmed: true,
		finished:  true,
	})

	if _, err := fixture.run(context.Background()); err != nil {
		t.Fatalf("runRace error = %v", err)
	}
	reported := <-fixture.reported

	if reported.Lifecycle.ClientGone {
		t.Fatal("the client never left, so the outcome must not say it did")
	}
	for _, attempt := range reported.Attempts {
		if !reported.IsWinner(attempt) {
			continue
		}
		if got := reported.Labels(attempt).Visibility; got != VisibilityWinner {
			t.Fatalf("winner visibility = %q, want %q", got, VisibilityWinner)
		}
	}
}
