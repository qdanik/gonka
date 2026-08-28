package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/goleak"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/scheduler"
)

// virtualTime is the simulator's clock and the coordinator's timer in one value, so a deadline is reached
// only by a test advancing time, never by a slow host. step is what one read of the clock costs, which buys
// latency without losing control of deadlines; arms reports every deadline the coordinator sets, which is
// how a test learns an event was applied rather than merely delivered.
type virtualTime struct {
	mu       sync.Mutex
	step     time.Duration
	now      time.Time
	deadline time.Time
	armed    bool
	fired    chan time.Time
	arms     chan time.Duration
}

func newVirtualTime() *virtualTime {
	return &virtualTime{
		now:   testEpoch,
		fired: make(chan time.Time, 1),
		arms:  make(chan time.Duration, 256),
	}
}

func (v *virtualTime) Now() time.Time {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.now = v.now.Add(v.step)
	return v.now
}

func (v *virtualTime) Reset(delay time.Duration) {
	select {
	case v.arms <- delay:
	default:
	}
	v.mu.Lock()
	v.deadline, v.armed = v.now.Add(delay), true
	due := !v.deadline.After(v.now)
	v.mu.Unlock()
	v.discard()
	if due {
		v.ring()
	}
}

func (v *virtualTime) Stop() {
	v.mu.Lock()
	v.armed = false
	v.mu.Unlock()
	v.discard()
}

func (v *virtualTime) Fired() <-chan time.Time { return v.fired }

func (v *virtualTime) advance(step time.Duration) {
	v.mu.Lock()
	v.now = v.now.Add(step)
	due := v.armed && !v.deadline.After(v.now)
	v.mu.Unlock()
	if due {
		v.ring()
	}
}

// waitArmed blocks until a deadline no further out than limit is armed. Only a per-chunk deadline is
// that close, so it proves the coordinator has accounted for the chunk before time is advanced.
func (v *virtualTime) waitArmed(t *testing.T, limit time.Duration) {
	t.Helper()
	for {
		select {
		case delay := <-v.arms:
			if delay > 0 && delay <= limit {
				return
			}
		case <-time.After(10 * time.Second):
			t.Fatal("no deadline was armed within the expected range")
			return
		}
	}
}

func (v *virtualTime) ring() {
	select {
	case v.fired <- time.Time{}:
	default:
	}
}

func (v *virtualTime) discard() {
	select {
	case <-v.fired:
	default:
	}
}

// simSnapshots lets a test move the chain phase mid-attempt, which is the only way an attempt can be
// ended by a phase transition rather than by its host.
type simSnapshots struct {
	current atomic.Pointer[chain.PhaseSnapshot]
}

func newSimSnapshots(initial chain.PhaseSnapshot) *simSnapshots {
	snapshots := &simSnapshots{}
	snapshots.current.Store(&initial)
	return snapshots
}

func (s *simSnapshots) Snapshot() chain.PhaseSnapshot { return *s.current.Load() }

func (s *simSnapshots) set(next chain.PhaseSnapshot) { s.current.Store(&next) }

// simTracker records what the engine reported and, when a test sets health, answers the health questions
// from the real tracker so an escalation is driven by the detector rather than by a flag the test set.
type simTracker struct {
	*stubPerf
	health  *perf.Tracker
	mu      sync.Mutex
	samples []perf.Sample
}

func (p *simTracker) RecordSample(sample perf.Sample) {
	p.mu.Lock()
	p.samples = append(p.samples, sample)
	p.mu.Unlock()
	if p.health != nil {
		p.health.RecordSample(sample)
	}
}

func (p *simTracker) Ejected(participant, model string) bool {
	if p.health != nil {
		return p.health.Ejected(participant, model)
	}
	return p.stubPerf.Ejected(participant, model)
}

func (p *simTracker) Degraded(participant, model string) bool {
	if p.health != nil {
		return p.health.Degraded(participant, model)
	}
	return p.stubPerf.Degraded(participant, model)
}

func (p *simTracker) recorded() []perf.Sample {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]perf.Sample(nil), p.samples...)
}

type windowMove struct {
	participant string
	verdict     limits.Verdict
}

type simWindows struct {
	*slotLedger
	mu    sync.Mutex
	moves []windowMove
}

func (w *simWindows) OnResult(participant, _ string, verdict limits.Verdict) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.moves = append(w.moves, windowMove{participant: participant, verdict: verdict})
}

func (w *simWindows) recorded() []windowMove {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]windowMove(nil), w.moves...)
}

type simMetrics struct {
	races     chan RaceOutcome
	timeouts  chan TimeoutEvent
	mu        sync.Mutex
	overflows []string
}

func newSimMetrics() *simMetrics {
	return &simMetrics{
		races:    make(chan RaceOutcome, 4),
		timeouts: make(chan TimeoutEvent, 16),
	}
}

func (m *simMetrics) RecordRace(outcome RaceOutcome) { m.races <- outcome }

func (m *simMetrics) RecordTimeout(event TimeoutEvent) { m.timeouts <- event }

func (m *simMetrics) RecordClassifyOverflow(participant, _ string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.overflows = append(m.overflows, participant)
}

func (m *simMetrics) classifyOverflows() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.overflows...)
}

type simLedger struct{ rows chan RaceOutcome }

func newSimLedger() *simLedger { return &simLedger{rows: make(chan RaceOutcome, 4)} }

func (l *simLedger) RecordRequest(outcome RaceOutcome) { l.rows <- outcome }

// simDispatchParams stands in for the concrete params the caller builds. The engine must forward the
// value unread to both routing and settlement, so a shape it could not possibly interpret is the point.
type simDispatchParams struct{ prompt string }

// simPoster stands in for the chain vote. entered/release let a test hold a vote open and observe
// that shutdown waits for it.
type simPoster struct {
	entered chan uint64
	release chan struct{}
	vote    string
	err     error

	mu       sync.Mutex
	posted   []uint64
	resolved []any
}

func (p *simPoster) resolvedWith(params any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resolved = append(p.resolved, params)
}

func (p *simPoster) paramsSeen() []any {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]any(nil), p.resolved...)
}

func newSimPoster() *simPoster {
	return &simPoster{entered: make(chan uint64, 8), vote: timeoutKindExecution}
}

func (p *simPoster) SettleTimeout(_ context.Context, nonce uint64, _ time.Time) (TimeoutVote, error) {
	p.entered <- nonce
	if p.release != nil {
		<-p.release
	}
	p.mu.Lock()
	p.posted = append(p.posted, nonce)
	p.mu.Unlock()
	return TimeoutVote{Kind: p.vote}, p.err
}

func (p *simPoster) settled() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint64(nil), p.posted...)
}

// simulator's pinned is the operator's suspicious list, set before run() so the engine reads it from the
// same dependency main supplies rather than from a policy the test built itself.
type simulator struct {
	engine    *Engine
	picker    *stubPicker
	target    *simTargets
	perf      *simTracker
	windows   *simWindows
	metrics   *simMetrics
	ledger    *simLedger
	poster    *simPoster
	snapshots *simSnapshots
	clock     *virtualTime
	client    *recordingSink
	model     string
	pinned    map[string]bool
}

func engineSettings(policy EscalationPolicy, modes config.Modes) *config.Config {
	return &config.Config{
		Modes: modes,
		Stream: config.Stream{
			DrainTimeoutSeconds:         2_400,
			ClassifyMaxAttemptBytes:     1 << 20,
			ClassifyMaxParticipantBytes: 10 << 20,
			ClassifyMaxGlobalBytes:      100 << 20,
		},
		Engine: config.Engine{
			ReceiptTimeoutMS:       policy.ReceiptTimeout.Milliseconds(),
			FirstTokenFloorMS:      policy.FirstTokenFloor.Milliseconds(),
			InterChunkStallMS:      policy.InterChunkStall.Milliseconds(),
			LoserGraceMS:           policy.LoserGrace.Milliseconds(),
			MaxSpeculativeAttempts: int64(policy.MaxSpeculativeAttempts),
		},
	}
}

func newSimulator(t *testing.T, policy EscalationPolicy, hosts int, model string) *simulator {
	t.Helper()
	return newSimulatorInPhase(t, policy, hosts, model, config.Modes{}, chain.PhaseSnapshot{})
}

func newSimulatorInPhase(
	t *testing.T,
	policy EscalationPolicy,
	hosts int,
	model string,
	modes config.Modes,
	phase chain.PhaseSnapshot,
) *simulator {
	t.Helper()
	sim := &simulator{
		picker:    &stubPicker{},
		target:    &simTargets{scriptedTarget: &scriptedTarget{scripts: map[uint64]*hostScript{}, hosts: hosts, labels: map[int]string{}}},
		perf:      &simTracker{stubPerf: &stubPerf{ejected: map[string]bool{}, degraded: map[string]bool{}}},
		windows:   &simWindows{slotLedger: newSlotLedger()},
		metrics:   newSimMetrics(),
		ledger:    newSimLedger(),
		poster:    newSimPoster(),
		snapshots: newSimSnapshots(phase),
		clock:     newVirtualTime(),
		client:    &recordingSink{},
		model:     model,
		pinned:    map[string]bool{},
	}
	sim.engine = NewEngine(Deps{
		Picker:     sim.picker,
		Targets:    sim.target,
		Windows:    sim.windows,
		Perf:       sim.perf,
		Snapshots:  sim.snapshots,
		Config:     config.NewHolder(engineSettings(policy, modes)),
		Metrics:    sim.metrics,
		Ledger:     sim.ledger,
		Suspicious: func(participant string) bool { return sim.pinned[participant] },
		Timeouts: func(_ string, params any) (TimeoutPoster, bool) {
			sim.poster.resolvedWith(params)
			return sim.poster, true
		},
		Now:   sim.clock.Now,
		Timer: func() raceTimer { return sim.clock },
	})
	t.Cleanup(sim.engine.Stop)
	return sim
}

func (s *simulator) host(nonce uint64, hostIdx int, participant string, script *hostScript) {
	s.target.mu.Lock()
	s.target.scripts[nonce] = script
	s.target.labels[hostIdx] = "label-" + participant
	s.target.mu.Unlock()
	s.picker.mu.Lock()
	s.picker.queue = append(s.picker.queue, scheduler.Assignment{
		Escrow: "escrow-1",
		Host:   participant,
		Nonce:  fakePrepared{nonce: nonce, hostIdx: hostIdx},
	})
	s.picker.mu.Unlock()
}

func (s *simulator) profile() Request {
	return Request{
		RequestID:   "request-1",
		Model:       s.model,
		InputTokens: 1_000,
		Params:      simDispatchParams{prompt: "hello"},
	}
}

// parkVotes holds every vote inside the poster and returns the gate. Cleanup opens it too, because Stop
// is a barrier over the vote a race owes: a failed assertion that left one parked would hang the package
// rather than fail it.
func (s *simulator) parkVotes(t *testing.T) func() {
	t.Helper()
	s.poster.release = make(chan struct{})
	open := sync.OnceFunc(func() { close(s.poster.release) })
	t.Cleanup(open)
	return open
}

func (s *simulator) run(ctx context.Context) (RaceOutcome, error) {
	return s.engine.Run(ctx, s.profile(), s.client)
}

// reported is the race's single outcome, taken from the one point that records it.
func (s *simulator) reported(t *testing.T) RaceOutcome {
	t.Helper()
	select {
	case outcome := <-s.metrics.races:
		return outcome
	case <-time.After(10 * time.Second):
		t.Fatal("the race never reported an outcome")
		return RaceOutcome{}
	}
}

func (s *simulator) assertOneOutcome(t *testing.T) {
	t.Helper()
	if extra := len(s.metrics.races); extra != 0 {
		t.Fatalf("outcomes reported = %d, want 1", 1+extra)
	}
}

func (s *simulator) assertNoSlotLeaked(t *testing.T, attempts int) {
	t.Helper()
	for range attempts {
		select {
		case <-s.windows.releases:
		case <-time.After(10 * time.Second):
			t.Fatal("an attempt never released its host slot")
		}
	}
	acquired, released := s.perf.counts()
	if acquired != attempts || released != attempts {
		t.Fatalf("perf acquire/release = %d/%d, want %d/%d", acquired, released, attempts, attempts)
	}
}

// settleAll waits out the votes the race left owing, so what was posted can be asserted rather than
// polled for.
func (s *simulator) settleAll() { s.engine.Stop() }

func (s *simulator) timeoutEvents() []TimeoutEvent {
	events := make([]TimeoutEvent, 0, len(s.metrics.timeouts))
	for range cap(s.metrics.timeouts) {
		select {
		case event := <-s.metrics.timeouts:
			events = append(events, event)
		default:
			return events
		}
	}
	return events
}

func attemptFor(t *testing.T, outcome RaceOutcome, nonce uint64) AttemptOutcome {
	t.Helper()
	for _, attempt := range outcome.Attempts {
		if attempt.Nonce == nonce {
			return attempt
		}
	}
	t.Fatalf("outcome has no attempt for nonce %d", nonce)
	return AttemptOutcome{}
}

// speculativePolicy escalates only when an attempt has finished, so a re-pick is attributable to the
// attempt that failed rather than to a receipt clock.
func speculativePolicy(maxAttempts int) EscalationPolicy {
	policy := settledPolicy()
	policy.MaxSpeculativeAttempts = maxAttempts
	return policy
}

func contentEvent(text string) string {
	return fmt.Sprintf("data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", text)
}

const roleEvent = `data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n"

func TestSimulatorStreamsAHealthyRequestByteForByte(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.clock.step = time.Millisecond
	body := string(readFixture(t, "content_stream.sse"))
	sim.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{body[:40], body[40:]},
		confirmed: true,
		finished:  true,
	})

	outcome, err := sim.run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if !outcome.Succeeded || outcome.WinnerNonce != 10 {
		t.Fatalf("outcome = winner %d succeeded %v, want winner 10 succeeded true", outcome.WinnerNonce, outcome.Succeeded)
	}
	if got := string(sim.client.forwarded()); got != body {
		t.Fatalf("client stream differs from the host stream:\n got %q\nwant %q", got, body)
	}
	reported := sim.reported(t)
	winner := attemptFor(t, reported, 10)
	if winner.Terminal != TerminalWon || winner.ContentSource != "delta.content" {
		t.Fatalf("winner = %v/%q, want TerminalWon/delta.content", winner.Terminal, winner.ContentSource)
	}
	if moves := sim.windows.recorded(); len(moves) != 1 || moves[0].verdict != limits.Success {
		t.Fatalf("window moves = %+v, want one Success", moves)
	}
	if samples := sim.perf.recorded(); len(samples) != 1 || !samples[0].Responsive {
		t.Fatalf("samples = %+v, want one responsive sample", samples)
	}
	if overflows := sim.metrics.classifyOverflows(); len(overflows) != 0 {
		t.Errorf("classify overflows = %v, want none for a stream inside every cap", overflows)
	}
	if events := sim.timeoutEvents(); len(events) != 0 {
		t.Fatalf("timeout events = %+v, want none for a settled nonce", events)
	}
	sim.assertNoSlotLeaked(t, 1)
	sim.assertOneOutcome(t)
}

// Every hop from the transport's writer to the client must carry the flush, or the client sees
// nothing until the response is over.
func TestSimulatorFlushesEveryChunkThroughToTheClient(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentEvent("first"), contentEvent("second")},
		flush:     true,
		confirmed: true,
		finished:  true,
	})

	if _, err := sim.run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	sim.reported(t)

	if got := sim.client.flushed(); got != 2 {
		t.Fatalf("client flushes = %d, want one per streamed chunk", got)
	}
}

// The host's own refusal is the response: the client must see it as the host wrote it, not a rewrite.
func TestSimulatorForwardsTheHostsRefusalVerbatim(t *testing.T) {
	const refusal = `{"error":{"code":400,"message":"model does not exist","type":"BadRequestError"}}`
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{
		receipt:  true,
		chunks:   []string{"data: " + refusal + "\n\n"},
		finished: true,
	})

	_, err := sim.run(context.Background())

	var hostErr *HostApplicationError
	if !errors.As(err, &hostErr) {
		t.Fatalf("Run() error = %v, want a host application error", err)
	}
	if got := hostErr.Payload; got != refusal {
		t.Fatalf("payload = %s, want the host's own error event %s", got, refusal)
	}
	if hostErr.HTTPStatus() != 400 {
		t.Fatalf("status = %d, want the 400 the host reported", hostErr.HTTPStatus())
	}
	sim.reported(t)
}

func TestSimulatorContentProducerWinsOverAnEmptyStream(t *testing.T) {
	sim := newSimulator(t, racePolicy(2), 2, qwenModel)
	arrive, release := make(chan uint64, 2), make(chan struct{})
	sim.host(10, 0, "empty-host", &hostScript{
		arrive: arrive, release: release,
		receipt: true, confirmed: true, finished: true,
	})
	sim.host(11, 1, "content-host", &hostScript{
		arrive: arrive, release: release,
		receipt: true, chunks: []string{roleEvent, contentEvent("hello")},
		confirmed: true, finished: true,
	})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := sim.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	<-arrive
	<-arrive
	close(release)
	<-outcomes

	reported := sim.reported(t)
	if reported.WinnerNonce != 11 || !reported.Succeeded {
		t.Fatalf("winner = %d succeeded %v, want 11/true", reported.WinnerNonce, reported.Succeeded)
	}
	if empty := attemptFor(t, reported, 10); empty.Terminal != TerminalEmptyStream {
		t.Fatalf("empty host terminal = %v, want TerminalEmptyStream", empty.Terminal)
	}
	if got, want := string(sim.client.forwarded()), roleEvent+contentEvent("hello"); got != want {
		t.Fatalf("client stream = %q, want %q", got, want)
	}
	sim.assertOneOutcome(t)
}

func TestSimulatorErrorStreamNeitherCrownsNorReadsAsEmpty(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{
		receipt:  true,
		chunks:   []string{`data: {"error":{"code":500,"message":"backend failed","type":"server_error"}}` + "\n\n"},
		finished: true,
	})

	outcome, err := sim.run(context.Background())

	var hostErr *HostApplicationError
	if !errors.As(err, &hostErr) || hostErr.Message != "backend failed" {
		t.Fatalf("Run() error = %v, want the host's own refusal", err)
	}
	if outcome.Succeeded || outcome.WinnerNonce != 0 {
		t.Fatalf("an error event crowned a winner: %+v", outcome)
	}
	reported := sim.reported(t)
	attempt := attemptFor(t, reported, 10)
	if attempt.Terminal != TerminalErrorStream {
		t.Fatalf("terminal = %v, want TerminalErrorStream", attempt.Terminal)
	}
	if attempt.ContentChunks != 1 {
		t.Errorf("ContentChunks = %d, want 1: an error event still counts as a chunk", attempt.ContentChunks)
	}
	if reported.DeniesCrowning(attempt) {
		t.Error("an error stream was charged as if the host had answered with nothing")
	}
	if len(sim.client.forwarded()) != 0 {
		t.Errorf("client received %q from an attempt that never earned the crown", sim.client.forwarded())
	}
	if moves := sim.windows.recorded(); len(moves) != 1 || moves[0].verdict != limits.ModelOutcome {
		t.Fatalf("window moves = %+v, want one ModelOutcome", moves)
	}
}

func TestSimulatorHoldsPreContentChunksUntilTheCrownIsWon(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{roleEvent, roleEvent, contentEvent("hi")},
		confirmed: true,
		finished:  true,
	})

	if _, err := sim.run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	want := roleEvent + roleEvent + contentEvent("hi")
	if got := string(sim.client.forwarded()); got != want {
		t.Fatalf("client stream = %q, want the buffered prefix flushed in order ahead of it: %q", got, want)
	}
	sim.reported(t)
}

func TestSimulatorCapabilityRefusalDoesNotCrownAndGrowsTheContextHint(t *testing.T) {
	sim := newSimulator(t, speculativePolicy(2), 2, qwenModel)
	sim.host(10, 0, "small-host", &hostScript{
		receipt: true,
		chunks: []string{
			`data: {"error":{"code":400,"message":"` + vllmContextTotalMessage + `","type":"BadRequestError"}}` + "\n\n",
		},
	})
	sim.host(11, 1, "large-host", &hostScript{
		receipt: true, chunks: []string{contentEvent("hi")},
		confirmed: true, finished: true,
	})

	outcome, err := sim.run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if outcome.WinnerNonce != 11 {
		t.Fatalf("winner = %d, want the host that could serve the prompt", outcome.WinnerNonce)
	}
	reported := sim.reported(t)
	if refused := attemptFor(t, reported, 10); refused.Terminal != TerminalCapabilityRefused {
		t.Fatalf("terminal = %v, want TerminalCapabilityRefused", refused.Terminal)
	}
	sim.picker.mu.Lock()
	profiles := append([]scheduler.RequestProfile(nil), sim.picker.profiles...)
	sim.picker.mu.Unlock()
	if len(profiles) != 2 {
		t.Fatalf("Pick calls = %d, want 2", len(profiles))
	}
	if profiles[1].ContextHint != 41_200 {
		t.Errorf("re-pick ContextHint = %d, want 41200", profiles[1].ContextHint)
	}
	if len(profiles[1].Exclude) != 1 || profiles[1].Exclude[0] != "small-host" {
		t.Errorf("re-pick Exclude = %v, want [small-host]", profiles[1].Exclude)
	}
	if recorded := sim.perf.limits; len(recorded) != 1 || recorded[0].maxTokens != 40_960 {
		t.Errorf("recorded context limits = %+v, want one of 40960", recorded)
	}
}

func TestSimulatorForwardsTheFixtureCorpusUnchangedAtEveryChunkSize(t *testing.T) {
	body := readFixture(t, "content_stream.sse")
	for _, chunkSize := range []int{1, 3, 64, 1024, 8192} {
		t.Run("chunk="+strconv.Itoa(chunkSize), func(t *testing.T) {
			sim := newSimulator(t, settledPolicy(), 1, qwenModel)
			chunks := make([]string, 0, len(body)/chunkSize+1)
			for start := 0; start < len(body); start += chunkSize {
				chunks = append(chunks, string(body[start:min(start+chunkSize, len(body))]))
			}
			sim.host(10, 0, "host-0", &hostScript{
				receipt: true, chunks: chunks, confirmed: true, finished: true,
			})

			if _, err := sim.run(context.Background()); err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			if !bytes.Equal(sim.client.forwarded(), body) {
				t.Fatalf("client stream differs from the host stream at chunk size %d", chunkSize)
			}
			if winner := attemptFor(t, sim.reported(t), 10); winner.ContentSource != "delta.content" {
				t.Fatalf("ContentSource = %q, want delta.content", winner.ContentSource)
			}
		})
	}
}

// A stream whose last event never gets its newline is classified when the stream closes, so the host
// is not charged for an empty answer it did not give.
func TestSimulatorFlushesTheFinalEventBeforeDecidingEmptiness(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{
		receipt:  true,
		chunks:   []string{string(readFixture(t, "newlineless_final_content.sse"))},
		finished: true,
	})

	if _, err := sim.run(context.Background()); !errors.Is(err, ErrAllAttemptsFailed) {
		t.Fatalf("Run() error = %v, want ErrAllAttemptsFailed", err)
	}

	reported := sim.reported(t)
	attempt := attemptFor(t, reported, 10)
	if attempt.Terminal != TerminalLost {
		t.Fatalf("terminal = %v, want TerminalLost", attempt.Terminal)
	}
	if attempt.ContentSource != "delta.content" {
		t.Errorf("ContentSource = %q, want delta.content from the flushed tail", attempt.ContentSource)
	}
	if reported.DeniesCrowning(attempt) {
		t.Error("a flushed content event was still charged as an empty answer")
	}
}

func TestSimulatorPhaseTransitionAbortSkipsTheSampleAndTheVote(t *testing.T) {
	sim := newSimulatorInPhase(t, settledPolicy(), 1, qwenModel,
		config.Modes{PoCMode: config.PoCModeRelaxed}, chain.PhaseSnapshot{})
	arrive, release := make(chan uint64, 1), make(chan struct{})
	sim.host(10, 0, "host-0", &hostScript{arrive: arrive, release: release, receipt: true})

	outcomes := make(chan error, 1)
	go func() {
		_, err := sim.run(context.Background())
		outcomes <- err
	}()
	<-arrive
	sim.snapshots.set(chain.PhaseSnapshot{EpochPhase: chain.EpochPhasePoCGenerate})
	close(release)
	<-outcomes

	reported := sim.reported(t)
	attempt := attemptFor(t, reported, 10)
	if !attempt.PhaseTransitionAborted {
		t.Fatal("an attempt the phase transition ended was blamed on its host")
	}
	if exemption := reported.sampleExemption(attempt); exemption != ExemptPhaseAborted {
		t.Errorf("sample exemption = %v, want ExemptPhaseAborted", exemption)
	}
	if samples := sim.perf.recorded(); len(samples) != 0 {
		t.Errorf("samples = %+v, want none", samples)
	}
	if moves := sim.windows.recorded(); len(moves) != 0 {
		t.Errorf("window moves = %+v, want none", moves)
	}
	sim.settleAll()
	events := sim.timeoutEvents()
	if len(events) != 1 || events[0].Action != TimeoutActionSkipped || events[0].Reason != timeoutReasonPhaseAborted {
		t.Fatalf("timeout events = %+v, want one skipped/%s", events, timeoutReasonPhaseAborted)
	}
	if posted := sim.poster.settled(); len(posted) != 0 {
		t.Errorf("votes posted = %v, want none", posted)
	}
}

// The shutdown barrier: the vote a finished race still owes is registered before its goroutine
// starts, so Stop can never observe the engine as idle while a nonce is unsettled.
func TestSimulatorStopWaitsForTheVoteAFinishedRaceStillOwes(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.poster.release = make(chan struct{})
	sim.host(10, 0, "host-0", &hostScript{receipt: true, err: errors.New("host refused")})

	if _, err := sim.run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want the failed attempt's error")
	}
	sim.reported(t)
	<-sim.poster.entered

	stopped := make(chan struct{})
	go func() {
		sim.engine.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a vote was still in flight")
	default:
	}
	close(sim.poster.release)
	<-stopped

	if posted := sim.poster.settled(); len(posted) != 1 || posted[0] != 10 {
		t.Fatalf("votes posted = %v, want [10]", posted)
	}
}

// The escrow the race dispatched through is held past Run's return, because the vote its nonce owes is
// posted from a goroutine that outlives the race. A hold given back at Run's return lets a rotation
// close the session the vote still needs.
func TestSimulatorTheEscrowStaysHeldUntilTheVoteIsPosted(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	postVote := sim.parkVotes(t)
	sim.host(10, 0, "host-0", &hostScript{receipt: true, err: errors.New("host refused")})

	if _, err := sim.run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want the failed attempt's error")
	}
	sim.reported(t)
	<-sim.poster.entered

	holds, releases := sim.target.held()
	if holds != 1 {
		t.Fatalf("escrow holds taken = %d, want 1", holds)
	}
	if releases != 0 {
		t.Fatalf("escrow holds given back while the vote is still in flight = %d, want 0", releases)
	}

	postVote()
	sim.engine.Stop()

	if _, releases := sim.target.held(); releases != 1 {
		t.Errorf("escrow holds given back after the vote = %d, want 1", releases)
	}
}

// A race that panics owes the escrow its hold back, or the rotation that retires it waits on a request
// nobody is running.
func TestSimulatorAPanickingRaceGivesTheEscrowHoldBack(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{receipt: true})
	sim.target.panicking.Store(true)

	func() {
		defer func() {
			if panicked := recover(); panicked == nil {
				t.Error("Run() recovered the panic instead of re-raising it")
			}
		}()
		_, _ = sim.run(context.Background())
	}()

	holds, releases := sim.target.held()
	if holds != 1 || releases != 1 {
		t.Fatalf("escrow holds taken/given back = %d/%d, want 1/1", holds, releases)
	}
}

// The shutdown invariant: a race that is still running when Stop is called owes a vote, so Stop cannot
// return until that race has posted it. Waiting only for races that had already finished would leave
// the nonce of an in-flight one stranded when the process exits.
func TestSimulatorStopWaitsForARaceThatIsStillRunning(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	streaming, resume := make(chan uint64, 1), make(chan struct{})
	sim.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentEvent("hi")},
		streaming: streaming,
		resume:    resume,
	})

	outcomes := make(chan error, 1)
	go func() {
		_, err := sim.run(context.Background())
		outcomes <- err
	}()
	<-streaming

	stopped := make(chan struct{})
	go func() {
		sim.engine.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a race was still running")
	case <-time.After(250 * time.Millisecond):
	}

	close(resume)
	<-outcomes
	<-stopped

	if posted := sim.poster.settled(); len(posted) != 1 || posted[0] != 10 {
		t.Fatalf("votes posted = %v, want [10] before Stop returned", posted)
	}
	sim.reported(t)
}

// A host that produces content and then goes silent has not finished its nonce and is not credited
// with one. Its own attempt is still cancelled and accounted for, and because reaching the streaming
// hard timeout puts it past the long-response exemption, the vote is skipped for that reason rather
// than dropped unrecorded.
// A host the backstop cut answers for it: it held the request for twenty minutes and finished
// nothing, so its window contracts and its nonce goes to a timeout vote rather than being excused as
// a long response still in progress.
func TestSimulatorAHostCutAtTheBackstopAnswersForIt(t *testing.T) {
	policy := settledPolicy()
	policy.InterChunkStall = 500 * time.Millisecond
	sim := newSimulator(t, policy, 1, qwenModel)
	streaming, resume := make(chan uint64, 1), make(chan struct{})
	sim.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentEvent("hi")},
		streaming: streaming,
		resume:    resume,
		hold:      true,
	})

	outcomes := make(chan error, 1)
	go func() {
		_, err := sim.run(context.Background())
		outcomes <- err
	}()
	<-streaming
	sim.clock.waitArmed(t, policy.InterChunkStall)
	sim.clock.advance(streamingHardTimeout + time.Second)
	close(resume)
	<-outcomes

	reported := sim.reported(t)
	attempt := attemptFor(t, reported, 10)
	if attempt.Terminal != TerminalHardTimeout {
		t.Fatalf("terminal = %v, want TerminalHardTimeout", attempt.Terminal)
	}
	if attempt.NonceFinished || attempt.Confirmed {
		t.Fatal("a host cut at the backstop was credited with the nonce it never finished")
	}
	if moves := sim.windows.recorded(); len(moves) == 0 {
		t.Error("window did not move for a host that held the request past the backstop")
	}
	sim.settleAll()
	events := sim.timeoutEvents()
	voted := false
	for _, event := range events {
		if event.Action == TimeoutActionSkipped {
			t.Fatalf("timeout events = %+v, want the vote to run rather than be skipped", events)
		}
		voted = voted || event.Action == TimeoutActionCompleted
	}
	if !voted {
		t.Fatalf("timeout events = %+v, want the vote carried through to a miss", events)
	}
	sim.assertNoSlotLeaked(t, 1)
}

func TestSimulatorBurnEmptyIsAModelOutcomeAndPlainEmptyIsNot(t *testing.T) {
	burnStream := `data: {"choices":[{"delta":{}}],"usage":{"completion_tokens":90}}` + "\n\n"
	cases := []struct {
		name         string
		model        string
		wantTerminal Terminal
		wantDenied   bool
	}{
		{
			name:         "thinking_budget_route_burned_the_tokens",
			model:        kimiModel,
			wantTerminal: TerminalBurnEmpty,
		},
		{
			name:         "any_other_route_answered_with_nothing",
			model:        qwenModel,
			wantTerminal: TerminalEmptyStream,
			wantDenied:   true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			sim := newSimulator(t, settledPolicy(), 1, testCase.model)
			sim.host(10, 0, "host-0", &hostScript{receipt: true, chunks: []string{burnStream}, finished: true})

			if _, err := sim.run(context.Background()); !errors.Is(err, ErrEmptyStream) {
				t.Fatalf("Run() error = %v, want ErrEmptyStream", err)
			}

			reported := sim.reported(t)
			attempt := attemptFor(t, reported, 10)
			if attempt.Terminal != testCase.wantTerminal {
				t.Fatalf("terminal = %v, want %v", attempt.Terminal, testCase.wantTerminal)
			}
			if reported.DeniesCrowning(attempt) != testCase.wantDenied {
				t.Errorf("DeniesCrowning = %v, want %v", reported.DeniesCrowning(attempt), testCase.wantDenied)
			}
			if moves := sim.windows.recorded(); len(moves) != 1 || moves[0].verdict != limits.ModelOutcome {
				t.Fatalf("window moves = %+v, want one ModelOutcome", moves)
			}
		})
	}
}

func TestSimulatorSpeculativeSecondaryWinsAndItsLosersAreDrained(t *testing.T) {
	sim := newSimulator(t, racePolicy(2), 2, qwenModel)
	arrive, release := make(chan uint64, 2), make(chan struct{})
	sim.host(10, 0, "slow-host", &hostScript{
		arrive: arrive, release: release,
		receipt: true, chunks: []string{roleEvent}, confirmed: true, finished: true,
	})
	sim.host(11, 1, "fast-host", &hostScript{
		arrive: arrive, release: release,
		receipt: true, chunks: []string{contentEvent("hi")}, confirmed: true, finished: true,
	})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := sim.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	<-arrive
	<-arrive
	close(release)
	<-outcomes

	reported := sim.reported(t)
	if reported.WinnerNonce != 11 {
		t.Fatalf("winner = %d, want the only content producer", reported.WinnerNonce)
	}
	if got, want := string(sim.client.forwarded()), contentEvent("hi"); got != want {
		t.Fatalf("client stream = %q, want only the winner's bytes %q", got, want)
	}
	won := 0
	for _, attempt := range reported.Attempts {
		if attempt.Terminal == TerminalWon {
			won++
		}
	}
	if won != 1 || len(reported.Attempts) != 2 {
		t.Fatalf("attempts = %d with %d crowned, want 2 with 1", len(reported.Attempts), won)
	}
	sim.assertNoSlotLeaked(t, 2)
	sim.assertOneOutcome(t)
}

func TestSimulatorClientDisconnectStillSettlesTheNonce(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	streaming, resume := make(chan uint64, 1), make(chan struct{})
	sim.host(10, 0, "host-0", &hostScript{
		receipt:   true,
		chunks:    []string{contentEvent("hi"), contentEvent("after the client left")},
		streaming: streaming,
		resume:    resume,
		confirmed: true,
		finished:  true,
	})

	clientCtx, disconnect := context.WithCancel(context.Background())
	outcomes := make(chan error, 1)
	go func() {
		_, err := sim.run(clientCtx)
		outcomes <- err
	}()
	<-streaming
	disconnect()

	if err := <-outcomes; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v, want context.Canceled", err)
	}
	close(resume)

	reported := sim.reported(t)
	attempt := attemptFor(t, reported, 10)
	if !attempt.NonceFinished {
		t.Fatal("the race abandoned its nonce when the client left")
	}
	if attempt.Terminal != TerminalWon {
		t.Fatalf("terminal = %v, want TerminalWon: the host ran to completion", attempt.Terminal)
	}
	sim.settleAll()
	if posted := sim.poster.settled(); len(posted) != 0 {
		t.Errorf("votes posted = %v, want none for a finished nonce", posted)
	}
	sim.assertNoSlotLeaked(t, 1)
	sim.assertOneOutcome(t)
}

func TestSimulatorGarbageStreamNeverCrowns(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{
		receipt: true,
		chunks: []string{
			"data: {\x00\x00\x00\n\n",
			`data: {"choices":[{"delta"` + "\n",
			"data: [DONE]\n\n",
		},
		finished: true,
	})

	outcome, err := sim.run(context.Background())

	if !errors.Is(err, ErrEmptyStream) {
		t.Fatalf("Run() error = %v, want ErrEmptyStream", err)
	}
	if outcome.WinnerNonce != 0 {
		t.Fatalf("garbage crowned nonce %d", outcome.WinnerNonce)
	}
	if len(sim.client.forwarded()) != 0 {
		t.Errorf("client received %q from a stream that produced no content", sim.client.forwarded())
	}
	if attempt := attemptFor(t, sim.reported(t), 10); attempt.Terminal != TerminalEmptyStream {
		t.Fatalf("terminal = %v, want TerminalEmptyStream", attempt.Terminal)
	}
}

// The suspicious list reaches Decide through the engine's own dependency, so the branch is proven
// reachable from what main wires rather than from a policy the test constructed.
func TestSimulatorPinnedPrimaryStartsASecondAttemptImmediately(t *testing.T) {
	sim := newSimulator(t, racePolicy(2), 2, qwenModel)
	sim.pinned["pinned-host"] = true
	arrive, release := make(chan uint64, 2), make(chan struct{})
	sim.host(10, 0, "pinned-host", &hostScript{
		arrive: arrive, release: release,
		receipt: true, chunks: []string{roleEvent}, confirmed: true, finished: true,
	})
	sim.host(11, 1, "trusted-host", &hostScript{
		arrive: arrive, release: release,
		receipt: true, chunks: []string{contentEvent("hi")}, confirmed: true, finished: true,
	})

	outcomes := make(chan RaceOutcome, 1)
	go func() {
		outcome, err := sim.run(context.Background())
		if err != nil {
			t.Error(err)
		}
		outcomes <- outcome
	}()
	// Both attempts arrive before either is released, which is only possible if the second was started
	// at once rather than after the first had failed or timed out.
	<-arrive
	<-arrive
	close(release)
	<-outcomes

	reported := sim.reported(t)
	if reported.Decision != StartPrimarySuspicious {
		t.Fatalf("decision = %q, want %q", reported.Decision, StartPrimarySuspicious)
	}
	if len(reported.Attempts) != 2 {
		t.Fatalf("attempts = %d, want 2 started immediately", len(reported.Attempts))
	}
	if !attemptFor(t, reported, 10).Suspicious {
		t.Error("the pinned host's attempt is not marked suspicious")
	}
	if attemptFor(t, reported, 11).Suspicious {
		t.Error("the unpinned host's attempt is marked suspicious")
	}
	sim.assertNoSlotLeaked(t, 2)
	sim.assertOneOutcome(t)
}

type panickingPicker struct{}

func (p *panickingPicker) Pick(context.Context, scheduler.RequestProfile) (scheduler.Assignment, error) {
	panic("picker exploded")
}

func (p *panickingPicker) HostDiverged(string, string, time.Time) bool { return false }

func (p *panickingPicker) HostServed(string, string, time.Time) {}

// net/http recovers a handler panic per connection, so a race that panics without releasing its
// registration leaves the process alive and Stop waiting forever. The engine is built here rather
// than through the simulator because the simulator's cleanup is itself a Stop, which would turn a
// failing assertion into a hung suite.
func TestPanickingRaceReleasesTheStopBarrierAndRepanics(t *testing.T) {
	races := NewEngine(Deps{
		Picker:    &panickingPicker{},
		Config:    config.NewHolder(engineSettings(settledPolicy(), config.Modes{})),
		Snapshots: newSimSnapshots(chain.PhaseSnapshot{}),
		Now:       func() time.Time { return testEpoch },
	})

	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		_, _ = races.Run(context.Background(), Request{RequestID: "panicking", Model: qwenModel}, &bytes.Buffer{})
	}()

	select {
	case panicked := <-recovered:
		if panicked == nil {
			t.Fatal("the panic was swallowed; the client would be answered with an outcome no race produced")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run() neither returned nor panicked")
	}

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		races.Stop()
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() never returned: the panicking race still holds its registration")
	}
}
