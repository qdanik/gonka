package engine

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
)

var (
	// ErrStopped rejects a request the engine can no longer see through to a settled nonce.
	ErrStopped = errors.New("engine stopped")

	ErrAllAttemptsFailed = errors.New("every attempt failed")
)

// crownDenialStrikes is how many content-free answers cost a host the crown. See race.md, "Crown denial".
const crownDenialStrikes = 3

// hostTracker is satisfied by *perf.Tracker.
type hostTracker interface {
	hostPerf
	RecordSample(sample perf.Sample)
}

// hostWindows is satisfied by *limits.ParticipantLimiter; Acquire is absent on purpose. See rules.md, "5. The slot and the escrow hold are taken with the nonce, and given back after the vote".
type hostWindows interface {
	hostLimiter
	OnResult(participant, model string, verdict limits.Verdict)
}

type raceMetrics interface {
	RecordRace(outcome RaceOutcome)
	RecordTimeout(event TimeoutEvent)
	RecordClassifyOverflow(participant, model string)
}

// raceLedger runs on the client's response path, so it must return without waiting on its storage.
type raceLedger interface {
	RecordRequest(outcome RaceOutcome)
}

// escrowLifecycle is called on the response path, so an implementation must mark and return, not reach the chain.
type escrowLifecycle interface {
	OnEscrowMissing(escrowID string)
}

// escrowTargets is the dispatch boundary main wires; resolving a target also takes a hold that outlives the race.
type escrowTargets interface {
	Target(escrowID string) (target DispatchTarget, release func(), ok bool)
}

// Deps is what an engine is wired to. See README, "Timeout votes", for why Timeouts is resolved per race.
type Deps struct {
	Picker    picker
	Targets   escrowTargets
	Windows   hostWindows
	Perf      hostTracker
	Snapshots snapshotSource
	Config    *config.Holder
	Metrics   raceMetrics
	Ledger    raceLedger
	Lifecycle escrowLifecycle

	Suspicious func(participant string) bool

	Timeouts func(escrowID string, params any) (TimeoutPoster, bool)

	Now   func() time.Time
	Timer func() raceTimer
}

// Request is one client request as a race sees it; OnEscrow fires at the last moment a header can still be set.
type Request struct {
	RequestID    string
	Model        string
	Escrow       string
	InputTokens  uint64
	ClientStream bool

	Params any

	OnEscrow func(escrowID string)
}

// Engine admits races and is the barrier that outlives them. See race.md, "Stop".
type Engine struct {
	deps  Deps
	carry *carryBudget
	crown *crownStrikes

	mu      sync.Mutex
	stopped bool
	tracked sync.WaitGroup
}

func NewEngine(deps Deps) *Engine {
	return &Engine{
		deps:  deps,
		carry: newCarryBudget(deps.Config.Load().Stream),
		crown: newCrownStrikes(),
	}
}

// Run races one request to a single winner and streams that winner's bytes to client. See README, "From pick to report".
func (e *Engine) Run(ctx context.Context, request Request, client io.Writer) (RaceOutcome, error) {
	registration := e.admit()
	if registration == nil {
		return RaceOutcome{}, ErrStopped
	}
	// A panicking race still owes the barrier its release. See race.md, "Stop".
	defer func() {
		if panicked := recover(); panicked != nil {
			registration.release()
			panic(panicked)
		}
	}()
	settings := e.deps.Config.Load()
	outcome, err := runRace(ctx, e.raceDeps(settings, request, registration), raceRequest{Request: request, Client: client})
	if err != nil {
		return outcome, err
	}
	return outcome, outcome.failure()
}

// Stop returns once every race it admitted has posted the vote that settles its nonces. See race.md, "Stop".
func (e *Engine) Stop() {
	e.mu.Lock()
	e.stopped = true
	e.mu.Unlock()
	e.tracked.Wait()
}

// admit registers a race before it starts, returning nil once the engine is stopped. See race.md, "Stop".
func (e *Engine) admit() *raceRegistration {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return nil
	}
	e.tracked.Add(1)
	return &raceRegistration{engine: e}
}

// raceRegistration is one race's place in the Stop barrier; releasing is idempotent. See README, "The Stop barrier and the escrow hold".
type raceRegistration struct {
	engine   *Engine
	released atomic.Bool

	mu         sync.Mutex
	escrowHold func()
}

// holdEscrow keeps the first hold for as long as the vote is owed; a later one counts the same escrow and goes back.
func (r *raceRegistration) holdEscrow(release func()) {
	if release == nil {
		return
	}
	r.mu.Lock()
	if r.escrowHold == nil && !r.released.Load() {
		r.escrowHold, release = release, nil
	}
	r.mu.Unlock()
	if release != nil {
		release()
	}
}

func (r *raceRegistration) release() {
	if !r.released.CompareAndSwap(false, true) {
		return
	}
	r.mu.Lock()
	hold := r.escrowHold
	r.escrowHold = nil
	r.mu.Unlock()
	if hold != nil {
		hold()
	}
	r.engine.tracked.Done()
}

// heldTargets hands the coordinator a plain target and parks its hold on the registration.
type heldTargets struct {
	escrows      escrowTargets
	registration *raceRegistration
}

func (h heldTargets) Target(escrowID string) (DispatchTarget, bool) {
	target, release, ok := h.escrows.Target(escrowID)
	if !ok {
		return nil, false
	}
	h.registration.holdEscrow(release)
	return target, true
}

func (e *Engine) raceDeps(settings *config.Config, request Request, registration *raceRegistration) raceDeps {
	return raceDeps{
		Picker:       e.deps.Picker,
		Targets:      heldTargets{escrows: e.deps.Targets, registration: registration},
		Limiter:      e.deps.Windows,
		Perf:         e.deps.Perf,
		Crown:        suspicionGate{pinned: e.deps.Suspicious, strikes: e.crown},
		Snapshots:    e.deps.Snapshots,
		Policy:       EscalationPolicyFromConfig(settings.Engine),
		Modes:        settings.Modes,
		DrainTimeout: DrainTimeoutFromConfig(settings.Stream),
		Classify:     e.classify(request.Model),
		Now:          e.deps.Now,
		Timer:        e.deps.Timer,
		Hold:         registration.holdEscrow,
		Report:       func(outcome RaceOutcome) { e.record(outcome, request.Params, registration) },
	}
}

// suspicionGate folds the operator's manual pins into the automatic crowning penalty. See race.md, "Crown denial".
type suspicionGate struct {
	pinned  func(participant string) bool
	strikes *crownStrikes
}

func (g suspicionGate) Denied(participant, model string) bool {
	return (g.pinned != nil && g.pinned(participant)) || g.strikes.Denied(participant, model)
}

func (g suspicionGate) Observe(participant, model string, contentless bool) {
	g.strikes.Observe(participant, model, contentless)
}

func (e *Engine) classify(model string) func(string) streamClassifier {
	return func(participant string) streamClassifier {
		return newSSEClassifier(e.carry, participant, model, func() {
			if e.deps.Metrics != nil {
				e.deps.Metrics.RecordClassifyOverflow(participant, model)
			}
		})
	}
}

// record translates one outcome into every consumer's vocabulary, applying the exemption ladder once. See rules.md, "2. Exactly one outcome and exactly one winner per race, on every path".
func (e *Engine) record(outcome RaceOutcome, params any, registration *raceRegistration) {
	for _, attempt := range outcome.Attempts {
		if sample, exemption := outcome.Sample(attempt); exemption == SampleRecorded {
			e.deps.Perf.RecordSample(sample)
		}
		if verdict, moves := outcome.Verdict(attempt); moves {
			e.deps.Windows.OnResult(attempt.Participant, outcome.Model, verdict)
		}
	}
	if e.deps.Metrics != nil {
		e.deps.Metrics.RecordRace(outcome)
	}
	// Marked before the row is queued, so an observer that has seen the row has also seen the mark.
	if e.deps.Lifecycle != nil && outcome.Lifecycle.EscrowMissing && outcome.EscrowID != "" {
		e.deps.Lifecycle.OnEscrowMissing(outcome.EscrowID)
	}
	if e.deps.Ledger != nil {
		e.deps.Ledger.RecordRequest(outcome)
	}
	e.settle(outcome, params, registration)
}

// settle posts the chain vote for every nonce the race left unfinished, on its own goroutine. See race.md, "Timeout votes".
func (e *Engine) settle(outcome RaceOutcome, params any, registration *raceRegistration) {
	if len(outcome.TimeoutPlan()) == 0 {
		registration.release()
		return
	}
	var poster TimeoutPoster
	if e.deps.Timeouts != nil {
		if resolved, ok := e.deps.Timeouts(outcome.EscrowID, params); ok {
			poster = resolved
		}
	}
	go func() {
		defer registration.release()
		for _, event := range SettleTimeouts(context.Background(), poster, outcome) {
			if e.deps.Metrics != nil {
				e.deps.Metrics.RecordTimeout(event)
			}
		}
	}()
}

func (o RaceOutcome) failure() error {
	switch {
	case o.Succeeded:
		return nil
	case len(o.Attempts) == 0:
		return ErrAllAttemptsFailed
	// The crowned attempt's bytes are already on the wire, so no other payload can take their place.
	case o.winnerStreamed():
		return ErrWinnerIncomplete
	}
	if hostErr := o.hostError(); hostErr != nil {
		return hostErr
	}
	if o.everyAttempt(AttemptOutcome.emptyStream) {
		return ErrEmptyStream
	}
	return ErrAllAttemptsFailed
}

func (o RaceOutcome) winnerStreamed() bool {
	for _, attempt := range o.Attempts {
		if o.IsWinner(attempt) && attempt.ContentChunks > 0 {
			return true
		}
	}
	return false
}

// hostError prefers the crowned attempt's refusal: it is the answer the client asked for.
func (o RaceOutcome) hostError() *HostApplicationError {
	var found *AttemptOutcome
	for index := range o.Attempts {
		attempt := &o.Attempts[index]
		if attempt.ErrorSource == "" {
			continue
		}
		if found == nil {
			found = attempt
		}
		if o.IsWinner(*attempt) {
			found = attempt
			break
		}
	}
	if found == nil {
		return nil
	}
	return &HostApplicationError{
		Code:    found.ErrorCode,
		Type:    found.ErrorType,
		Message: found.ErrorMessage,
		Payload: found.ErrorPayload,
	}
}

func (o RaceOutcome) everyAttempt(holds func(AttemptOutcome) bool) bool {
	for _, attempt := range o.Attempts {
		if !holds(attempt) {
			return false
		}
	}
	return true
}

type crownKey struct{ participant, model string }

// crownStrikes withholds the crown from a host answering without content, but leaves it in rotation. See rules.md, "9. Bounded by construction".
type crownStrikes struct {
	mu      sync.Mutex
	strikes map[crownKey]int
}

func newCrownStrikes() *crownStrikes { return &crownStrikes{strikes: map[crownKey]int{}} }

func (g *crownStrikes) Denied(participant, model string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.strikes[crownKey{participant: participant, model: model}] >= crownDenialStrikes
}

func (g *crownStrikes) Observe(participant, model string, contentless bool) {
	key := crownKey{participant: participant, model: model}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !contentless {
		delete(g.strikes, key)
		return
	}
	g.strikes[key]++
}
