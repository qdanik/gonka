package engine

import (
	"context"
	"errors"
	"io"
	"sync"
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

// crownDenialStrikes is how many content-free answers cost a host the crown. One content-bearing
// answer buys it back, so the denial lasts only as long as the host keeps producing nothing.
const crownDenialStrikes = 3

// hostTracker is satisfied by *perf.Tracker.
type hostTracker interface {
	hostPerf
	RecordSample(sample perf.Sample)
	RecordFirstToken(model string, inputTokens uint64, firstTokenMs float64)
}

// hostWindows is satisfied by *limits.ParticipantLimiter. Acquire is absent on purpose: the scheduler
// takes the slot in the same step that commits the nonce, and the attempt spending it gives it back.
type hostWindows interface {
	hostLimiter
	OnResult(participant, model string, verdict limits.Verdict)
}

type raceMetrics interface {
	RecordRace(outcome RaceOutcome)
	RecordTimeout(event TimeoutEvent)
	RecordClassifyOverflow(participant, model string)
}

type Deps struct {
	Picker    picker
	Targets   targets
	Windows   hostWindows
	Perf      hostTracker
	Snapshots snapshotSource
	Config    *config.Holder
	Metrics   raceMetrics

	// Timeouts yields the vote poster for one escrow and one request's payload. Escrows rotate, so it
	// is resolved per race rather than held.
	Timeouts func(escrowID string, body []byte) (TimeoutPoster, bool)

	Now   func() time.Time
	Timer func() raceTimer
}

// Request describes one chat-completions request. The bytes the host receives travel beside it,
// opaque to the engine.
type Request struct {
	RequestID     string
	Model         string
	InputTokens   uint64
	Stream        bool
	RequiresTools bool
	ContextHint   uint64
}

type Engine struct {
	deps  Deps
	carry *carryBudget
	crown *crownStrikes

	mu      sync.Mutex
	stopped bool
	// tracked counts the races whose vote has not been posted yet. A race is registered under mu before
	// it starts and released only once its vote is settled, so no nonce can be observed as nobody's.
	tracked sync.WaitGroup
}

func NewEngine(deps Deps) *Engine {
	return &Engine{
		deps:  deps,
		carry: newCarryBudget(deps.Config.Load().Stream),
		crown: newCrownStrikes(),
	}
}

// Run races one request to a single winner and streams that winner's bytes to client. The returned
// outcome is the client's view: losers may still be streaming, and the race reports itself once, from
// whichever goroutine ends it.
func (e *Engine) Run(ctx context.Context, profile Request, body []byte, client io.Writer) (RaceOutcome, error) {
	if !e.admit() {
		return RaceOutcome{}, ErrStopped
	}
	settings := e.deps.Config.Load()
	outcome, err := runRace(ctx, e.raceDeps(settings, profile, body), raceRequest{
		RequestID:     profile.RequestID,
		Model:         profile.Model,
		InputTokens:   profile.InputTokens,
		Stream:        profile.Stream,
		RequiresTools: profile.RequiresTools,
		ContextHint:   profile.ContextHint,
		Params:        body,
		Client:        client,
	})
	if err != nil {
		return outcome, err
	}
	return outcome, outcome.failure()
}

// Stop refuses new races and is a barrier over the ones already running: it returns once every race it
// admitted has posted the vote that settles its nonces. A race bounds itself, so the wait is bounded by
// the deadlines the race arms rather than by a host.
func (e *Engine) Stop() {
	e.mu.Lock()
	e.stopped = true
	e.mu.Unlock()
	e.tracked.Wait()
}

// admit registers a race before it starts. Registering here rather than when it finishes is what keeps
// a Stop that overlaps a running race from observing the engine as idle.
func (e *Engine) admit() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stopped {
		return false
	}
	e.tracked.Add(1)
	return true
}

func (e *Engine) raceDeps(settings *config.Config, profile Request, body []byte) raceDeps {
	return raceDeps{
		Picker:       e.deps.Picker,
		Targets:      e.deps.Targets,
		Limiter:      e.deps.Windows,
		Perf:         e.deps.Perf,
		Crown:        e.crown,
		Snapshots:    e.deps.Snapshots,
		Policy:       EscalationPolicyFromConfig(settings.Engine),
		Modes:        settings.Modes,
		DrainTimeout: DrainTimeoutFromConfig(settings.Stream),
		Classify:     e.classify(profile.Model),
		Now:          e.deps.Now,
		Timer:        e.deps.Timer,
		Report:       func(outcome RaceOutcome) { e.record(outcome, body) },
	}
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

// record translates one outcome into every consumer's vocabulary. The exemption ladder is applied
// here and nowhere else, so what a host is judged on cannot differ between two recording paths.
func (e *Engine) record(outcome RaceOutcome, body []byte) {
	for _, attempt := range outcome.Attempts {
		if sample, exemption := outcome.Sample(attempt); exemption == SampleRecorded {
			e.deps.Perf.RecordSample(sample)
			if firstTokenMs := firstTokenLatencyMs(attempt); firstTokenMs > 0 {
				e.deps.Perf.RecordFirstToken(outcome.Model, outcome.InputTokens, firstTokenMs)
			}
		}
		if verdict, moves := outcome.Verdict(attempt); moves {
			e.deps.Windows.OnResult(attempt.Participant, outcome.Model, verdict)
		}
	}
	if e.deps.Metrics != nil {
		e.deps.Metrics.RecordRace(outcome)
	}
	e.settle(outcome, body)
}

// settle posts the chain vote for every nonce the race left unfinished and ends the race's
// registration. The wait is the protocol's, measured in minutes, so it runs beside the race.
func (e *Engine) settle(outcome RaceOutcome, body []byte) {
	if len(outcome.TimeoutPlan()) == 0 {
		e.tracked.Done()
		return
	}
	var poster TimeoutPoster
	if e.deps.Timeouts != nil {
		if resolved, ok := e.deps.Timeouts(outcome.EscrowID, body); ok {
			poster = resolved
		}
	}
	go func() {
		defer e.tracked.Done()
		for _, event := range SettleTimeouts(context.Background(), poster, outcome) {
			if e.deps.Metrics != nil {
				e.deps.Metrics.RecordTimeout(event)
			}
		}
	}()
}

func firstTokenLatencyMs(a AttemptOutcome) float64 {
	if a.SendTime.IsZero() || a.FirstToken.IsZero() {
		return 0
	}
	return float64(a.FirstToken.Sub(a.SendTime).Milliseconds())
}

func (o RaceOutcome) failure() error {
	switch {
	case o.Succeeded:
		return nil
	case len(o.Attempts) == 0:
		return ErrAllAttemptsFailed
	// The crowned attempt's bytes are already on the wire, so no other attempt's payload can be put
	// in their place.
	case o.winnerStreamed():
		return ErrWinnerIncomplete
	}
	if hostErr := o.hostError(); hostErr != nil {
		return hostErr
	}
	switch {
	case o.everyAttempt(AttemptOutcome.emptyStream):
		return ErrEmptyStream
	case !o.Stream && o.everyAttempt(cancelledWithoutContent):
		return ErrNonStreamTimeout
	}
	return ErrAllAttemptsFailed
}

func (o RaceOutcome) winnerStreamed() bool {
	for _, attempt := range o.Attempts {
		if attempt.Nonce == o.WinnerNonce && o.WinnerNonce != 0 && attempt.ContentChunks > 0 {
			return true
		}
	}
	return false
}

// hostError prefers the crowned attempt's refusal: a host that was chosen to answer is the one whose
// answer the client asked for.
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
		if o.WinnerNonce != 0 && attempt.Nonce == o.WinnerNonce {
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

func cancelledWithoutContent(a AttemptOutcome) bool {
	return a.Terminal == TerminalClientCancelled && a.ContentChunks == 0
}

type crownKey struct{ participant, model string }

// crownStrikes withholds the crown from a host that answers without content, while leaving it in the
// scheduler's rotation. The participant set is the bounded validator set, so entries are never
// evicted; a content-bearing answer removes one.
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
