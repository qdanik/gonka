package limits

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

type Verdict int

const (
	Success Verdict = iota
	Overload
	TransportFault
	ModelOutcome
) // Overload=429/503; ModelOutcome=model-caused (empty stream etc.), never a host signal

type ParticipantConfig struct {
	InitialWindow int64
	MaxWindow     int64
	TripThreshold int64
	BaseOpen      time.Duration
	MaxOpen       time.Duration
}

type key struct {
	participant string
	model       string
}

type hostState struct {
	window                   float64
	inflight                 int
	consecutiveTransportFail int
	openUntil                time.Time
	backoffCount             int
	halfOpen                 bool
}

type ParticipantLimiter struct {
	mu     sync.Mutex
	cfg    ParticipantConfig
	states map[key]*hostState
	now    func() time.Time
	jitter func(time.Duration) time.Duration
}

func NewParticipantLimiter(cfg ParticipantConfig, now func() time.Time) *ParticipantLimiter {
	return &ParticipantLimiter{
		cfg:    cfg,
		states: make(map[key]*hostState),
		now:    now,
		jitter: defaultJitter,
	}
}

// defaultJitter: up to 20% of base (gRPC connection-backoff's JITTER 0.2), so reopened breakers don't retry in lockstep.
func defaultJitter(base time.Duration) time.Duration {
	span := int64(base) / 5
	if span <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(span))
}

func (l *ParticipantLimiter) stateLocked(k key) *hostState {
	state, ok := l.states[k]
	if !ok {
		state = &hostState{window: float64(l.cfg.InitialWindow)}
		l.states[k] = state
	}
	return state
}

func wouldAdmitLocked(state *hostState, now time.Time) bool {
	if now.Before(state.openUntil) {
		return false
	}
	halfOpen := state.halfOpen || !state.openUntil.IsZero()
	effectiveWindow := int(math.Floor(state.window))
	if halfOpen {
		effectiveWindow = 1
	}
	return state.inflight < effectiveWindow
}

// Call order per attempt is Acquire, OnResult, Release: the utilization gate reads inflight before Release frees it.
func (l *ParticipantLimiter) Acquire(participant, model string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	state := l.stateLocked(key{participant: participant, model: model})
	now := l.now()

	if now.Before(state.openUntil) {
		return false
	}
	if !state.openUntil.IsZero() {
		state.halfOpen = true
	}
	if !wouldAdmitLocked(state, now) {
		return false
	}
	state.inflight++
	return true
}

// Available peeks Acquire's admit decision for participant/model without mutating inflight, the breaker, or creating state for a participant never seen before.
func (l *ParticipantLimiter) Available(participant, model string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.states[key{participant: participant, model: model}]
	if !ok {
		state = &hostState{window: float64(l.cfg.InitialWindow)}
	}
	return wouldAdmitLocked(state, l.now())
}

func (l *ParticipantLimiter) Release(participant, model string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.states[key{participant: participant, model: model}]
	if !ok || state.inflight == 0 {
		return
	}
	state.inflight--
}

func (l *ParticipantLimiter) OnResult(participant, model string, verdict Verdict) {
	if verdict == ModelOutcome {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	state := l.stateLocked(key{participant: participant, model: model})
	now := l.now()

	switch verdict {
	case Success:
		if float64(state.inflight) >= state.window/2 {
			state.window = math.Min(state.window+1, float64(l.cfg.MaxWindow))
		}
		state.consecutiveTransportFail = 0
		if state.halfOpen {
			state.halfOpen = false
			state.openUntil = time.Time{} // clear the trip itself, not just the probe flag, or the next Acquire re-flags half-open
			if state.backoffCount > 0 {
				state.backoffCount--
			}
		}
	case Overload:
		state.window = math.Max(state.window*0.5, 1)
		state.consecutiveTransportFail = 0
	case TransportFault:
		state.consecutiveTransportFail++
		// A half-open probe gets one try: any fault reopens immediately, not after TripThreshold-many.
		if state.consecutiveTransportFail >= int(l.cfg.TripThreshold) || state.halfOpen {
			backoff := time.Duration(float64(l.cfg.BaseOpen) * math.Pow(1.6, float64(state.backoffCount)))
			backoff += l.jitter(backoff)
			if backoff > l.cfg.MaxOpen {
				backoff = l.cfg.MaxOpen
			}
			state.openUntil = now.Add(backoff)
			if backoff < l.cfg.MaxOpen { // stop counting once saturated so 1.6^count can't overflow the Duration
				state.backoffCount++
			}
			state.halfOpen = false
			state.consecutiveTransportFail = 0
		}
	}
}
