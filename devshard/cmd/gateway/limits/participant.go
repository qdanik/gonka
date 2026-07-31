package limits

import (
	"math"
	"math/rand"
	"sort"
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

type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half_open"
)

// HostWindow is one tracked participant/model pair as a reader sees it.
type HostWindow struct {
	Participant string
	Model       string
	Window      float64
	Inflight    int
	Breaker     BreakerState
	Available   bool
}

// Snapshot returns every tracked pair in participant/model order, taken under one lock acquisition.
func (l *ParticipantLimiter) Snapshot() []HostWindow {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	windows := make([]HostWindow, 0, len(l.states))
	for tracked, state := range l.states {
		windows = append(windows, HostWindow{
			Participant: tracked.participant,
			Model:       tracked.model,
			Window:      state.window,
			Inflight:    state.inflight,
			Breaker:     breakerState(state, now),
			Available:   wouldAdmitLocked(state, now),
		})
	}
	sort.Slice(windows, func(first, second int) bool {
		if windows[first].Participant != windows[second].Participant {
			return windows[first].Participant < windows[second].Participant
		}
		return windows[first].Model < windows[second].Model
	})
	return windows
}

func breakerState(state *hostState, now time.Time) BreakerState {
	switch {
	case now.Before(state.openUntil):
		return BreakerOpen
	case state.halfOpen || !state.openUntil.IsZero():
		return BreakerHalfOpen
	}
	return BreakerClosed
}

// ClearQuarantine reopens every model's breaker for one participant and restores its window to the
// initial one, reporting whether the participant was tracked at all. Inflight is left alone: it
// counts attempts that are still running and is not part of the penalty.
func (l *ParticipantLimiter) ClearQuarantine(participant string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	cleared := false
	for tracked, state := range l.states {
		if tracked.participant != participant {
			continue
		}
		state.window = float64(l.cfg.InitialWindow)
		state.consecutiveTransportFail = 0
		state.openUntil = time.Time{}
		state.backoffCount = 0
		state.halfOpen = false
		cleared = true
	}
	return cleared
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
