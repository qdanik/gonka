package limits

import (
	"cmp"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	"devshard/cmd/gateway/internal/safemath"
)

type Verdict int

const (
	Success Verdict = iota
	Overload
	UpstreamFault
	TransportFault
	ModelOutcome
	MissedReceiptDeadline
	MissedFirstTokenDeadline
	LateSuccess
	EmptyAnswer
	EmptyAnswerLeftOpen
	DecodeStalled
) // See README.md, "What blames which window".

// WindowBounds is one congestion window's floor, starting size and additive step, in tokens.
type WindowBounds struct {
	Min     int64
	Initial int64
	Step    int64
}

// ModelWindows is one model's two congestion windows.
type ModelWindows struct {
	Input  WindowBounds
	Output WindowBounds
}

// RequestBounds is one congestion window's floor and starting size, counted in requests.
type RequestBounds struct {
	Min     int64
	Initial int64
}

// WindowPricing turns a model into the two windows a host gets for it. See capacity.md, "The participant limiter: IOCW".
type WindowPricing struct {
	Input                 RequestBounds
	Output                RequestBounds
	FallbackContextTokens int64
	FallbackOutputTokens  int64
	ContextTokensByModel  map[string]int64
	OutputTokensByModel   map[string]int64
}

// ParticipantConfig's IdleEviction of zero keeps every pair for the life of the process.
type ParticipantConfig struct {
	Pricing       WindowPricing
	Factors       CongestionFactors
	Slack         float64
	AfterFailures int64
	BaseOpen      time.Duration
	MaxOpen       time.Duration
	IdleEviction  time.Duration
}

func boundsIn(requests RequestBounds, requestTokens int64) WindowBounds {
	return WindowBounds{
		Min:     safemath.MulSaturating(requests.Min, requestTokens),
		Initial: safemath.MulSaturating(requests.Initial, requestTokens),
		Step:    requestTokens,
	}
}

func pinnedOr(pinned map[string]int64, model string, fallback int64) int64 {
	if tokens, named := pinned[model]; named && tokens > 0 {
		return tokens
	}
	return fallback
}

// windowsForLocked prices one model. See capacity.md, "The participant limiter: IOCW".
func (l *ParticipantLimiter) windowsForLocked(model string) ModelWindows {
	pricing := l.cfg.Pricing
	contextTokens := pinnedOr(pricing.ContextTokensByModel, model, pinnedOr(l.observedContext, model, pricing.FallbackContextTokens))
	return ModelWindows{
		Input:  boundsIn(pricing.Input, contextTokens),
		Output: boundsIn(pricing.Output, pinnedOr(pricing.OutputTokensByModel, model, pricing.FallbackOutputTokens)),
	}
}

type key struct {
	participant string
	model       string
}

// window is one congestion window and what is in flight against it. Peak is the high-water mark since the last adjustment.
type window struct {
	tokens   float64
	inflight int64
	peak     int64
	narrowed bool
}

func fits(inflight, cost int64, window float64) bool {
	return float64(inflight)+float64(cost) <= window
}

func (w *window) take(tokens int64) {
	w.inflight = safemath.AddSaturating(w.inflight, tokens)
	if w.inflight > w.peak {
		w.peak = w.inflight
	}
}

func (w *window) give(tokens int64) {
	w.inflight = max(w.inflight-tokens, 0)
}

func (w *window) reopen(tokens int64) {
	w.tokens = float64(tokens)
	w.peak = w.inflight
	w.narrowed = false
}

type hostState struct {
	bounds                  ModelWindows
	input                   window
	output                  window
	consecutiveCutoffFaults int
	openUntil               time.Time
	backoffCount            int
	halfOpen                bool
	lastUsed                time.Time
}

func (s *hostState) idle() bool {
	return s.input.inflight == 0 && s.output.inflight == 0
}

// cutoffNarrator is satisfied by *journal.Journal; it is called under the limiter's lock, so it must queue and return. See README.md, "When a host stops taking work".
type cutoffNarrator interface {
	HostCutOff(participant, model, reason string, backoffCount int, cutOffFor time.Duration)
	HostCutOffLifted(participant, model string, backoffCount int)
}

type ParticipantLimiter struct {
	mu              sync.Mutex
	cfg             ParticipantConfig
	observedContext map[string]int64
	states          map[key]*hostState
	now             func() time.Time
	jitter          func(time.Duration) time.Duration
	lastSweep       time.Time
	narrator        cutoffNarrator
}

func NewParticipantLimiter(cfg ParticipantConfig, now func() time.Time) *ParticipantLimiter {
	return &ParticipantLimiter{
		cfg:    cfg,
		states: make(map[key]*hostState),
		now:    now,
		jitter: defaultJitter,
	}
}

// SetNarrator binds the journal the limiter's cut-off edges are written through; call it before the limiter is shared.
func (l *ParticipantLimiter) SetNarrator(narrator cutoffNarrator) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.narrator = narrator
}

// defaultJitter: up to 20% of base (gRPC connection-backoff's JITTER 0.2), so reopened cutoffs don't retry in lockstep.
func defaultJitter(base time.Duration) time.Duration {
	span := int64(base) / 5
	if span <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(span))
}

// Reconfigure keeps a window already earned and lifts one below the new initial size. See capacity.md, "The participant limiter: IOCW".
func (l *ParticipantLimiter) Reconfigure(cfg ParticipantConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cfg = cfg
	for tracked, state := range l.states {
		state.bounds = l.windowsForLocked(tracked.model)
		state.input.tokens = max(state.input.tokens, float64(state.bounds.Input.Initial))
		state.output.tokens = max(state.output.tokens, float64(state.bounds.Output.Initial))
	}
}

func (l *ParticipantLimiter) stateLocked(k key) *hostState {
	state, ok := l.states[k]
	if !ok {
		state = l.freshState(k.model)
		l.states[k] = state
	}
	return state
}

// ObserveModels takes the context length governance reports for each model. See capacity.md, "The participant limiter: IOCW".
func (l *ParticipantLimiter) ObserveModels(contextTokens map[string]int64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.observedContext == nil {
		l.observedContext = make(map[string]int64, len(contextTokens))
	}
	maps.Copy(l.observedContext, contextTokens)
	for tracked, state := range l.states {
		state.bounds = l.windowsForLocked(tracked.model)
		state.input.tokens = max(state.input.tokens, float64(state.bounds.Input.Min))
		state.output.tokens = max(state.output.tokens, float64(state.bounds.Output.Min))
	}
}

func (l *ParticipantLimiter) freshState(model string) *hostState {
	bounds := l.windowsForLocked(model)
	return &hostState{
		bounds: bounds,
		input:  window{tokens: float64(bounds.Input.Initial)},
		output: window{tokens: float64(bounds.Output.Initial)},
	}
}

// smallestRequest is what Admits peeks with: a host that cannot take one token of each has no headroom at all.
var smallestRequest = TokenCost{Input: 1, Output: 1}

func admissionLocked(state *hostState, cost TokenCost, now time.Time) Admission {
	switch cutoffState(state, now) {
	case CutoffOpen:
		return AdmissionCutOff
	case CutoffHalfOpen:
		if state.idle() {
			return AdmissionOpen
		}
		return AdmissionCutOff
	}
	if state.idle() {
		return AdmissionOpen
	}
	if fits(state.input.inflight, cost.Input, state.input.tokens) &&
		fits(state.output.inflight, cost.Output, state.output.tokens) {
		return AdmissionOpen
	}
	return AdmissionWindowFull
}

// Acquire takes the tokens one attempt needs and hands back the release that gives them back exactly once, or the admission that refused them.
func (l *ParticipantLimiter) Acquire(participant, model string, cost TokenCost) (func(), Admission) {
	return l.take(participant, model, cost, false)
}

// Overdraft takes the tokens whether or not they fit, and is refused only by a cut-off. See routing.md, "The forced send".
func (l *ParticipantLimiter) Overdraft(participant, model string, cost TokenCost) (func(), Admission) {
	return l.take(participant, model, cost, true)
}

func (l *ParticipantLimiter) take(participant, model string, cost TokenCost, overFullWindow bool) (func(), Admission) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	tracked := key{participant: participant, model: model}
	state := l.stateLocked(tracked)
	state.lastUsed = now
	l.forgetIdleLocked(now)

	switch refusedBy := admissionLocked(state, cost, now); refusedBy {
	case AdmissionCutOff:
		return nil, refusedBy
	case AdmissionWindowFull:
		if !overFullWindow {
			return nil, refusedBy
		}
	}
	if !state.openUntil.IsZero() {
		state.halfOpen = true
	}
	state.input.take(max(cost.Input, 0))
	state.output.take(max(cost.Output, 0))
	return l.releaseFor(tracked, cost), AdmissionOpen
}

func (l *ParticipantLimiter) releaseFor(tracked key, cost TokenCost) func() {
	var once sync.Once
	return func() {
		once.Do(func() { l.release(tracked, cost) })
	}
}

func (l *ParticipantLimiter) release(tracked key, cost TokenCost) {
	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.states[tracked]
	if !ok {
		return
	}
	state.input.give(max(cost.Input, 0))
	state.output.give(max(cost.Output, 0))
	state.lastUsed = l.now()
}

// Available peeks whether a host would take the smallest request, mutating nothing. See capacity.md, "The participant limiter: IOCW".
func (l *ParticipantLimiter) Available(participant, model string) bool {
	return l.Admits(participant, model) == AdmissionOpen
}

// Admits peeks why a host would refuse the smallest request, mutating nothing. See capacity.md, "The participant limiter: IOCW".
func (l *ParticipantLimiter) Admits(participant, model string) Admission {
	l.mu.Lock()
	defer l.mu.Unlock()

	state, ok := l.states[key{participant: participant, model: model}]
	if !ok {
		state = l.freshState(model)
	}
	return admissionLocked(state, smallestRequest, l.now())
}

// HostWindow is one tracked participant/model pair as a reader sees it.
type HostWindow struct {
	Participant          string
	Model                string
	InputWindowTokens    float64
	OutputWindowTokens   float64
	InflightInputTokens  int64
	InflightOutputTokens int64
	Cutoff               CutoffState
	BackoffCount         int
	Available            bool
}

// Snapshot returns every tracked pair in participant/model order, copied under one lock and sorted after it releases.
func (l *ParticipantLimiter) Snapshot() []HostWindow {
	l.mu.Lock()
	now := l.now()
	windows := make([]HostWindow, 0, len(l.states))
	for tracked, state := range l.states {
		windows = append(windows, HostWindow{
			Participant:          tracked.participant,
			Model:                tracked.model,
			InputWindowTokens:    state.input.tokens,
			OutputWindowTokens:   state.output.tokens,
			InflightInputTokens:  state.input.inflight,
			InflightOutputTokens: state.output.inflight,
			Cutoff:               cutoffState(state, now),
			BackoffCount:         state.backoffCount,
			Available:            admissionLocked(state, smallestRequest, now) == AdmissionOpen,
		})
	}
	l.mu.Unlock()

	slices.SortFunc(windows, func(first, second HostWindow) int {
		return cmp.Or(cmp.Compare(first.Participant, second.Participant), cmp.Compare(first.Model, second.Model))
	})
	return windows
}

func cutoffState(state *hostState, now time.Time) CutoffState {
	switch {
	case now.Before(state.openUntil):
		return CutoffOpen
	case state.halfOpen || !state.openUntil.IsZero():
		return CutoffHalfOpen
	}
	return CutoffClosed
}

// ClearQuarantine reopens one participant's cutoffs. See capacity.md, "The participant limiter: IOCW".
func (l *ParticipantLimiter) ClearQuarantine(participant string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	cleared := false
	for tracked, state := range l.states {
		if tracked.participant != participant {
			continue
		}
		state.input.reopen(state.bounds.Input.Initial)
		state.output.reopen(state.bounds.Output.Initial)
		state.consecutiveCutoffFaults = 0
		state.openUntil = time.Time{}
		state.backoffCount = 0
		state.halfOpen = false
		cleared = true
	}
	return cleared
}

// See capacity.md, "Nothing here is persisted".
func (l *ParticipantLimiter) forgetIdleLocked(now time.Time) {
	idleFor := l.cfg.IdleEviction
	if idleFor <= 0 || now.Sub(l.lastSweep) < idleFor/10 {
		return
	}
	l.lastSweep = now
	for tracked, state := range l.states {
		if state.idle() && !now.Before(state.openUntil) && now.Sub(state.lastUsed) > idleFor {
			delete(l.states, tracked)
		}
	}
}

func cutoffReason(halfOpen bool) string {
	if halfOpen {
		return cutoffReasonProbeFailed
	}
	return cutoffReasonConsecutiveFaults
}

// OnResult applies one finished attempt to the host's windows and its cutoff.
func (l *ParticipantLimiter) OnResult(result Result) {
	answer := responseFor(result.Verdict)
	if answer.inert() {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	state := l.stateLocked(key{participant: result.Participant, model: result.Model})
	now := l.now()
	state.lastUsed = now

	narrowingTier, blamed := answer.tier, answer.dimension
	if narrowingTier == tierNone && result.Verdict == Success {
		if congested := result.Pressure.congestedDimension(l.cfg.Slack); congested != dimensionNone {
			narrowingTier, blamed = tierSoft, congested
		} else {
			l.growLocked(state, result.Carried)
		}
	}
	if by := l.cfg.Factors.narrowingFor(narrowingTier, blamed); by.moves() {
		l.narrowLocked(state, by)
	}
	l.applyBreakerLocked(state, answer.breaker, result.Participant, result.Model, now)
}

// growLocked widens each window whose peak earned it. See README.md, "Additive increase".
func (l *ParticipantLimiter) growLocked(state *hostState, carried TokenCost) {
	if float64(state.input.peak) >= state.input.tokens/2 {
		state.input.tokens = grownBy(state.input.tokens, carried.Input, float64(state.bounds.Input.Step), !state.input.narrowed)
		state.input.peak = state.input.inflight
	}
	if float64(state.output.peak) >= state.output.tokens/2 {
		state.output.tokens = grownBy(state.output.tokens, carried.Output, float64(state.bounds.Output.Step), !state.output.narrowed)
		state.output.peak = state.output.inflight
	}
}

// narrowLocked applies one signal's factors to the two windows.
func (l *ParticipantLimiter) narrowLocked(state *hostState, by narrowing) {
	narrowWindow(&state.input, by.input, state.bounds.Input.Min)
	narrowWindow(&state.output, by.output, state.bounds.Output.Min)
}

// narrowWindow ends slow start and restarts the peak, for a window it actually moved. See README.md, "Additive increase".
func narrowWindow(w *window, factor float64, floor int64) {
	if factor == 1 {
		return
	}
	w.tokens = narrowedTo(w.tokens, factor, float64(floor))
	w.peak = w.inflight
	w.narrowed = true
}

func (l *ParticipantLimiter) applyBreakerLocked(state *hostState, effect breakerEffect, participant, model string, now time.Time) {
	switch effect {
	case breakerClears:
		state.consecutiveCutoffFaults = 0
	case breakerRecovers:
		state.consecutiveCutoffFaults = 0
		if !state.halfOpen {
			return
		}
		state.halfOpen = false
		state.openUntil = time.Time{}
		if state.backoffCount > 0 {
			state.backoffCount--
		}
		if l.narrator != nil {
			l.narrator.HostCutOffLifted(participant, model, state.backoffCount)
		}
	case breakerCounts:
		state.consecutiveCutoffFaults++
		if state.consecutiveCutoffFaults < int(l.cfg.AfterFailures) && !state.halfOpen {
			return
		}
		capped := min(time.Duration(float64(l.cfg.BaseOpen)*math.Pow(1.6, float64(state.backoffCount))), l.cfg.MaxOpen)
		state.openUntil = now.Add(capped + l.jitter(capped))
		if capped < l.cfg.MaxOpen {
			state.backoffCount++
		}
		reason := cutoffReason(state.halfOpen)
		state.halfOpen = false
		state.consecutiveCutoffFaults = 0
		if l.narrator != nil {
			l.narrator.HostCutOff(participant, model, reason, state.backoffCount, state.openUntil.Sub(now))
		}
	}
}
