package limits

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

const (
	reasonTooManyConcurrentRequests = "too many concurrent requests"
	reasonTooManyInputTokens        = "too many input tokens in flight"
)

type RateLimitError struct {
	Reason     string
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string {
	if e == nil {
		return "rate limit exceeded"
	}
	return fmt.Sprintf("rate limit exceeded: %s", e.Reason)
}

// ModelCapacity is one model's capacity for a single Acquire. CurrentWeight and ScaleFactor must
// already be availability- and block-filtered (0 when requests are blocked); Acquire never re-reads the snapshot.
type ModelCapacity struct {
	ScaleFactor                 float64
	CurrentWeight               float64
	BaselineWeight              float64
	MaxConcurrentPer10000Weight float64
}

type ModelOverride struct {
	MaxConcurrent  *int64
	MaxInputTokens *int64
}

type GatewayConfig struct {
	MaxConcurrent  int64
	MaxInputTokens int64
	AcquireWait    time.Duration
	ModelLimits    map[string]ModelOverride
}

type modelCounter struct {
	inFlight    int64
	inputTokens int64
}

// waiter is one blocked Acquire. A releasing request hands it the slot directly and closes ready,
// so an admitted waiter never re-competes with newly arriving requests.
type waiter struct {
	model    string
	tokens   int64
	capacity ModelCapacity
	reason   string // why it had to queue, reported if the wait times out
	ready    chan struct{}
}

type GatewayLimiter struct {
	mu     sync.Mutex
	cfg    GatewayConfig
	models map[string]*modelCounter
	total  modelCounter
	queue  []*waiter
}

func NewGatewayLimiter(cfg GatewayConfig) *GatewayLimiter {
	return &GatewayLimiter{cfg: cfg, models: map[string]*modelCounter{}}
}

func (l *GatewayLimiter) AcquireForModel(ctx context.Context, model string, inputTokens int64, capacity ModelCapacity) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if inputTokens <= 0 {
		inputTokens = 1
	}
	model = strings.TrimSpace(model)

	l.mu.Lock()
	global, perModel := l.admissionsFor(model, inputTokens, capacity)
	if reason := impossibleReason(global, perModel); reason != "" {
		l.mu.Unlock()
		return &RateLimitError{Reason: reason, RetryAfter: l.cfg.AcquireWait}
	}
	// A queued waiter is one that capacity currently cannot serve, so admitting a request that fits
	// does not overtake it: a freed slot is handed to the queue directly, under this same lock.
	reason := l.blockedReasonLocked(model, global, perModel)
	if reason == "" {
		l.takeLocked(model, inputTokens)
		l.mu.Unlock()
		return nil
	}
	if l.cfg.AcquireWait <= 0 {
		l.mu.Unlock()
		return &RateLimitError{Reason: reason, RetryAfter: l.cfg.AcquireWait}
	}

	blocked := &waiter{model: model, tokens: inputTokens, capacity: capacity, reason: reason, ready: make(chan struct{})}
	l.queue = append(l.queue, blocked)
	l.promoteLocked() // capacity may already be free; the queue only enforces order, it is not a delay
	l.mu.Unlock()

	timer := time.NewTimer(l.cfg.AcquireWait)
	defer timer.Stop()
	select {
	case <-blocked.ready:
		return nil
	case <-timer.C:
		if !l.dequeue(blocked) {
			return nil // promoted as the deadline fired: the slot is already ours
		}
		return &RateLimitError{Reason: blocked.reason, RetryAfter: l.cfg.AcquireWait}
	case <-ctx.Done():
		if !l.dequeue(blocked) {
			l.ReleaseForModel(model, inputTokens) // promoted for a caller that is already gone
		}
		return ctx.Err()
	}
}

// dequeue reports whether the waiter was still queued, meaning it never received a slot.
func (l *GatewayLimiter) dequeue(w *waiter) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, queued := range l.queue {
		if queued == w {
			l.queue = append(l.queue[:i], l.queue[i+1:]...)
			return true
		}
	}
	return false
}

// promoteLocked hands freed capacity to queued waiters in arrival order. A waiter blocked only by
// its own model's limit is skipped so it cannot stall unrelated models, but an exhausted global
// budget stops the sweep: that capacity is shared, so queueing for it stays first-come-first-served.
func (l *GatewayLimiter) promoteLocked() {
	for i := 0; i < len(l.queue); {
		w := l.queue[i]
		global, perModel := l.admissionsFor(w.model, w.tokens, w.capacity)
		if global.blockedReason(l.total.inFlight, l.total.inputTokens) != "" {
			return
		}
		var inFlight, inputTokens int64
		if counter, ok := l.models[w.model]; ok {
			inFlight, inputTokens = counter.inFlight, counter.inputTokens
		}
		if perModel.blockedReason(inFlight, inputTokens) != "" {
			i++
			continue
		}
		l.queue = append(l.queue[:i], l.queue[i+1:]...)
		l.takeLocked(w.model, w.tokens)
		close(w.ready)
	}
}

func (l *GatewayLimiter) takeLocked(model string, inputTokens int64) {
	counter := l.counterLocked(model)
	counter.inFlight++
	counter.inputTokens += inputTokens
	l.total.inFlight++
	l.total.inputTokens += inputTokens
}

// The configured maxima are process-wide: an unrecognized model shares them rather than receiving a
// fresh quota, and a per-model override only narrows one model further.
func (l *GatewayLimiter) admissionsFor(model string, inputTokens int64, capacity ModelCapacity) (global, perModel admission) {
	global = admission{requestedTokens: inputTokens}
	global.concurrencyLimit, global.concurrencyLimited = effectiveConcurrencyLimit(l.cfg.MaxConcurrent, capacity)
	global.inputTokenLimit, global.inputTokenLimited = effectiveInputTokenLimit(l.cfg.MaxInputTokens, capacity)

	perModel = admission{requestedTokens: inputTokens}
	override, ok := l.cfg.ModelLimits[model]
	if !ok {
		return global, perModel
	}
	if override.MaxConcurrent != nil {
		perModel.concurrencyLimit, perModel.concurrencyLimited = scaleClamp(*override.MaxConcurrent, capacity.ScaleFactor), true
	}
	if override.MaxInputTokens != nil {
		perModel.inputTokenLimit, perModel.inputTokenLimited = scaleClamp(*override.MaxInputTokens, capacity.ScaleFactor), true
	}
	return global, perModel
}

func (l *GatewayLimiter) ReleaseForModel(model string, inputTokens int64) {
	if inputTokens <= 0 {
		inputTokens = 1
	}
	model = strings.TrimSpace(model)

	l.mu.Lock()
	defer l.mu.Unlock()
	l.releaseLocked(model, inputTokens)
	l.promoteLocked()
}

func (l *GatewayLimiter) releaseLocked(model string, inputTokens int64) {
	counter, ok := l.models[model]
	if !ok {
		return
	}
	counter.inFlight = max(counter.inFlight-1, 0)
	counter.inputTokens = max(counter.inputTokens-inputTokens, 0)
	if counter.inFlight == 0 && counter.inputTokens == 0 {
		delete(l.models, model)
	}
	l.total.inFlight = max(l.total.inFlight-1, 0)
	l.total.inputTokens = max(l.total.inputTokens-inputTokens, 0)
}

func (l *GatewayLimiter) blockedReasonLocked(model string, global, perModel admission) string {
	if reason := global.blockedReason(l.total.inFlight, l.total.inputTokens); reason != "" {
		return reason
	}
	var inFlight, inputTokens int64
	if counter, ok := l.models[model]; ok {
		inFlight, inputTokens = counter.inFlight, counter.inputTokens
	}
	return perModel.blockedReason(inFlight, inputTokens)
}

func impossibleReason(global, perModel admission) string {
	if reason := global.impossible(); reason != "" {
		return reason
	}
	return perModel.impossible()
}

func (l *GatewayLimiter) counterLocked(model string) *modelCounter {
	counter, ok := l.models[model]
	if !ok {
		counter = &modelCounter{}
		l.models[model] = counter
	}
	return counter
}

type admission struct {
	concurrencyLimit   int64
	concurrencyLimited bool
	inputTokenLimit    int64
	inputTokenLimited  bool
	requestedTokens    int64
}

func (a admission) blockedReason(inFlight, inFlightTokens int64) string {
	if a.concurrencyLimited && inFlight+1 > a.concurrencyLimit {
		return reasonTooManyConcurrentRequests
	}
	if a.inputTokenLimited && inFlightTokens+a.requestedTokens > a.inputTokenLimit {
		return reasonTooManyInputTokens
	}
	return ""
}

func (a admission) impossible() string {
	return a.blockedReason(0, 0)
}

func effectiveConcurrencyLimit(baseMaxConcurrent int64, capacity ModelCapacity) (limit int64, limited bool) {
	if capacity.MaxConcurrentPer10000Weight > 0 && capacity.BaselineWeight > 0 {
		current := weightConcurrencyLimit(capacity.CurrentWeight, capacity.MaxConcurrentPer10000Weight)
		baseline := weightConcurrencyLimit(capacity.BaselineWeight, capacity.MaxConcurrentPer10000Weight)
		return min(current, baseline), true // current weight must never lift the cap above the baseline-derived one
	}
	if baseMaxConcurrent <= 0 {
		return 0, false
	}
	return scaleClamp(baseMaxConcurrent, capacity.ScaleFactor), true
}

func effectiveInputTokenLimit(baseMaxInputTokens int64, capacity ModelCapacity) (limit int64, limited bool) {
	if baseMaxInputTokens <= 0 {
		return 0, false
	}
	return scaleClamp(baseMaxInputTokens, capacity.ScaleFactor), true
}

func scaleClamp(base int64, scale float64) int64 {
	if base <= 0 {
		return 0
	}
	return int64(math.Floor(float64(base) * clampUnit(scale)))
}

func clampUnit(scale float64) float64 {
	switch {
	case math.IsNaN(scale): // fail closed: a corrupted scale must not grant unlimited capacity
		return 0
	case scale < 0:
		return 0
	case scale > 1:
		return 1
	default:
		return scale
	}
}
