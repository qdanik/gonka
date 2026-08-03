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
	reasonQueueTooDeep              = "queue is deeper than the wait budget can clear"
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
	MaxConcurrent     int64
	MaxInputTokens    int64
	AcquireWait       time.Duration
	QueueDepthPerSlot int64
	ModelLimits       map[string]ModelOverride
}

type modelCounter struct {
	inFlight    int64
	inputTokens int64
	enforced    admission
}

// waiter is one blocked Acquire; reason is why it had to queue and is what a timed-out wait reports.
// See gateway-capacity-and-health.md, "The queue".
type waiter struct {
	model    string
	tokens   int64
	capacity ModelCapacity
	reason   string
	ready    chan struct{}
}

// GatewayLimiter is the gateway-wide FIFO admission limiter. Each model records the admission it was
// last judged against, because overrides and capacity weights make the cap a per-model answer.
type GatewayLimiter struct {
	mu     sync.Mutex
	cfg    GatewayConfig
	models map[string]*modelCounter
	total  modelCounter
	queue  []*waiter
}

// InFlight is what one scope of the limiter currently holds.
type InFlight struct {
	Requests    int64
	InputTokens int64
	QueueDepth  int
}

// Enforced is the pair a model's requests are actually judged against, after its own overrides and its
// own capacity weights. There is no gateway-wide answer: two models rarely share one cap.
type Enforced struct {
	MaxConcurrentRequests  int64
	MaxInputTokensInFlight int64
}

type LimiterSnapshot struct {
	Total                            InFlight
	ByModel                          map[string]InFlight
	EnforcedByModel                  map[string]Enforced
	ConfiguredMaxConcurrentRequests  int64
	ConfiguredMaxInputTokensInFlight int64
}

func NewGatewayLimiter(cfg GatewayConfig) *GatewayLimiter {
	return &GatewayLimiter{cfg: cfg, models: map[string]*modelCounter{}}
}

// Reconfigure replaces the caps every later admission is judged against and sweeps the queue.
// See gateway-capacity-and-health.md, "The queue".
func (l *GatewayLimiter) Reconfigure(cfg GatewayConfig) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cfg = cfg
	l.promoteLocked()
}

func (l *GatewayLimiter) Snapshot() LimiterSnapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	snapshot := LimiterSnapshot{
		Total:                            InFlight{Requests: l.total.inFlight, InputTokens: l.total.inputTokens},
		ByModel:                          make(map[string]InFlight, len(l.models)),
		EnforcedByModel:                  make(map[string]Enforced, len(l.models)),
		ConfiguredMaxConcurrentRequests:  l.cfg.MaxConcurrent,
		ConfiguredMaxInputTokensInFlight: l.cfg.MaxInputTokens,
	}
	for model, counter := range l.models {
		snapshot.ByModel[model] = InFlight{Requests: counter.inFlight, InputTokens: counter.inputTokens}
		snapshot.EnforcedByModel[model] = Enforced{
			MaxConcurrentRequests:  counter.enforced.concurrencyLimit,
			MaxInputTokensInFlight: counter.enforced.inputTokenLimit,
		}
	}
	for _, blocked := range l.queue {
		queued := snapshot.ByModel[blocked.model]
		queued.QueueDepth++
		snapshot.ByModel[blocked.model] = queued
		snapshot.Total.QueueDepth++
	}
	return snapshot
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
	acquireWait := l.cfg.AcquireWait
	admitted := l.admissionFor(model, inputTokens, capacity)
	l.counterLocked(model).enforced = admitted
	if reason := admitted.impossible(); reason != "" {
		l.mu.Unlock()
		return &RateLimitError{Reason: reason, RetryAfter: acquireWait}
	}
	// A queued waiter is one that capacity currently cannot serve, so admitting a request that fits
	// does not overtake it: a freed slot is handed to the queue directly, under this same lock.
	reason := l.blockedReasonLocked(model, admitted)
	if reason == "" {
		l.takeLocked(model, inputTokens)
		l.mu.Unlock()
		return nil
	}
	if acquireWait <= 0 {
		l.mu.Unlock()
		return &RateLimitError{Reason: reason, RetryAfter: acquireWait}
	}

	if l.queueTooDeepLocked(model, admitted) {
		l.mu.Unlock()
		return &RateLimitError{Reason: reasonQueueTooDeep, RetryAfter: acquireWait}
	}

	blocked := &waiter{model: model, tokens: inputTokens, capacity: capacity, reason: reason, ready: make(chan struct{})}
	l.queue = append(l.queue, blocked)
	l.promoteLocked() // capacity may already be free; the queue only enforces order, it is not a delay
	l.mu.Unlock()

	timer := time.NewTimer(acquireWait)
	defer timer.Stop()
	select {
	case <-blocked.ready:
		return nil
	case <-timer.C:
		if !l.dequeue(blocked) {
			return nil // promoted as the deadline fired: the slot is already ours
		}
		return &RateLimitError{Reason: blocked.reason, RetryAfter: acquireWait}
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

// promoteLocked hands freed capacity to queued waiters in arrival order, skipping — rather than stopping
// at — a waiter its own model still cannot serve. See gateway-capacity-and-health.md, "The queue".
func (l *GatewayLimiter) promoteLocked() {
	for i := 0; i < len(l.queue); {
		waiting := l.queue[i]
		admitted := l.admissionFor(waiting.model, waiting.tokens, waiting.capacity)
		l.counterLocked(waiting.model).enforced = admitted
		if l.blockedReasonLocked(waiting.model, admitted) != "" {
			i++
			continue
		}
		l.queue = append(l.queue[:i], l.queue[i+1:]...)
		l.takeLocked(waiting.model, waiting.tokens)
		close(waiting.ready)
	}
}

// queueTooDeepLocked reports a queue this request cannot reach the front of in time. See
// gateway-capacity-and-health.md, "The wait budget".
func (l *GatewayLimiter) queueTooDeepLocked(model string, admitted admission) bool {
	perSlot := l.cfg.QueueDepthPerSlot
	if perSlot <= 0 || admitted.concurrencyLimit <= 0 {
		return false
	}
	queued := int64(0)
	for _, waiting := range l.queue {
		if waiting.model == model {
			queued++
		}
	}
	return queued >= admitted.concurrencyLimit*perSlot
}

func (l *GatewayLimiter) takeLocked(model string, inputTokens int64) {
	counter := l.counterLocked(model)
	counter.inFlight++
	counter.inputTokens += inputTokens
	l.total.inFlight++
	l.total.inputTokens += inputTokens
}

// admissionFor computes one model's caps: the configured maxima are each model's own budget, and an
// override replaces the configured maximum for its model rather than narrowing it further.
// See gateway-capacity-and-health.md, "The gateway limiter".
func (l *GatewayLimiter) admissionFor(model string, inputTokens int64, capacity ModelCapacity) admission {
	maxConcurrent, maxInputTokens := l.cfg.MaxConcurrent, l.cfg.MaxInputTokens
	if override, ok := l.cfg.ModelLimits[model]; ok {
		if override.MaxConcurrent != nil {
			maxConcurrent = *override.MaxConcurrent
		}
		if override.MaxInputTokens != nil {
			maxInputTokens = *override.MaxInputTokens
		}
	}
	admitted := admission{requestedTokens: inputTokens}
	admitted.concurrencyLimit, admitted.concurrencyLimited = effectiveConcurrencyLimit(maxConcurrent, capacity)
	admitted.inputTokenLimit, admitted.inputTokenLimited = effectiveInputTokenLimit(maxInputTokens, capacity)
	return admitted
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

// releaseLocked leaves an idle counter in place. Deleting it cost an allocation on the next acquire for
// the same model, and a model that goes quiet now reads zero in the snapshot instead of disappearing,
// which a reader cannot tell apart from the gateway being gone. The map is bounded by routable models.
func (l *GatewayLimiter) releaseLocked(model string, inputTokens int64) {
	counter, ok := l.models[model]
	if !ok {
		return
	}
	counter.inFlight = max(counter.inFlight-1, 0)
	counter.inputTokens = max(counter.inputTokens-inputTokens, 0)
	l.total.inFlight = max(l.total.inFlight-1, 0)
	l.total.inputTokens = max(l.total.inputTokens-inputTokens, 0)
}

func (l *GatewayLimiter) blockedReasonLocked(model string, admitted admission) string {
	var inFlight, inputTokens int64
	if counter, ok := l.models[model]; ok {
		inFlight, inputTokens = counter.inFlight, counter.inputTokens
	}
	return admitted.blockedReason(inFlight, inputTokens)
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

// scaleClamp rounds to nearest rather than flooring. See gateway-capacity-and-health.md, "The gateway limiter".
func scaleClamp(base int64, scale float64) int64 {
	if base <= 0 {
		return 0
	}
	return min(int64(math.Round(float64(base)*clampUnit(scale))), base)
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
