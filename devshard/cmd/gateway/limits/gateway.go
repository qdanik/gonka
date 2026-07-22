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

type GatewayLimiter struct {
	mu     sync.Mutex
	cond   *sync.Cond
	cfg    GatewayConfig
	models map[string]*modelCounter
}

func NewGatewayLimiter(cfg GatewayConfig) *GatewayLimiter {
	l := &GatewayLimiter{cfg: cfg, models: map[string]*modelCounter{}}
	l.cond = sync.NewCond(&l.mu)
	return l
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
	defer l.mu.Unlock()

	baseMaxConcurrent, baseMaxInputTokens := l.limitsForModel(model)
	concurrencyLimit, concurrencyLimited := effectiveConcurrencyLimit(baseMaxConcurrent, capacity)
	inputTokenLimit, inputTokenLimited := effectiveInputTokenLimit(baseMaxInputTokens, capacity)
	adm := admission{
		concurrencyLimit:   concurrencyLimit,
		concurrencyLimited: concurrencyLimited,
		inputTokenLimit:    inputTokenLimit,
		inputTokenLimited:  inputTokenLimited,
		requestedTokens:    inputTokens,
	}

	if err := l.admitLocked(ctx, model, adm); err != nil {
		return err
	}

	counter := l.counterLocked(model)
	counter.inFlight++
	counter.inputTokens += inputTokens
	return nil
}

func (l *GatewayLimiter) ReleaseForModel(model string, inputTokens int64) {
	if inputTokens <= 0 {
		inputTokens = 1
	}
	model = strings.TrimSpace(model)

	l.mu.Lock()
	defer l.mu.Unlock()
	if counter, ok := l.models[model]; ok {
		counter.inFlight = max(counter.inFlight-1, 0)
		counter.inputTokens = max(counter.inputTokens-inputTokens, 0)
		if counter.inFlight == 0 && counter.inputTokens == 0 {
			delete(l.models, model)
		}
	}
	l.cond.Broadcast()
}

func (l *GatewayLimiter) admitLocked(ctx context.Context, model string, adm admission) error {
	if reason := adm.impossible(); reason != "" {
		return &RateLimitError{Reason: reason, RetryAfter: l.cfg.AcquireWait}
	}

	reason := l.blockedReasonLocked(model, adm)
	if reason == "" {
		return nil
	}
	if l.cfg.AcquireWait <= 0 {
		return &RateLimitError{Reason: reason, RetryAfter: l.cfg.AcquireWait}
	}

	deadline := time.Now().Add(l.cfg.AcquireWait)
	stop := make(chan struct{})
	defer close(stop)
	go l.broadcastAt(ctx, deadline, stop)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if reason = l.blockedReasonLocked(model, adm); reason == "" {
			return nil
		}
		if !time.Now().Before(deadline) {
			return &RateLimitError{Reason: reason, RetryAfter: l.cfg.AcquireWait}
		}
		l.cond.Wait()
	}
}

func (l *GatewayLimiter) blockedReasonLocked(model string, adm admission) string {
	var inFlight, inputTokens int64
	if counter, ok := l.models[model]; ok {
		inFlight, inputTokens = counter.inFlight, counter.inputTokens
	}
	return adm.blockedReason(inFlight, inputTokens)
}

// mu before Broadcast avoids racing a waiter that hasn't reached cond.Wait yet (a lost wakeup).
func (l *GatewayLimiter) broadcastAt(ctx context.Context, deadline time.Time, stop <-chan struct{}) {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-timer.C:
		l.lockedBroadcast()
	case <-ctx.Done():
		l.lockedBroadcast()
	case <-stop:
	}
}

func (l *GatewayLimiter) lockedBroadcast() {
	l.mu.Lock()
	l.cond.Broadcast()
	l.mu.Unlock()
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

func (l *GatewayLimiter) limitsForModel(model string) (maxConcurrent, maxInputTokens int64) {
	maxConcurrent, maxInputTokens = l.cfg.MaxConcurrent, l.cfg.MaxInputTokens
	override, ok := l.cfg.ModelLimits[model]
	if !ok {
		return maxConcurrent, maxInputTokens
	}
	if override.MaxConcurrent != nil {
		maxConcurrent = *override.MaxConcurrent
	}
	if override.MaxInputTokens != nil {
		maxInputTokens = *override.MaxInputTokens
	}
	return maxConcurrent, maxInputTokens
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
