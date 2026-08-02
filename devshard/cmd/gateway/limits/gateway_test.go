package limits

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/internal/leakcheck"
)

func fullScale() ModelCapacity { return ModelCapacity{ScaleFactor: 1} }

func int64Ptr(v int64) *int64 { return &v }

func TestAcquireForModel_AdmitsUnderCap(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 2, AcquireWait: time.Second})

	if err := limiter.AcquireForModel(context.Background(), "modelA", 10, fullScale()); err != nil {
		t.Fatalf("first AcquireForModel: %v", err)
	}
	if err := limiter.AcquireForModel(context.Background(), "modelA", 10, fullScale()); err != nil {
		t.Fatalf("second AcquireForModel: %v", err)
	}
}

func TestAcquireForModel_InputTokenBudgetEnforced(t *testing.T) {
	t.Parallel()
	wait := 60 * time.Millisecond
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 100, MaxInputTokens: 100, AcquireWait: wait})

	if err := limiter.AcquireForModel(context.Background(), "modelA", 80, fullScale()); err != nil {
		t.Fatalf("first AcquireForModel: %v", err)
	}

	start := time.Now()
	err := limiter.AcquireForModel(context.Background(), "modelA", 50, fullScale())
	elapsed := time.Since(start)

	var rateLimitErr *RateLimitError
	if !errors.As(err, &rateLimitErr) {
		t.Fatalf("second AcquireForModel error = %v, want *RateLimitError", err)
	}
	if rateLimitErr.Reason != reasonTooManyInputTokens {
		t.Errorf("Reason = %q, want %q", rateLimitErr.Reason, reasonTooManyInputTokens)
	}
	if rateLimitErr.RetryAfter != wait {
		t.Errorf("RetryAfter = %v, want %v", rateLimitErr.RetryAfter, wait)
	}
	if elapsed < wait {
		t.Errorf("elapsed = %v, want >= AcquireWait %v (should wait the full budget before rejecting)", elapsed, wait)
	}
}

func TestAcquireForModel_BlocksThenAdmitsWhenSlotFreesWithinWait(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1, AcquireWait: time.Second})

	if err := limiter.AcquireForModel(context.Background(), "modelA", 1, fullScale()); err != nil {
		t.Fatalf("initial AcquireForModel: %v", err)
	}

	go func() {
		time.Sleep(80 * time.Millisecond)
		limiter.ReleaseForModel("modelA", 1)
	}()

	start := time.Now()
	err := limiter.AcquireForModel(context.Background(), "modelA", 1, fullScale())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("AcquireForModel after release = %v, want nil", err)
	}
	if elapsed < 30*time.Millisecond {
		t.Errorf("elapsed = %v, want at least ~30ms of blocking before the release freed a slot", elapsed)
	}
	if elapsed >= time.Second {
		t.Errorf("elapsed = %v, want well under the 1s AcquireWait", elapsed)
	}
}

func TestSnapshot_CountsAWaiterAgainstItsModelsQueue(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1, MaxInputTokens: 100, AcquireWait: time.Second})

	if err := limiter.AcquireForModel(context.Background(), "modelA", 10, fullScale()); err != nil {
		t.Fatalf("initial AcquireForModel: %v", err)
	}
	queued := make(chan error, 1)
	go func() { queued <- limiter.AcquireForModel(context.Background(), "modelA", 10, fullScale()) }()

	waitFor(t, func() bool { return limiter.Snapshot().Total.QueueDepth == 1 })
	snapshot := limiter.Snapshot()
	if got := snapshot.ByModel["modelA"]; got.Requests != 1 || got.InputTokens != 10 || got.QueueDepth != 1 {
		t.Fatalf("ByModel[modelA] = %+v, want 1 request, 10 tokens, 1 queued", got)
	}
	if snapshot.EffectiveMaxConcurrentRequests != 1 || snapshot.EffectiveMaxInputTokensInFlight != 100 {
		t.Fatalf("effective caps = %d/%d, want 1/100", snapshot.EffectiveMaxConcurrentRequests, snapshot.EffectiveMaxInputTokensInFlight)
	}

	limiter.ReleaseForModel("modelA", 10)
	if err := <-queued; err != nil {
		t.Fatalf("queued AcquireForModel = %v, want nil", err)
	}
	if got := limiter.Snapshot().Total.QueueDepth; got != 0 {
		t.Fatalf("queue depth after promotion = %d, want 0", got)
	}
	limiter.ReleaseForModel("modelA", 10)
}

func waitFor(t *testing.T, holds func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !holds() {
		if time.Now().After(deadline) {
			t.Fatal("condition never held")
		}
		runtime.Gosched()
	}
}

func TestAcquireForModel_TimesOutWhenNoSlotFrees(t *testing.T) {
	t.Parallel()
	wait := 60 * time.Millisecond
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1, AcquireWait: wait})

	if err := limiter.AcquireForModel(context.Background(), "modelA", 1, fullScale()); err != nil {
		t.Fatalf("initial AcquireForModel: %v", err)
	}

	start := time.Now()
	err := limiter.AcquireForModel(context.Background(), "modelA", 1, fullScale())
	elapsed := time.Since(start)

	var rateLimitErr *RateLimitError
	if !errors.As(err, &rateLimitErr) {
		t.Fatalf("AcquireForModel error = %v, want *RateLimitError", err)
	}
	if rateLimitErr.Reason != reasonTooManyConcurrentRequests {
		t.Errorf("Reason = %q, want %q", rateLimitErr.Reason, reasonTooManyConcurrentRequests)
	}
	if rateLimitErr.RetryAfter != wait {
		t.Errorf("RetryAfter = %v, want %v", rateLimitErr.RetryAfter, wait)
	}
	if elapsed < wait {
		t.Errorf("elapsed = %v, want >= AcquireWait %v", elapsed, wait)
	}
	if elapsed > wait+2*time.Second {
		t.Errorf("elapsed = %v, want reasonably close to AcquireWait %v", elapsed, wait)
	}
}

func TestAcquireForModel_CtxCancelWakesPromptly(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1, AcquireWait: time.Second})

	if err := limiter.AcquireForModel(context.Background(), "modelA", 1, fullScale()); err != nil {
		t.Fatalf("initial AcquireForModel: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(25 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- limiter.AcquireForModel(ctx, "modelA", 1, fullScale()) }()

	select {
	case err := <-done:
		elapsed := time.Since(start)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("AcquireForModel error = %v, want context.Canceled", err)
		}
		if elapsed >= time.Second {
			t.Errorf("elapsed = %v, want well under the 1s AcquireWait (ctx-cancel should wake it promptly)", elapsed)
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("AcquireForModel did not return within 400ms of ctx cancellation")
	}
}

func TestAcquireForModel_ContextAlreadyCancelled(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 5, AcquireWait: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := limiter.AcquireForModel(ctx, "modelA", 1, fullScale()); !errors.Is(err, context.Canceled) {
		t.Fatalf("AcquireForModel with pre-cancelled ctx = %v, want context.Canceled", err)
	}
}

func TestAcquireForModel_ScaleFactorZeroBlocksImmediately(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 5, AcquireWait: time.Second})

	start := time.Now()
	err := limiter.AcquireForModel(context.Background(), "modelA", 1, ModelCapacity{ScaleFactor: 0})
	elapsed := time.Since(start)

	var rateLimitErr *RateLimitError
	if !errors.As(err, &rateLimitErr) {
		t.Fatalf("AcquireForModel error = %v, want *RateLimitError", err)
	}
	if rateLimitErr.Reason != reasonTooManyConcurrentRequests {
		t.Errorf("Reason = %q, want %q", rateLimitErr.Reason, reasonTooManyConcurrentRequests)
	}
	if rateLimitErr.RetryAfter != time.Second {
		t.Errorf("RetryAfter = %v, want 1s (the configured AcquireWait)", rateLimitErr.RetryAfter)
	}
	if elapsed >= 200*time.Millisecond {
		t.Errorf("elapsed = %v, want near-instant rejection (scale can't change mid-wait, so waiting is pointless)", elapsed)
	}
}

func TestAcquireForModel_PerTenThousandWeightDynamicCap(t *testing.T) {
	t.Parallel()
	wait := 40 * time.Millisecond
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1000, AcquireWait: wait})
	capacity := ModelCapacity{
		CurrentWeight:               2000,
		BaselineWeight:              2000,
		MaxConcurrentPer10000Weight: 5, // weightConcurrencyLimit(2000, 5) = floor(2000*5/10000) = 1
	}

	if err := limiter.AcquireForModel(context.Background(), "modelA", 1, capacity); err != nil {
		t.Fatalf("first AcquireForModel: %v", err)
	}

	start := time.Now()
	err := limiter.AcquireForModel(context.Background(), "modelA", 1, capacity)
	elapsed := time.Since(start)

	var rateLimitErr *RateLimitError
	if !errors.As(err, &rateLimitErr) {
		t.Fatalf("second AcquireForModel error = %v, want *RateLimitError (dynamic cap of 1 already used)", err)
	}
	if elapsed < wait {
		t.Errorf("elapsed = %v, want >= AcquireWait %v", elapsed, wait)
	}
}

func TestAcquireForModel_DynamicCapClampsToBaselineWeight(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1000, AcquireWait: time.Millisecond})
	capacity := ModelCapacity{
		CurrentWeight:               5000, // above baseline
		BaselineWeight:              1000,
		MaxConcurrentPer10000Weight: 100, // current-derived = 50, baseline-derived = 10
	}

	for i := 0; i < 10; i++ {
		if err := limiter.AcquireForModel(context.Background(), "modelA", 1, capacity); err != nil {
			t.Fatalf("acquire %d = %v, want admitted under the baseline-derived cap of 10", i, err)
		}
	}
	if err := limiter.AcquireForModel(context.Background(), "modelA", 1, capacity); err == nil {
		t.Fatal("11th AcquireForModel = nil, want rejection: current weight must not lift the cap above the baseline-derived limit")
	}
}

func TestAcquireForModel_BaselineZeroIsUnlimited(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 0, AcquireWait: time.Millisecond})

	for i := 0; i < 50; i++ {
		if err := limiter.AcquireForModel(context.Background(), "modelA", 1, ModelCapacity{ScaleFactor: 0}); err != nil {
			t.Fatalf("acquire %d = %v, want nil (baseline 0 means unlimited even at scale 0)", i, err)
		}
	}
}

func TestAcquireForModel_PerModelOverride(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{
		MaxConcurrent: 10,
		AcquireWait:   30 * time.Millisecond,
		ModelLimits: map[string]ModelOverride{
			"tight-model": {MaxConcurrent: int64Ptr(1)},
		},
	})

	if err := limiter.AcquireForModel(context.Background(), "tight-model", 1, fullScale()); err != nil {
		t.Fatalf("first AcquireForModel(tight-model): %v", err)
	}

	start := time.Now()
	if err := limiter.AcquireForModel(context.Background(), "tight-model", 1, fullScale()); err == nil {
		t.Fatal("second AcquireForModel(tight-model) = nil, want rejection under the per-model override cap of 1")
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("elapsed = %v, want >= AcquireWait 30ms", elapsed)
	}

	for i := 0; i < 5; i++ {
		if err := limiter.AcquireForModel(context.Background(), "other-model", 1, fullScale()); err != nil {
			t.Fatalf("AcquireForModel(other-model) %d = %v, want nil (its own budget of 10, not the override)", i, err)
		}
	}
}

func TestReleaseForModel_UnknownModelIsNoop(t *testing.T) {
	t.Parallel()
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1})
	limiter.ReleaseForModel("never-acquired", 5)
}

func TestRateLimitError_Error(t *testing.T) {
	t.Parallel()
	err := &RateLimitError{Reason: reasonTooManyConcurrentRequests, RetryAfter: time.Second}
	want := "rate limit exceeded: too many concurrent requests"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestGatewayLimiter_ConcurrencyRace(t *testing.T) {
	defer leakcheck.VerifyNone(t)

	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 3, MaxInputTokens: 30, AcquireWait: 20 * time.Millisecond})

	const goroutines = 20
	const iterations = 25
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				err := limiter.AcquireForModel(ctx, "modelA", 3, fullScale())
				cancel()
				if err != nil {
					continue
				}
				limiter.ReleaseForModel("modelA", 3)
			}
		}()
	}
	wg.Wait()

	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	// The invariant is that every acquire was released, not that the entry was cleaned up: idle
	// counters are kept so a model that goes quiet reads zero instead of vanishing from the snapshot.
	if counter, held := limiter.models["modelA"]; held && (counter.inFlight != 0 || counter.inputTokens != 0) {
		t.Errorf("residual counter after race = %+v, want zero on both", counter)
	}
	if limiter.total.inFlight != 0 || limiter.total.inputTokens != 0 {
		t.Errorf("residual total after race = %+v, want zero on both", limiter.total)
	}
}

// The configured maximum is each model's own budget, as it was before the rewrite: an operator running
// two models configured headroom for each, not one budget they contend over.
func TestAcquireForModelGivesEachModelItsOwnBudget(t *testing.T) {
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1})
	capacity := ModelCapacity{ScaleFactor: 1}
	ctx := context.Background()

	if err := limiter.AcquireForModel(ctx, "model-a", 1, capacity); err != nil {
		t.Fatalf("acquire on model-a: %v", err)
	}
	if err := limiter.AcquireForModel(ctx, "model-b", 1, capacity); err != nil {
		t.Fatalf("acquire on model-b = %v, want nil: model-a's traffic must not spend model-b's budget", err)
	}

	var rateLimit *RateLimitError
	if err := limiter.AcquireForModel(ctx, "model-a", 1, capacity); !errors.As(err, &rateLimit) {
		t.Fatalf("second acquire on model-a = %v, want a rate-limit error: its own budget of 1 is spent", err)
	}
}

// A weight-derived cap is computed from one model's chain weight, so charging it against another
// model's traffic makes the enforced cap depend on which model's request happens to arrive first.
func TestAcquireForModelWeightDerivedCapIsNotSpentByAnotherModel(t *testing.T) {
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1000})
	ctx := context.Background()
	perModelCapacity := ModelCapacity{CurrentWeight: 4000, BaselineWeight: 4000, MaxConcurrentPer10000Weight: 5} // floor(4000*5/10000) = 2

	for i := 0; i < 2; i++ {
		if err := limiter.AcquireForModel(ctx, "model-a", 1, perModelCapacity); err != nil {
			t.Fatalf("acquire %d on model-a = %v, want nil under its weight-derived cap of 2", i, err)
		}
	}

	for i := 0; i < 2; i++ {
		if err := limiter.AcquireForModel(ctx, "model-b", 1, perModelCapacity); err != nil {
			t.Fatalf("acquire %d on model-b = %v, want nil: model-b's weight buys model-b two slots of its own", i, err)
		}
	}
	var rateLimit *RateLimitError
	if err := limiter.AcquireForModel(ctx, "model-b", 1, perModelCapacity); !errors.As(err, &rateLimit) {
		t.Fatalf("third acquire on model-b = %v, want a rate-limit error at its weight-derived cap of 2", err)
	}
}

// Flooring a cap of 1 at anything below full capacity yields 0, which refuses every request for the
// model rather than degrading it. The original rounded to nearest for exactly that reason.
func TestAcquireForModelSmallCapSurvivesPartialCapacity(t *testing.T) {
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1})

	if err := limiter.AcquireForModel(context.Background(), "model-a", 1, ModelCapacity{ScaleFactor: 0.5}); err != nil {
		t.Fatalf("acquire at half capacity under a cap of 1 = %v, want nil (rounded to 1, not floored to 0)", err)
	}
}

// waitForQueueLen polls the limiter's queue rather than sleeping, so the test is deterministic.
func waitForQueueLen(t *testing.T, limiter *GatewayLimiter, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		limiter.mu.Lock()
		got := len(limiter.queue)
		limiter.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue length = %d, want %d within 2s", got, want)
		}
		runtime.Gosched()
	}
}

// A freed slot goes to whoever queued first. Without this a steady arrival stream starves an
// existing waiter until its deadline expires, so it times out into a 429 while newer requests pass.
func TestAcquireForModelHandsFreedSlotToTheEarlierWaiter(t *testing.T) {
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1, AcquireWait: 2 * time.Second})
	capacity := ModelCapacity{ScaleFactor: 1}
	ctx := context.Background()

	if err := limiter.AcquireForModel(ctx, "model-a", 1, capacity); err != nil {
		t.Fatalf("initial acquire: %v", err)
	}

	earlier := make(chan error, 1)
	go func() { earlier <- limiter.AcquireForModel(ctx, "model-a", 1, capacity) }()
	waitForQueueLen(t, limiter, 1)

	later := make(chan error, 1)
	go func() { later <- limiter.AcquireForModel(ctx, "model-a", 1, capacity) }()
	waitForQueueLen(t, limiter, 2)

	limiter.ReleaseForModel("model-a", 1)

	select {
	case err := <-earlier:
		if err != nil {
			t.Fatalf("earlier waiter = %v, want admission", err)
		}
	case <-later:
		t.Fatal("the later arrival was admitted first; the earlier waiter can be starved past its deadline")
	case <-time.After(2 * time.Second):
		t.Fatal("no waiter was admitted after a slot was freed")
	}

	limiter.ReleaseForModel("model-a", 1)
	if err := <-later; err != nil {
		t.Fatalf("later waiter after a second release = %v, want admission", err)
	}
	limiter.ReleaseForModel("model-a", 1)
}

func TestReconfigureAppliesToTheNextAcquireAndReleasesTheQueue(t *testing.T) {
	limiter := NewGatewayLimiter(GatewayConfig{MaxConcurrent: 1, AcquireWait: 10 * time.Second})
	if err := limiter.AcquireForModel(context.Background(), "modelX", 1, fullScale()); err != nil {
		t.Fatalf("first AcquireForModel(): %v", err)
	}

	queued := make(chan error, 1)
	go func() { queued <- limiter.AcquireForModel(context.Background(), "modelX", 1, fullScale()) }()
	waitFor(t, func() bool { return limiter.Snapshot().Total.QueueDepth == 1 })

	limiter.Reconfigure(GatewayConfig{MaxConcurrent: 2, AcquireWait: 10 * time.Second})

	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("the queued request after a raised cap = %v, want admitted", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a raised cap never reached the queue: the waiter is still blocked")
	}
	if got := limiter.Snapshot().EffectiveMaxConcurrentRequests; got != 2 {
		t.Fatalf("EffectiveMaxConcurrentRequests = %d, want 2", got)
	}

	limiter.Reconfigure(GatewayConfig{MaxConcurrent: 1, AcquireWait: 0})
	var rateLimited *RateLimitError
	if err := limiter.AcquireForModel(context.Background(), "modelX", 1, fullScale()); !errors.As(err, &rateLimited) {
		t.Fatalf("AcquireForModel() after a lowered cap = %v, want a rate-limit error", err)
	}
}
