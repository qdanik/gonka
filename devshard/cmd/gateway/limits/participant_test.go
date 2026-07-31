package limits

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
)

var testEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func fixedNow(instant time.Time) func() time.Time {
	return func() time.Time { return instant }
}

// movingClock lets a test advance time between calls to cross a breaker's openUntil deterministically.
type movingClock struct {
	mu sync.Mutex
	t  time.Time
}

func newMovingClock(start time.Time) *movingClock {
	return &movingClock{t: start}
}

func (c *movingClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *movingClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func zeroJitter(time.Duration) time.Duration { return 0 }

func testConfig() ParticipantConfig {
	return ParticipantConfig{
		InitialWindow: 4,
		MaxWindow:     64,
		TripThreshold: 3,
		BaseOpen:      1 * time.Second,
		MaxOpen:       10 * time.Second,
	}
}

// newTestLimiter builds a limiter with deterministic (zero) jitter so backoff assertions are exact.
func newTestLimiter(cfg ParticipantConfig, now func() time.Time) *ParticipantLimiter {
	l := NewParticipantLimiter(cfg, now)
	l.jitter = zeroJitter
	return l
}

func withinTolerance(got, want time.Duration) bool {
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Millisecond
}

func TestAcquireAdmitsUpToWindowThenBlocks(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	for i := 0; i < 4; i++ {
		if !l.Acquire("p", "m") {
			t.Fatalf("Acquire() call %d = false, want true (InitialWindow=4)", i+1)
		}
	}
	if l.Acquire("p", "m") {
		t.Fatal("Acquire() at inflight==window = true, want false")
	}
}

func TestReleaseAllowsAnotherAcquire(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	for i := 0; i < 4; i++ {
		l.Acquire("p", "m")
	}

	l.Release("p", "m")

	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() after Release() freed a slot = false, want true")
	}
}

func TestReleaseFloorsAtZero(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	l.Release("p", "m") // never acquired
	l.Release("p", "m")

	for i := 0; i < 4; i++ {
		if !l.Acquire("p", "m") {
			t.Fatalf("Acquire() call %d after spurious Release()s = false, want true", i+1)
		}
	}
	if l.Acquire("p", "m") {
		t.Fatal("Acquire() at inflight==window after spurious Release()s = true, want false (Release must floor at 0, not go negative)")
	}
}

func TestSuccessBelowUtilizationGateLeavesWindowUnchanged(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.Acquire("p", "m") // inflight=1, window/2=2: gate not met

	l.OnResult("p", "m", Success)

	if got := l.states[key{participant: "p", model: "m"}].window; got != 4 {
		t.Fatalf("window after sub-gate Success = %v, want unchanged 4", got)
	}
}

func TestSuccessAtUtilizationGateIncrementsWindow(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.Acquire("p", "m")
	l.Acquire("p", "m") // inflight=2, window/2=2: gate met

	l.OnResult("p", "m", Success)

	if got := l.states[key{participant: "p", model: "m"}].window; got != 5 {
		t.Fatalf("window after at-gate Success = %v, want 5", got)
	}
}

func TestSuccessCapsAtMaxWindow(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.Acquire("p", "m")
	state := l.states[key{participant: "p", model: "m"}]
	state.window = 64
	state.inflight = 40 // comfortably above window/2: gate met

	l.OnResult("p", "m", Success)

	if state.window != 64 {
		t.Fatalf("window after Success at MaxWindow = %v, want capped at 64", state.window)
	}
}

func TestOverloadHalvesWindow(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.Acquire("p", "m")

	l.OnResult("p", "m", Overload)

	if got := l.states[key{participant: "p", model: "m"}].window; got != 2 {
		t.Fatalf("window after Overload = %v, want 2 (4*0.5)", got)
	}
}

func TestOverloadFloorsWindowAtOne(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.InitialWindow = 1
	l := newTestLimiter(cfg, fixedNow(testEpoch))
	l.Acquire("p", "m")

	l.OnResult("p", "m", Overload)

	if got := l.states[key{participant: "p", model: "m"}].window; got != 1 {
		t.Fatalf("window after Overload at floor = %v, want floor 1", got)
	}
}

func TestOverloadNeverTripsBreaker(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	for i := 0; i < 10; i++ {
		l.Acquire("p", "m")
		l.OnResult("p", "m", Overload)
		l.Release("p", "m")
	}

	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() after repeated Overload verdicts = false, want true (breaker must stay closed)")
	}
}

func TestTransportFaultTripsAtExactThreshold(t *testing.T) {
	t.Parallel()
	cfg := testConfig() // TripThreshold=3
	l := newTestLimiter(cfg, fixedNow(testEpoch))

	for i := int64(0); i < cfg.TripThreshold-1; i++ {
		l.OnResult("p", "m", TransportFault)
	}
	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() before TripThreshold reached = false, want true (breaker not yet open)")
	}
	l.Release("p", "m")

	l.OnResult("p", "m", TransportFault) // the TripThreshold-th consecutive fault

	if l.Acquire("p", "m") {
		t.Fatal("Acquire() at TripThreshold = true, want false (breaker must open)")
	}
}

func TestTransportFaultBackoffLadderGrowsThenCaps(t *testing.T) {
	t.Parallel()
	cfg := ParticipantConfig{
		InitialWindow: 4,
		MaxWindow:     64,
		TripThreshold: 1,
		BaseOpen:      1 * time.Second,
		MaxOpen:       3 * time.Second,
	}
	l := newTestLimiter(cfg, fixedNow(testEpoch))

	want := []time.Duration{
		1 * time.Second, // base * 1.6^0
		time.Duration(1.6 * float64(time.Second)),  // base * 1.6^1 = 1.6s
		time.Duration(2.56 * float64(time.Second)), // base * 1.6^2 = 2.56s
		3 * time.Second, // base * 1.6^3 = 4.096s, capped at MaxOpen
	}
	for i, wantBackoff := range want {
		l.OnResult("p", "m", TransportFault) // TripThreshold=1: every call re-trips
		got := l.states[key{participant: "p", model: "m"}].openUntil.Sub(testEpoch)
		if !withinTolerance(got, wantBackoff) {
			t.Fatalf("trip %d: openUntil-now = %v, want %v", i+1, got, wantBackoff)
		}
	}
}

func TestHalfOpenAllowsExactlyOneProbeAfterCooldown(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for i := int64(0); i < cfg.TripThreshold; i++ {
		l.OnResult("p", "m", TransportFault)
	}
	if l.Acquire("p", "m") {
		t.Fatal("Acquire() immediately after trip = true, want false (breaker open)")
	}

	state := l.states[key{participant: "p", model: "m"}]
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)

	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() after cooldown elapsed = false, want true (half-open probe)")
	}
	if l.Acquire("p", "m") {
		t.Fatal("second concurrent Acquire() during half-open = true, want false (exactly one probe allowed)")
	}
}

func TestHalfOpenSuccessClosesBreakerAndDecaysBackoff(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for i := int64(0); i < cfg.TripThreshold; i++ {
		l.OnResult("p", "m", TransportFault)
	}
	state := l.states[key{participant: "p", model: "m"}]
	backoffCountAfterTrip := state.backoffCount
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)
	l.Acquire("p", "m") // admits the half-open probe

	l.OnResult("p", "m", Success)

	if state.halfOpen {
		t.Fatal("halfOpen after probe Success = true, want false (breaker closed)")
	}
	if !state.openUntil.IsZero() {
		t.Fatalf("openUntil after probe Success = %v, want zero (breaker fully closed)", state.openUntil)
	}
	if state.backoffCount != backoffCountAfterTrip-1 {
		t.Fatalf("backoffCount after probe Success = %d, want %d (decayed by one)", state.backoffCount, backoffCountAfterTrip-1)
	}
	l.Release("p", "m")
	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() after breaker closed = false, want true")
	}
}

func TestHalfOpenTransportFaultReopensWithLongerCooldown(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for i := int64(0); i < cfg.TripThreshold; i++ {
		l.OnResult("p", "m", TransportFault)
	}
	state := l.states[key{participant: "p", model: "m"}]
	firstCooldown := state.openUntil.Sub(clock.now())
	clock.advance(firstCooldown + time.Millisecond)
	l.Acquire("p", "m") // half-open probe admitted
	probeTime := clock.now()

	l.OnResult("p", "m", TransportFault) // the probe itself fails: a single fault, not TripThreshold-many

	if state.halfOpen {
		t.Fatal("halfOpen after failed probe = true, want false (fully reopened)")
	}
	secondCooldown := state.openUntil.Sub(probeTime)
	if secondCooldown <= firstCooldown {
		t.Fatalf("second cooldown = %v, want longer than first cooldown %v", secondCooldown, firstCooldown)
	}
	if l.Acquire("p", "m") {
		t.Fatal("Acquire() right after re-opening = true, want false")
	}
}

func TestModelOutcomeNeverPenalizes(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.Acquire("p", "m")

	for i := 0; i < 20; i++ {
		l.OnResult("p", "m", ModelOutcome)
	}

	state, exists := l.states[key{participant: "p", model: "m"}]
	if !exists {
		t.Fatal("state missing after Acquire")
	}
	if state.window != 4 {
		t.Fatalf("window after ModelOutcome verdicts = %v, want unchanged 4", state.window)
	}
	if !state.openUntil.IsZero() || state.halfOpen || state.consecutiveTransportFail != 0 || state.backoffCount != 0 {
		t.Fatal("breaker state moved after ModelOutcome verdicts, want fully unchanged")
	}
}

func TestModelOutcomeWithoutPriorAcquireCreatesNoState(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	l.OnResult("never-acquired", "m", ModelOutcome)

	if _, exists := l.states[key{participant: "never-acquired", model: "m"}]; exists {
		t.Fatal("ModelOutcome created state for a host never Acquired, want a true no-op")
	}
}

func TestParticipantLimiterConcurrentAccessIsRaceFree(t *testing.T) {
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	const goroutines = 50
	const iterationsPerGoroutine = 200
	participants := []string{"p1", "p2", "p3"}
	models := []string{"m1", "m2"}
	verdicts := []Verdict{Success, Overload, TransportFault, ModelOutcome}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			source := rand.New(rand.NewSource(seed))
			for i := 0; i < iterationsPerGoroutine; i++ {
				participant := participants[source.Intn(len(participants))]
				model := models[source.Intn(len(models))]
				if l.Acquire(participant, model) {
					l.OnResult(participant, model, verdicts[source.Intn(len(verdicts))])
					l.Release(participant, model)
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

// The breaker's cooldown must stay strictly below perf's ejection horizon so perf
// remains the pool authority (BOUNDARY DECISION 1 in the phase-5 plan), not the breaker.
func TestBreakerMaxOpenDefaultStaysBelowPerfEjectionHorizon(t *testing.T) {
	t.Parallel()
	defaults := config.Defaults()
	breakerMaxOpen := time.Duration(defaults.Limits.Breaker.MaxOpenMS) * time.Millisecond
	perfEjectionMax := time.Duration(defaults.Perf.EjectionMaxSeconds) * time.Second

	if breakerMaxOpen >= perfEjectionMax {
		t.Fatalf("breaker MaxOpenMS default %v must stay below perf's ejection horizon %v", breakerMaxOpen, perfEjectionMax)
	}
}

func TestAvailableTrueOnFreshParticipant(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	if !l.Available("p", "m") {
		t.Fatal("Available() on a never-seen participant = false, want true (fresh full window)")
	}
	if _, exists := l.states[key{participant: "p", model: "m"}]; exists {
		t.Fatal("Available() on a never-seen participant created state, want no-op")
	}
}

func TestAvailableFalseWhileBreakerOpenThenTrueAfterCooldown(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for i := int64(0); i < cfg.TripThreshold; i++ {
		l.OnResult("p", "m", TransportFault)
	}
	if l.Available("p", "m") {
		t.Fatal("Available() while breaker Open = true, want false")
	}

	state := l.states[key{participant: "p", model: "m"}]
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)

	if !l.Available("p", "m") {
		t.Fatal("Available() after cooldown elapsed = false, want true (half-open probe available)")
	}
	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() after Available() reported half-open = false, want true (peek must not consume the probe)")
	}
}

func TestAvailableFalseDuringHalfOpenProbeInFlight(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for i := int64(0); i < cfg.TripThreshold; i++ {
		l.OnResult("p", "m", TransportFault)
	}
	state := l.states[key{participant: "p", model: "m"}]
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)
	if !l.Acquire("p", "m") {
		t.Fatal("Acquire() for the half-open probe = false, want true")
	}

	if l.Available("p", "m") {
		t.Fatal("Available() with the half-open probe in flight (inflight==window==1) = true, want false")
	}
}

func TestAvailableFalseAtWindowThenTrueAfterRelease(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	for i := 0; i < 4; i++ {
		l.Acquire("p", "m")
	}
	if l.Available("p", "m") {
		t.Fatal("Available() at inflight==window = true, want false")
	}

	l.Release("p", "m")

	if !l.Available("p", "m") {
		t.Fatal("Available() after Release() freed a slot = false, want true")
	}
}

func TestAvailableDoesNotConsumeWindowSlots(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	l := newTestLimiter(cfg, fixedNow(testEpoch))

	for i := 0; i < 10; i++ {
		l.Available("p", "m")
	}
	if _, exists := l.states[key{participant: "p", model: "m"}]; exists {
		t.Fatal("Available() created participant state as a side effect, want no-op until first Acquire")
	}

	for i := 0; i < int(cfg.InitialWindow); i++ {
		if !l.Acquire("p", "m") {
			t.Fatalf("Acquire() call %d after repeated Available() peeks = false, want true (peeks must not consume slots)", i+1)
		}
	}
	if l.Acquire("p", "m") {
		t.Fatal("Acquire() beyond window after repeated Available() peeks = true, want false")
	}
}

func TestAvailableLeavesExistingStateUnchanged(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.Acquire("p", "m")
	l.OnResult("p", "m", TransportFault)
	before := *l.states[key{participant: "p", model: "m"}]

	for i := 0; i < 10; i++ {
		l.Available("p", "m")
	}

	after := *l.states[key{participant: "p", model: "m"}]
	if before != after {
		t.Fatalf("Available() mutated existing state: before=%+v after=%+v", before, after)
	}
}

func TestAvailableConcurrentWithAcquireIsRaceFree(t *testing.T) {
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	const goroutines = 50
	const iterationsPerGoroutine = 200
	participants := []string{"p1", "p2", "p3"}
	models := []string{"m1", "m2"}
	verdicts := []Verdict{Success, Overload, TransportFault, ModelOutcome}

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			source := rand.New(rand.NewSource(seed))
			for i := 0; i < iterationsPerGoroutine; i++ {
				participant := participants[source.Intn(len(participants))]
				model := models[source.Intn(len(models))]
				l.Available(participant, model)
				if l.Acquire(participant, model) {
					l.OnResult(participant, model, verdicts[source.Intn(len(verdicts))])
					l.Release(participant, model)
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

func TestClearQuarantineReopensEveryModelsBreakerForOneParticipant(t *testing.T) {
	now := time.Now()
	limiter := NewParticipantLimiter(ParticipantConfig{
		InitialWindow: 4, MaxWindow: 8, TripThreshold: 1,
		BaseOpen: time.Minute, MaxOpen: time.Minute,
	}, func() time.Time { return now })
	limiter.jitter = func(time.Duration) time.Duration { return 0 }

	limiter.OnResult("host-a", "model-a", TransportFault)
	limiter.OnResult("host-a", "model-b", TransportFault)
	limiter.OnResult("host-b", "model-a", TransportFault)
	if limiter.Available("host-a", "model-a") {
		t.Fatal("host-a/model-a is available before the quarantine is cleared")
	}

	if !limiter.ClearQuarantine("host-a") {
		t.Fatal("ClearQuarantine(host-a) = false, want true for a tracked participant")
	}
	if !limiter.Available("host-a", "model-a") || !limiter.Available("host-a", "model-b") {
		t.Fatal("host-a is still unavailable after its quarantine was cleared")
	}
	if limiter.Available("host-b", "model-a") {
		t.Fatal("host-b was reopened by clearing host-a's quarantine")
	}
	if limiter.ClearQuarantine("host-never-seen") {
		t.Fatal("ClearQuarantine(unknown) = true, want false")
	}
}
