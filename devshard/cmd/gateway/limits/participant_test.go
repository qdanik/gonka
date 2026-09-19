package limits

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/config"
)

var testEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func fixedNow(instant time.Time) func() time.Time {
	return func() time.Time { return instant }
}

// movingClock lets a test advance time between calls to cross a cutoff's openUntil deterministically.
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

// oneToken prices a request at a single token on each dimension, so a window counted in tokens still
// reads as a count of requests and an expectation stays legible.
var oneToken = TokenCost{Input: 1, Output: 1}

func testConfig() ParticipantConfig {
	return ParticipantConfig{
		Pricing: WindowPricing{
			Input:                 RequestBounds{Min: 1, Initial: 4},
			Output:                RequestBounds{Min: 1, Initial: 4},
			FallbackContextTokens: 1,
			FallbackOutputTokens:  1,
		},
		Factors:       CongestionFactors{Soft: 0.85, Hard: 0.70, Severe: 0.50, Cross: 0.90},
		Slack:         0.30,
		AfterFailures: 3,
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

func (l *ParticipantLimiter) admitOne(participant, model string) (func(), bool) {
	release, admission := l.Acquire(participant, model, oneToken)
	return release, admission == AdmissionOpen
}

func (l *ParticipantLimiter) admits(participant, model string) bool {
	_, admitted := l.admitOne(participant, model)
	return admitted
}

// answered posts one verdict for a request that carried a single token on each dimension.
func (l *ParticipantLimiter) answered(participant, model string, verdict Verdict) {
	l.OnResult(Result{Participant: participant, Model: model, Verdict: verdict, Carried: oneToken})
}

func (l *ParticipantLimiter) windowsOf(t *testing.T, participant, model string) (input, output float64) {
	t.Helper()
	state, tracked := l.states[key{participant: participant, model: model}]
	if !tracked {
		t.Fatalf("participant %q is not tracked for model %q", participant, model)
	}
	return state.input.tokens, state.output.tokens
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

	for i := range 4 {
		if !l.admits("p", "m") {
			t.Fatalf("Acquire() call %d = false, want true (a four-token window takes four one-token requests)", i+1)
		}
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() with the window full = true, want false")
	}
}

func TestAHostWithNothingInFlightAdmitsARequestLargerThanItsWholeWindow(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	if _, admission := l.Acquire("p", "m", TokenCost{Input: 1_000_000, Output: 1_000_000}); admission != AdmissionOpen {
		t.Fatalf("Acquire() of a request larger than the window on an idle host = %s, want open: a prompt no window fits must not become one no host can ever serve", admission)
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() behind the oversized request = true, want false: the host is over its window until that one ends")
	}
}

func TestReleaseAllowsAnotherAcquire(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	release, _ := l.admitOne("p", "m")
	for range 3 {
		l.admitOne("p", "m")
	}

	release()

	if !l.admits("p", "m") {
		t.Fatal("Acquire() after a lease gave its tokens back = false, want true")
	}
}

func TestReleasingALeaseTwiceGivesBackOnlyWhatItTook(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	release, _ := l.admitOne("p", "m")
	for range 3 {
		l.admitOne("p", "m")
	}

	release()
	release()

	if !l.admits("p", "m") {
		t.Fatal("Acquire() after the released lease = false, want true")
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() beyond the window = true, want false: a lease released twice must not hand back tokens it never took")
	}
}

func TestSuccessBelowUtilizationGateLeavesWindowUnchanged(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m") // peak=1, window/2=2: gate not met

	l.OnResult(Result{Participant: "p", Model: "m", Verdict: Success, Carried: TokenCost{Input: 4, Output: 4}})

	input, output := l.windowsOf(t, "p", "m")
	if input != 4 || output != 4 {
		t.Fatalf("windows after a sub-gate Success = (%v, %v), want both unchanged at 4: an idle host must not accumulate an imaginary window", input, output)
	}
}

func TestAnAnswerCarryingAWindowsWorthOfTokensEarnsOneStep(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.admitOne("p", "m") // peak=2, window/2=2: gate met

	l.OnResult(Result{Participant: "p", Model: "m", Verdict: Success, Carried: TokenCost{Input: 4, Output: 4}})

	input, output := l.windowsOf(t, "p", "m")
	if input != 5 || output != 5 {
		t.Fatalf("windows after an answer carrying a whole window = (%v, %v), want both 5: growth is one step per window's worth of tokens", input, output)
	}
}

func TestOverloadNarrowsBothWindowsSoftly(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")

	l.answered("p", "m", Overload)

	input, output := l.windowsOf(t, "p", "m")
	if input != 3.4 || output != 3.4 {
		t.Fatalf("windows after Overload = (%v, %v), want both 3.4 (4 × 0.85): a host saying slow down blames neither dimension, so no cross factor applies", input, output)
	}
}

func TestOverloadFloorsWindowAtTheMinimum(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.Pricing.Input.Initial, cfg.Pricing.Output.Initial = 1, 1
	l := newTestLimiter(cfg, fixedNow(testEpoch))
	l.admitOne("p", "m")

	l.answered("p", "m", Overload)

	input, output := l.windowsOf(t, "p", "m")
	if input != 1 || output != 1 {
		t.Fatalf("windows after Overload at the floor = (%v, %v), want both the minimum 1", input, output)
	}
}

func TestOverloadNeverTripsCutoff(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	for range 10 {
		release, _ := l.admitOne("p", "m")
		l.answered("p", "m", Overload)
		if release != nil {
			release()
		}
	}

	if !l.admits("p", "m") {
		t.Fatal("Acquire() after repeated Overload verdicts = false, want true (cutoff must stay closed)")
	}
}

func TestUpstreamFaultNarrowsBothWindowsAndKeepsTheBreakersCount(t *testing.T) {
	t.Parallel()
	cfg := testConfig() // AfterFailures=3
	l := newTestLimiter(cfg, fixedNow(testEpoch))
	l.admitOne("p", "m")

	// A host answering 5xx between resets must not clear the count that opens the cutoff.
	for range cfg.AfterFailures - 1 {
		l.answered("p", "m", TransportFault)
		l.answered("p", "m", UpstreamFault)
	}
	hard := cfg.Factors.Hard
	floor := float64(cfg.Pricing.Input.Min)
	wantNarrowed := narrowedTo(narrowedTo(4, hard, floor), hard, floor)
	input, output := l.windowsOf(t, "p", "m")
	if input != wantNarrowed || output != wantNarrowed {
		t.Fatalf("windows after two 5xx answers = (%v, %v), want both %v: the host answered, so each one narrows at the hard factor", input, output, wantNarrowed)
	}

	l.answered("p", "m", TransportFault)
	if l.admits("p", "m") {
		t.Fatal("Acquire() after the threshold-th transport fault = true, want false (a 5xx must not clear the count that opens the cutoff)")
	}
}

func TestAMissedReceiptDeadlineNarrowsBothWindowsAndKeepsTheBreakersCount(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.answered("p", "m", TransportFault)
	l.answered("p", "m", TransportFault)

	l.answered("p", "m", MissedReceiptDeadline)

	input, output := l.windowsOf(t, "p", "m")
	if input != 2 || output != 2 {
		t.Fatalf("windows after a missed receipt deadline = (%v, %v), want both 2 (4 × 0.5): a receipt arrives before any prefill, so lateness blames the host, not a dimension", input, output)
	}
	l.answered("p", "m", TransportFault)
	if l.admits("p", "m") {
		t.Fatal("Acquire() after the threshold-th transport fault = true, want false (a missed deadline must not clear the breaker's count)")
	}
}

func TestAMissedFirstTokenDeadlineBlamesPrefillAndOnlyGrazesDecode(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")

	l.answered("p", "m", MissedFirstTokenDeadline)

	input, output := l.windowsOf(t, "p", "m")
	if input != 2 {
		t.Fatalf("input window after a missed first token = %v, want 2 (4 × 0.5): the receipt arrived and the first content did not, which is prefill", input)
	}
	if output != 3.6 {
		t.Fatalf("output window after a missed first token = %v, want 3.6 (4 × 0.9): prefill and decode share one device, so the window that was not blamed still gives ground", output)
	}
}

func TestAStalledStreamBlamesDecodeAndLeavesTheCutoffAlone(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.answered("p", "m", TransportFault)
	l.answered("p", "m", TransportFault)

	l.answered("p", "m", DecodeStalled)

	input, output := l.windowsOf(t, "p", "m")
	if output != 2 {
		t.Fatalf("output window after a stall = %v, want 2 (4 × 0.5): a stream that went silent between chunks is decode starved", output)
	}
	if input != 3.6 {
		t.Fatalf("input window after a stall = %v, want 3.6 (4 × 0.9): the window that was not blamed takes the cross factor", input)
	}
	if !l.admits("p", "m") {
		t.Fatal("Acquire() after a stall = false, want true: a host that answered slowly is not one the cutoff is for, and the outlier detector already answers a host that stalls chronically")
	}
}

func TestALateSuccessKeepsTheWindowAndClearsTheBreakersCount(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.admitOne("p", "m") // peak=2, window/2=2: a timely Success would widen
	l.answered("p", "m", TransportFault)
	l.answered("p", "m", TransportFault)

	l.OnResult(Result{Participant: "p", Model: "m", Verdict: LateSuccess, Carried: TokenCost{Input: 4, Output: 4}})

	input, output := l.windowsOf(t, "p", "m")
	if input != 4 || output != 4 {
		t.Fatalf("windows after a LateSuccess at the utilisation gate = (%v, %v), want both unchanged at 4: widening would undo the narrowing the lateness earned", input, output)
	}
	l.answered("p", "m", TransportFault)
	if !l.admits("p", "m") {
		t.Fatal("Acquire() after one fault following a LateSuccess = false, want true (the answer must clear the breaker's count)")
	}
}

func TestALateSuccessLiftsAHalfOpenCutoff(t *testing.T) {
	t.Parallel()
	clock := newMovingClock(testEpoch)
	cfg := testConfig()
	l := newTestLimiter(cfg, clock.now)
	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	clock.advance(cfg.BaseOpen)
	l.admitOne("p", "m") // the half-open probe

	l.answered("p", "m", LateSuccess)

	if got := l.Snapshot()[0].Cutoff; got != CutoffClosed {
		t.Fatalf("cutoff after a late answer to the probe = %q, want %q", got, CutoffClosed)
	}
}

func TestAnEmptyAnswerNarrowsBothWindowsAndKeepsTheBreakersCount(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.answered("p", "m", TransportFault)
	l.answered("p", "m", TransportFault)

	l.answered("p", "m", EmptyAnswer)

	input, output := l.windowsOf(t, "p", "m")
	if input != 2.8 || output != 2.8 {
		t.Fatalf("windows after an empty answer = (%v, %v), want both 2.8 (4 × 0.7)", input, output)
	}
	l.answered("p", "m", TransportFault)
	if l.admits("p", "m") {
		t.Fatal("Acquire() after the threshold-th transport fault = true, want false (an empty answer must not clear the breaker's count)")
	}
}

func TestAnEmptyAnswerLeftOpenNarrowsSeverelyAndCountsTowardsTheCutoff(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.answered("p", "m", TransportFault)
	l.answered("p", "m", TransportFault)

	l.answered("p", "m", EmptyAnswerLeftOpen)

	input, output := l.windowsOf(t, "p", "m")
	if input != 2 || output != 2 {
		t.Fatalf("windows after an empty answer that left its nonce open = (%v, %v), want both 2 (4 × 0.5)", input, output)
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() after the threshold-th fault = true, want false (an empty answer left open counts towards the cutoff)")
	}
}

func TestNoNarrowingGoesBelowTheMinimumWindow(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		verdict Verdict
	}{
		{"overload", Overload},
		{"upstream fault", UpstreamFault},
		{"missed receipt deadline", MissedReceiptDeadline},
		{"missed first token deadline", MissedFirstTokenDeadline},
		{"decode stalled", DecodeStalled},
		{"empty answer", EmptyAnswer},
		{"empty answer left open", EmptyAnswerLeftOpen},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			cfg := testConfig()
			cfg.Pricing.Input.Min, cfg.Pricing.Output.Min = 3, 3
			l := newTestLimiter(cfg, fixedNow(testEpoch))
			l.admitOne("p", "m")

			for range 20 {
				l.answered("p", "m", testCase.verdict)
			}

			input, output := l.windowsOf(t, "p", "m")
			if input != 3 || output != 3 {
				t.Fatalf("windows after twenty narrowings = (%v, %v), want both the minimum 3: a host a run of bad answers narrowed still takes enough work to earn its window back", input, output)
			}
		})
	}
}

func TestADelayedButSuccessfulAnswerNarrowsInsteadOfWidening(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.admitOne("p", "m") // the utilisation gate is met, so a healthy answer would widen

	l.OnResult(Result{
		Participant: "p", Model: "m", Verdict: Success,
		Carried:  TokenCost{Input: 4, Output: 4},
		Pressure: Pressure{Input: 1.5, Output: 1},
	})

	input, output := l.windowsOf(t, "p", "m")
	if input != 3.4 {
		t.Fatalf("input window after a slow but successful answer = %v, want 3.4 (4 × 0.85): a host slower than its own best is congested before it has failed anything", input)
	}
	if output != 3.6 {
		t.Fatalf("output window after a slow but successful answer = %v, want 3.6 (4 × 0.9): the window that was not blamed takes the cross factor", output)
	}
}

func TestLatencyInsideTheSlackStillWidens(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch)) // Slack=0.30
	l.admitOne("p", "m")
	l.admitOne("p", "m")

	l.OnResult(Result{
		Participant: "p", Model: "m", Verdict: Success,
		Carried:  TokenCost{Input: 4, Output: 4},
		Pressure: Pressure{Input: 1.29, Output: 1.29},
	})

	input, output := l.windowsOf(t, "p", "m")
	if input != 5 || output != 5 {
		t.Fatalf("windows after an answer inside the slack = (%v, %v), want both 5: ordinary jitter is not congestion", input, output)
	}
}

func TestTransportFaultTripsAtExactThreshold(t *testing.T) {
	t.Parallel()
	cfg := testConfig() // AfterFailures=3
	l := newTestLimiter(cfg, fixedNow(testEpoch))

	for range cfg.AfterFailures - 1 {
		l.answered("p", "m", TransportFault)
	}
	release, admitted := l.admitOne("p", "m")
	if !admitted {
		t.Fatal("Acquire() before AfterFailures reached = false, want true (cutoff not yet open)")
	}
	release()

	l.answered("p", "m", TransportFault) // the AfterFailures-th consecutive fault

	if l.admits("p", "m") {
		t.Fatal("Acquire() at AfterFailures = true, want false (cutoff must open)")
	}
}

func TestTransportFaultBackoffLadderGrowsThenCaps(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.AfterFailures, cfg.BaseOpen, cfg.MaxOpen = 1, 1*time.Second, 3*time.Second
	l := newTestLimiter(cfg, fixedNow(testEpoch))

	want := []time.Duration{
		1 * time.Second, // base * 1.6^0
		time.Duration(1.6 * float64(time.Second)),  // base * 1.6^1 = 1.6s
		time.Duration(2.56 * float64(time.Second)), // base * 1.6^2 = 2.56s
		3 * time.Second, // base * 1.6^3 = 4.096s, capped at MaxOpen
	}
	for i, wantBackoff := range want {
		l.answered("p", "m", TransportFault) // AfterFailures=1: every call re-trips
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

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() immediately after trip = true, want false (cutoff open)")
	}

	state := l.states[key{participant: "p", model: "m"}]
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)

	if !l.admits("p", "m") {
		t.Fatal("Acquire() after cooldown elapsed = false, want true (half-open probe)")
	}
	if l.admits("p", "m") {
		t.Fatal("second concurrent Acquire() during half-open = true, want false (exactly one probe allowed)")
	}
}

func TestHalfOpenSuccessClosesCutoffAndDecaysBackoff(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	state := l.states[key{participant: "p", model: "m"}]
	backoffCountAfterTrip := state.backoffCount
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)
	release, _ := l.admitOne("p", "m") // admits the half-open probe

	l.answered("p", "m", Success)

	if state.halfOpen {
		t.Fatal("halfOpen after probe Success = true, want false (cutoff closed)")
	}
	if !state.openUntil.IsZero() {
		t.Fatalf("openUntil after probe Success = %v, want zero (cutoff fully closed)", state.openUntil)
	}
	if state.backoffCount != backoffCountAfterTrip-1 {
		t.Fatalf("backoffCount after probe Success = %d, want %d (decayed by one)", state.backoffCount, backoffCountAfterTrip-1)
	}
	release()
	if !l.admits("p", "m") {
		t.Fatal("Acquire() after cutoff closed = false, want true")
	}
}

func TestHalfOpenTransportFaultReopensWithLongerCooldown(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	state := l.states[key{participant: "p", model: "m"}]
	firstCooldown := state.openUntil.Sub(clock.now())
	clock.advance(firstCooldown + time.Millisecond)
	l.admitOne("p", "m") // half-open probe admitted
	probeTime := clock.now()

	l.answered("p", "m", TransportFault) // the probe itself fails: a single fault, not AfterFailures-many

	if state.halfOpen {
		t.Fatal("halfOpen after failed probe = true, want false (fully reopened)")
	}
	secondCooldown := state.openUntil.Sub(probeTime)
	if secondCooldown <= firstCooldown {
		t.Fatalf("second cooldown = %v, want longer than first cooldown %v", secondCooldown, firstCooldown)
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() right after re-opening = true, want false")
	}
}

func TestModelOutcomeNeverPenalizes(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")

	for range 20 {
		l.answered("p", "m", ModelOutcome)
	}

	state, exists := l.states[key{participant: "p", model: "m"}]
	if !exists {
		t.Fatal("state missing after Acquire")
	}
	if state.input.tokens != 4 || state.output.tokens != 4 {
		t.Fatalf("windows after ModelOutcome verdicts = (%v, %v), want both unchanged at 4", state.input.tokens, state.output.tokens)
	}
	if !state.openUntil.IsZero() || state.halfOpen || state.consecutiveCutoffFaults != 0 || state.backoffCount != 0 {
		t.Fatal("cutoff state moved after ModelOutcome verdicts, want fully unchanged")
	}
}

func TestModelOutcomeWithoutPriorAcquireCreatesNoState(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	l.answered("never-acquired", "m", ModelOutcome)

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
	for g := range goroutines {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			source := rand.New(rand.NewSource(seed))
			for range iterationsPerGoroutine {
				participant := participants[source.Intn(len(participants))]
				model := models[source.Intn(len(models))]
				if release, admitted := l.admitOne(participant, model); admitted {
					l.answered(participant, model, verdicts[source.Intn(len(verdicts))])
					release()
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

// The cutoff's cooldown must stay strictly below perf's ejection horizon so perf
// remains the pool authority, not the cutoff.
func TestCutoffMaxOpenDefaultStaysBelowPerfEjectionHorizon(t *testing.T) {
	t.Parallel()
	defaults := config.Defaults()
	cutoffMaxOpen := time.Duration(defaults.Limits.HostCutoff.MaxMS) * time.Millisecond
	perfEjectionMax := time.Duration(defaults.Perf.EjectionMaxSeconds) * time.Second

	if cutoffMaxOpen >= perfEjectionMax {
		t.Fatalf("cutoff MaxMS default %v must stay below perf's ejection horizon %v", cutoffMaxOpen, perfEjectionMax)
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

func TestAvailableFalseWhileCutoffOpenThenTrueAfterCooldown(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	if l.Available("p", "m") {
		t.Fatal("Available() while cutoff Open = true, want false")
	}

	state := l.states[key{participant: "p", model: "m"}]
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)

	if !l.Available("p", "m") {
		t.Fatal("Available() after cooldown elapsed = false, want true (half-open probe available)")
	}
	if !l.admits("p", "m") {
		t.Fatal("Acquire() after Available() reported half-open = false, want true (peek must not consume the probe)")
	}
}

func TestAvailableFalseDuringHalfOpenProbeInFlight(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	clock := newMovingClock(testEpoch)
	l := newTestLimiter(cfg, clock.now)

	for range cfg.AfterFailures {
		l.answered("p", "m", TransportFault)
	}
	state := l.states[key{participant: "p", model: "m"}]
	clock.advance(state.openUntil.Sub(clock.now()) + time.Millisecond)
	if !l.admits("p", "m") {
		t.Fatal("Acquire() for the half-open probe = false, want true")
	}

	if l.Available("p", "m") {
		t.Fatal("Available() with the half-open probe in flight = true, want false")
	}
}

func TestAvailableFalseAtWindowThenTrueAfterRelease(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	release, _ := l.admitOne("p", "m")
	for range 3 {
		l.admitOne("p", "m")
	}
	if l.Available("p", "m") {
		t.Fatal("Available() with the window full = true, want false")
	}

	release()

	if !l.Available("p", "m") {
		t.Fatal("Available() after a lease gave its tokens back = false, want true")
	}
}

func TestAvailableDoesNotConsumeWindowTokens(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))

	for range 10 {
		l.Available("p", "m")
	}
	if _, exists := l.states[key{participant: "p", model: "m"}]; exists {
		t.Fatal("Available() created participant state as a side effect, want no-op until first Acquire")
	}

	for i := range 4 {
		if !l.admits("p", "m") {
			t.Fatalf("Acquire() call %d after repeated Available() peeks = false, want true (peeks must not consume tokens)", i+1)
		}
	}
	if l.admits("p", "m") {
		t.Fatal("Acquire() beyond the window after repeated Available() peeks = true, want false")
	}
}

func TestAvailableLeavesExistingStateUnchanged(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.answered("p", "m", TransportFault)
	before := *l.states[key{participant: "p", model: "m"}]

	for range 10 {
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
	for g := range goroutines {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			source := rand.New(rand.NewSource(seed))
			for range iterationsPerGoroutine {
				participant := participants[source.Intn(len(participants))]
				model := models[source.Intn(len(models))]
				l.Available(participant, model)
				if release, admitted := l.admitOne(participant, model); admitted {
					l.answered(participant, model, verdicts[source.Intn(len(verdicts))])
					release()
				}
			}
		}(int64(g))
	}
	wg.Wait()
}

func TestClearQuarantineReopensEveryModelsCutoffForOneParticipant(t *testing.T) {
	now := time.Now()
	cfg := testConfig()
	cfg.AfterFailures, cfg.BaseOpen, cfg.MaxOpen = 1, time.Minute, time.Minute
	limiter := newTestLimiter(cfg, func() time.Time { return now })

	limiter.answered("host-a", "model-a", TransportFault)
	limiter.answered("host-a", "model-b", TransportFault)
	limiter.answered("host-b", "model-a", TransportFault)
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

// The engine releases an attempt's lease in a defer and reports its verdict afterwards, so a growth gate
// reading the live count sees the tokens already given back and refuses to grow a window that was
// genuinely saturated. The decision must come out the same whichever call lands first.
func TestWindowGrowthDoesNotDependOnReleaseOrder(t *testing.T) {
	base := time.Unix(0, 0)
	grow := func(releaseFirst bool) float64 {
		limiter := newTestLimiter(testConfig(), func() time.Time { return base })
		var first func()
		for range 2 {
			release, admitted := limiter.admitOne("host-a", "model-a")
			if !admitted {
				t.Fatal("Acquire refused inside the initial window")
			}
			if first == nil {
				first = release
			}
		}
		answer := Result{
			Participant: "host-a", Model: "model-a", Verdict: Success,
			Carried: TokenCost{Input: 4, Output: 4},
		}
		if releaseFirst {
			first()
			limiter.OnResult(answer)
		} else {
			limiter.OnResult(answer)
			first()
		}
		for _, host := range limiter.Snapshot() {
			if host.Participant == "host-a" && host.Model == "model-a" {
				return host.InputWindowTokens
			}
		}
		t.Fatal("the limiter reported no window for the host it just admitted")
		return 0
	}

	released, reported := grow(true), grow(false)
	if released != reported {
		t.Fatalf("window = %v when the lease was released first and %v when OnResult ran first; the order must not matter", released, reported)
	}
	if released <= 4 {
		t.Fatalf("window = %v, want growth past the initial 4: half a four-token window is the growth threshold", released)
	}
}

// windowOf reads one participant's current input window out of the snapshot the collector also reads.
func windowOf(t *testing.T, limiter *ParticipantLimiter, participant string) float64 {
	t.Helper()
	for _, window := range limiter.Snapshot() {
		if window.Participant == participant {
			return window.InputWindowTokens
		}
	}
	t.Fatalf("participant %q absent from the snapshot", participant)
	return 0
}

// An operator raising the initial window after a bad episode expects the raise to reach participants
// already tracked. Without that the setting only ever applies to a restarted gateway.
func TestReconfigureLiftsAWindowThatCollapsedBelowTheNewInitial(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(testConfig(), fixedNow(testEpoch))
	limiter.admitOne("host-a", "model-a")
	limiter.answered("host-a", "model-a", Overload)
	if collapsed := windowOf(t, limiter, "host-a"); collapsed >= 4 {
		t.Fatalf("window = %v, want it shrunk below the initial 4 by the overload", collapsed)
	}

	lifted := testConfig()
	lifted.Pricing.Input.Initial, lifted.Pricing.Output.Initial = 32, 32
	limiter.Reconfigure(lifted)

	if got := windowOf(t, limiter, "host-a"); got != 32 {
		t.Fatalf("window = %v, want the new initial 32", got)
	}
}

// A window a host earned is its own: raising the initial lifts one that collapsed below it and leaves a wider
// one alone, because where a window stops is what the host's congestion signals say.
func TestReconfigureLeavesAWindowThatAlreadyGrewPastTheNewInitial(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(testConfig(), fixedNow(testEpoch))
	for range 40 {
		limiter.admitOne("host-a", "model-a")
		limiter.OnResult(Result{
			Participant: "host-a", Model: "model-a", Verdict: Success,
			Carried: TokenCost{Input: 64, Output: 64},
		})
	}
	grown := windowOf(t, limiter, "host-a")
	if grown <= 8 {
		t.Fatalf("window = %v, want it grown well past 8 by the successes", grown)
	}

	narrowed := testConfig()
	narrowed.Pricing.Input.Initial, narrowed.Pricing.Output.Initial = 1, 1
	limiter.Reconfigure(narrowed)

	if got := windowOf(t, limiter, "host-a"); got != grown {
		t.Fatalf("window = %v, want the %v the host had earned: a lowered initial is a floor, not a ceiling", got, grown)
	}
}

// Jitter exists so hosts that trip together do not reopen together; clamping after it would erase it
// exactly when every host is saturated at MaxOpen.
func TestSaturatedBackoffStillCarriesJitter(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.AfterFailures, cfg.BaseOpen, cfg.MaxOpen = 1, 1*time.Second, 3*time.Second
	l := NewParticipantLimiter(cfg, fixedNow(testEpoch))
	l.jitter = func(backoff time.Duration) time.Duration { return backoff / 5 }

	for range 4 { // the ladder saturates well before the fourth trip
		l.answered("p", "m", TransportFault)
	}

	got := l.states[key{participant: "p", model: "m"}].openUntil.Sub(testEpoch)
	if !withinTolerance(got, cfg.MaxOpen+cfg.MaxOpen/5) {
		t.Fatalf("saturated backoff = %v, want %v (MaxOpen plus its jitter)", got, cfg.MaxOpen+cfg.MaxOpen/5)
	}
}

func idleEvictionConfig() ParticipantConfig {
	settings := testConfig()
	settings.IdleEviction = time.Hour
	return settings
}

func openWindow(participant string, inflight int64) HostWindow {
	return HostWindow{
		Participant: participant, Model: "model-a",
		InputWindowTokens: 4, OutputWindowTokens: 4,
		InflightInputTokens: inflight, InflightOutputTokens: inflight,
		Cutoff: CutoffClosed, Available: true,
	}
}

// A pair nothing has used for the eviction window is forgotten; one still carrying an attempt is not.
func TestAcquireForgetsAPairIdlePastTheEvictionWindow(t *testing.T) {
	t.Parallel()
	clock := newMovingClock(testEpoch)
	limiter := newTestLimiter(idleEvictionConfig(), clock.now)
	release, admitted := limiter.admitOne("host-released", "model-a")
	require.True(t, admitted)
	release()
	require.True(t, limiter.admits("host-busy", "model-a"))

	clock.advance(time.Hour + time.Minute)
	require.True(t, limiter.admits("host-new", "model-a"))

	require.Equal(t, []HostWindow{
		openWindow("host-busy", 1),
		openWindow("host-new", 1),
	}, limiter.Snapshot())
}

// A running cut-off survives idle eviction; an idle half-open pair past the window does not.
func TestAcquireForgetsAnIdleHalfOpenPairButKeepsARunningCutoff(t *testing.T) {
	t.Parallel()
	settings := idleEvictionConfig()
	settings.AfterFailures, settings.BaseOpen, settings.MaxOpen = 1, 2*time.Hour, 4*time.Hour
	clock := newMovingClock(testEpoch)
	limiter := newTestLimiter(settings, clock.now)
	limiter.answered("host-probing", "model-a", TransportFault)
	clock.advance(2*time.Hour + time.Minute)
	release, admitted := limiter.admitOne("host-probing", "model-a")
	require.True(t, admitted, "an expired cut-off admits one probe")
	release()
	limiter.answered("host-cut-off", "model-a", TransportFault)

	clock.advance(time.Hour + time.Minute)
	require.True(t, limiter.admits("host-trigger", "model-a"))

	require.Equal(t, []HostWindow{
		{
			Participant: "host-cut-off", Model: "model-a",
			InputWindowTokens: 4, OutputWindowTokens: 4,
			Cutoff: CutoffOpen, BackoffCount: 1,
		},
		openWindow("host-trigger", 1),
	}, limiter.Snapshot())
}

// A lease released now stamps lastUsed, so a pair whose verdict is still pending is kept.
func TestAcquireKeepsAPairJustReleasedEvenPastTheEvictionWindow(t *testing.T) {
	t.Parallel()
	clock := newMovingClock(testEpoch)
	limiter := newTestLimiter(idleEvictionConfig(), clock.now)
	release, admitted := limiter.admitOne("host-pending", "model-a")
	require.True(t, admitted)

	clock.advance(time.Hour + time.Minute)
	release()
	require.True(t, limiter.admits("host-trigger", "model-a"))

	require.Len(t, limiter.Snapshot(), 2, "host-pending was released just now; its verdict may still be reported")
}

// OnResult stamps lastUsed too, so a verdict arriving after a release keeps postponing the pair's eviction.
func TestAcquireKeepsAPairWhoseVerdictArrivedAfterItWasReleased(t *testing.T) {
	t.Parallel()
	clock := newMovingClock(testEpoch)
	limiter := newTestLimiter(idleEvictionConfig(), clock.now)
	release, admitted := limiter.admitOne("host-late-result", "model-a")
	require.True(t, admitted)
	release()

	clock.advance(55 * time.Minute)
	limiter.answered("host-late-result", "model-a", Success)

	clock.advance(10 * time.Minute)
	require.True(t, limiter.admits("host-trigger", "model-a"))

	require.Len(t, limiter.Snapshot(), 2, "the verdict landed 10m ago, not 1h5m, because OnResult refreshed lastUsed")
}

// The scan runs under the limiter's one lock on the admission path, so it runs at most once per tenth of the window.
func TestAcquireScansForIdlePairsAtMostOncePerTenthOfTheWindow(t *testing.T) {
	t.Parallel()
	clock := newMovingClock(testEpoch)
	limiter := newTestLimiter(idleEvictionConfig(), clock.now)
	release, admitted := limiter.admitOne("host-idle", "model-a")
	require.True(t, admitted)
	release()
	clock.advance(58 * time.Minute)
	require.True(t, limiter.admits("host-trigger", "model-a"))

	clock.advance(3 * time.Minute)
	require.True(t, limiter.admits("host-trigger", "model-a"))
	require.Len(t, limiter.Snapshot(), 2, "three minutes after the last scan, host-idle may not be forgotten yet")

	clock.advance(3 * time.Minute)
	require.True(t, limiter.admits("host-trigger", "model-a"))
	require.Len(t, limiter.Snapshot(), 1)
}

func TestIdleEvictionIsRaceFreeUnderConcurrentAdmission(t *testing.T) {
	settings := idleEvictionConfig()
	settings.IdleEviction = time.Minute
	clock := newMovingClock(testEpoch)
	limiter := newTestLimiter(settings, clock.now)
	participants := []string{"p1", "p2", "p3"}

	var workers sync.WaitGroup
	for _, participant := range participants {
		workers.Go(func() {
			for range 200 {
				clock.advance(time.Second)
				if release, admitted := limiter.admitOne(participant, "m"); admitted {
					limiter.answered(participant, "m", Success)
					release()
				}
			}
		})
	}
	workers.Wait()

	for _, window := range limiter.Snapshot() {
		require.Zero(t, window.InflightInputTokens, window.Participant)
		require.Zero(t, window.InflightOutputTokens, window.Participant)
	}
}

// pricedConfig counts windows in requests the way production does, so a test can state a context length
// in tokens and read the window back in tokens.
func pricedConfig() ParticipantConfig {
	settings := testConfig()
	settings.Pricing.Input = RequestBounds{Min: 1, Initial: 4}
	settings.Pricing.Output = RequestBounds{Min: 1, Initial: 4}
	settings.Pricing.FallbackContextTokens = 1_000
	settings.Pricing.FallbackOutputTokens = 100
	return settings
}

func TestAFreshHostIsPricedAtTheContextLengthGovernanceReports(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(pricedConfig(), fixedNow(testEpoch))
	l.ObserveModels(map[string]int64{"long-context": 400_000})

	l.Acquire("p", "long-context", oneToken)
	l.Acquire("p", "unnamed-model", oneToken)

	input, output := l.windowsOf(t, "p", "long-context")
	if input != 4*400_000 {
		t.Errorf("input window = %v, want %v: prefill is counted in the tokens of the model's own context length", input, 4*400_000)
	}
	if output != 4*100 {
		t.Errorf("output window = %v, want 400: decode is counted in the model's output cap, which governance does not set", output)
	}
	if unnamed, _ := l.windowsOf(t, "p", "unnamed-model"); unnamed != 4*1_000 {
		t.Errorf("input window of a model governance did not name = %v, want the default %v", unnamed, 4*1_000)
	}
}

func TestAnOperatorsPinOutranksWhatGovernanceReports(t *testing.T) {
	t.Parallel()
	settings := pricedConfig()
	settings.Pricing.ContextTokensByModel = map[string]int64{"pinned": 8_000}
	l := newTestLimiter(settings, fixedNow(testEpoch))
	l.ObserveModels(map[string]int64{"pinned": 400_000})

	l.Acquire("p", "pinned", oneToken)

	if input, _ := l.windowsOf(t, "p", "pinned"); input != 4*8_000 {
		t.Fatalf("input window = %v, want %v: an operator who pinned a context length means it", input, 4*8_000)
	}
}

// An escrow created later must not hand a host back the window a run of bad answers took from it.
func TestObservingModelsKeepsAWindowAHostHasAlreadyEarned(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(pricedConfig(), fixedNow(testEpoch))
	l.ObserveModels(map[string]int64{"model-a": 1_000})
	l.Acquire("p", "model-a", oneToken)
	l.answered("p", "model-a", EmptyAnswerLeftOpen)
	narrowed, _ := l.windowsOf(t, "p", "model-a")

	l.ObserveModels(map[string]int64{"model-a": 1_000})

	if input, _ := l.windowsOf(t, "p", "model-a"); input != narrowed {
		t.Fatalf("input window = %v, want the %v the host had earned: observing a model is not an operator raising the initial window", input, narrowed)
	}
}

func TestObservingASmallerContextMovesTheFloorAndLeavesTheWindowAlone(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(pricedConfig(), fixedNow(testEpoch))
	l.ObserveModels(map[string]int64{"model-a": 1_000})
	l.Acquire("p", "model-a", oneToken)
	opened, _ := l.windowsOf(t, "p", "model-a")

	l.ObserveModels(map[string]int64{"model-a": 100})

	input, _ := l.windowsOf(t, "p", "model-a")
	if input != opened {
		t.Fatalf("input window = %v, want the %v it already had: governance renaming a context is not a reason to take a window away", input, opened)
	}
	if got := l.states[key{participant: "p", model: "model-a"}].bounds.Input; got.Min != 100 || got.Step != 100 {
		t.Fatalf("bounds = %+v, want a floor and a step of 100: the unit a window is judged in follows the model", got)
	}
}

// Growth is one rung per answer, before any congestion and after it alike: a host that was narrowed climbs
// back at the same rate it climbed in the first place, and no answer is worth more than one request of room.
func TestAWindowEarnsOneStepPerAnswerWhetherOrNotItWasEverNarrowed(t *testing.T) {
	t.Parallel()
	l := newTestLimiter(testConfig(), fixedNow(testEpoch))
	l.admitOne("p", "m")
	l.admitOne("p", "m")
	wholeWindow := Result{Participant: "p", Model: "m", Verdict: Success, Carried: TokenCost{Input: 4, Output: 4}}

	l.OnResult(wholeWindow)

	input, output := l.windowsOf(t, "p", "m")
	if input != 5 || output != 5 {
		t.Fatalf("windows after one answer = (%v, %v), want both 5: one answer is worth one request of room", input, output)
	}

	l.answered("p", "m", Overload)
	narrowedInput, _ := l.windowsOf(t, "p", "m")
	l.admitOne("p", "m")
	l.admitOne("p", "m")
	l.OnResult(wholeWindow)

	if grown, _ := l.windowsOf(t, "p", "m"); grown != narrowedInput+1 {
		t.Fatalf("window = %v after congestion, want %v: the rung is the same one on the way back up", grown, narrowedInput+1)
	}
}
