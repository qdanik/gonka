package limits

import (
	"math"
	"testing"
)

func testFactors() CongestionFactors {
	return CongestionFactors{Soft: 0.85, Hard: 0.70, Severe: 0.50, Cross: 0.90}
}

func TestEveryVerdictNamesItsTierDimensionAndBreaker(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		verdict       Verdict
		wantTier      tier
		wantDimension dimension
		wantBreaker   breakerEffect
	}{
		{"success", Success, tierNone, dimensionNone, breakerRecovers},
		{"late success", LateSuccess, tierNone, dimensionNone, breakerRecovers},
		{"model outcome", ModelOutcome, tierNone, dimensionNone, breakerUntouched},
		{"overload", Overload, tierSoft, dimensionBoth, breakerClears},
		{"upstream fault", UpstreamFault, tierHard, dimensionBoth, breakerUntouched},
		{"empty answer", EmptyAnswer, tierHard, dimensionBoth, breakerUntouched},
		{"empty answer left open", EmptyAnswerLeftOpen, tierSevere, dimensionBoth, breakerCounts},
		{"transport fault", TransportFault, tierNone, dimensionNone, breakerCounts},
		{"decode stalled", DecodeStalled, tierSevere, dimensionOutput, breakerUntouched},
		{"missed receipt deadline", MissedReceiptDeadline, tierSevere, dimensionBoth, breakerUntouched},
		{"missed first token deadline", MissedFirstTokenDeadline, tierSevere, dimensionInput, breakerUntouched},
	}

	named := make(map[Verdict]bool, len(testCases))
	for _, testCase := range testCases {
		named[testCase.verdict] = true
	}
	for verdict := Success; verdict <= DecodeStalled; verdict++ {
		if !named[verdict] {
			t.Errorf("verdict %d has no row: a verdict nobody named falls through responseFor to the inert answer and moves nothing", verdict)
		}
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := responseFor(testCase.verdict)
			if got.tier != testCase.wantTier {
				t.Errorf("tier = %q, want %q: the ladder decides how hard this verdict narrows", got.tier, testCase.wantTier)
			}
			if got.dimension != testCase.wantDimension {
				t.Errorf("dimension = %q, want %q: narrowing the wrong window starves the side that was healthy", got.dimension, testCase.wantDimension)
			}
			if got.breaker != testCase.wantBreaker {
				t.Errorf("breaker = %v, want %v: the cutoff counts faults the host never answered", got.breaker, testCase.wantBreaker)
			}
		})
	}
}

func TestABlamedWindowTakesItsTierAndTheOtherTakesTheCrossFactor(t *testing.T) {
	t.Parallel()

	factors := testFactors()
	for _, testCase := range []struct {
		name          string
		narrowingTier tier
		blamed        dimension
		wantInput     float64
		wantOutput    float64
	}{
		{"prefill blamed", tierSevere, dimensionInput, 0.50, 0.90},
		{"decode blamed", tierSevere, dimensionOutput, 0.90, 0.50},
		{"host blamed as a whole", tierHard, dimensionBoth, 0.70, 0.70},
		{"soft congestion on prefill", tierSoft, dimensionInput, 0.85, 0.90},
		{"nothing blamed", tierNone, dimensionNone, 1, 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got := factors.narrowingFor(testCase.narrowingTier, testCase.blamed)
			if got.input != testCase.wantInput || got.output != testCase.wantOutput {
				t.Fatalf("narrowing = (%v, %v), want (%v, %v): prefill and decode share one device, so the window that was not blamed still gives ground",
					got.input, got.output, testCase.wantInput, testCase.wantOutput)
			}
		})
	}
}

func TestAHostWideSignalCarriesNoCrossFactor(t *testing.T) {
	t.Parallel()

	got := testFactors().narrowingFor(tierSevere, dimensionBoth)
	if got.input != 0.50 || got.output != 0.50 {
		t.Fatalf("narrowing = (%v, %v), want (0.5, 0.5): a signal that blames the host as a whole is not a cross-dimension penalty", got.input, got.output)
	}
}

func TestAdditiveIncreaseAddsOneStepPerWindowOfTokens(t *testing.T) {
	t.Parallel()

	const (
		start = 1000.0
		step  = 100.0
	)

	window := start
	carried := int64(0)
	for carried < int64(start) {
		window = grownBy(window, 10, step, false)
		carried += 10
	}

	if math.Abs(window-(start+step)) > step/10 {
		t.Fatalf("window = %v after a window's worth of tokens, want about %v: growth is paced by throughput, one step per window served", window, start+step)
	}
}

func TestAWiderWindowEarnsItsNextRungMoreSlowly(t *testing.T) {
	t.Parallel()

	narrow := grownBy(1000, 100, 100, false) - 1000
	wide := grownBy(10000, 100, 100, false) - 10000
	if !(narrow > wide) {
		t.Fatalf("narrow window grew by %v and wide by %v, want the narrow one to grow faster: growth divided by the window is what keeps a large window from running away", narrow, wide)
	}
}

func TestAnIdleOrCostlessAnswerDoesNotGrowTheWindow(t *testing.T) {
	t.Parallel()

	if got := grownBy(1000, 0, 100, false); got != 1000 {
		t.Fatalf("window = %v, want it unchanged at 1000: an answer that carried nothing proves no throughput", got)
	}
}

func TestDelayPressureNamesTheCongestedWindow(t *testing.T) {
	t.Parallel()

	const slack = 0.3
	for _, testCase := range []struct {
		name     string
		pressure Pressure
		want     dimension
	}{
		{"unobserved", Pressure{}, dimensionNone},
		{"both at their best", Pressure{Input: 1, Output: 1}, dimensionNone},
		{"inside the slack", Pressure{Input: 1.29, Output: 1.29}, dimensionNone},
		{"prefill slower than its best", Pressure{Input: 1.5, Output: 1}, dimensionInput},
		{"decode slower than its best", Pressure{Input: 1, Output: 1.5}, dimensionOutput},
		{"both slower", Pressure{Input: 1.5, Output: 1.5}, dimensionBoth},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.pressure.congestedDimension(slack); got != testCase.want {
				t.Fatalf("congested dimension = %q, want %q: a host slower than its own best is congested before it has failed anything", got, testCase.want)
			}
		})
	}
}

func TestNarrowingStopsAtTheFloor(t *testing.T) {
	t.Parallel()

	if got := narrowedTo(100, 0.5, 80); got != 80 {
		t.Fatalf("window = %v, want the floor 80: a host a run of bad answers narrowed still takes enough work to earn its window back", got)
	}
}

func TestNarrowingNeverGoesBelowOneToken(t *testing.T) {
	t.Parallel()

	if got := narrowedTo(1, 0.5, 0); got != 1 {
		t.Fatalf("window = %v, want 1: a window of zero tokens can never admit anything again", got)
	}
}

func TestSlowStartTakesTheWholeOfWhatItCarried(t *testing.T) {
	t.Parallel()

	if got := grownBy(1000, 250, 100, true); got != 1250 {
		t.Fatalf("window = %v, want 1250: a window still looking for the host's capacity doubles over a window served, rather than earning one step", got)
	}
}
