package perf

import (
	"math"
	"testing"
	"time"
)

// testEpoch is a fixed reference instant; every test time is derived from
// it via Add so assertions never depend on the real wall clock.
var testEpoch = time.Date(2026, 7, 20, 12, 0, 0, 0, time.UTC)

func almostEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) <= epsilon
}

func TestPeakEWMAColdValueReturnsPrior(t *testing.T) {
	e := newPeakEWMA(10*time.Minute, 100)
	if got := e.Value(testEpoch); got != 100 {
		t.Fatalf("Value() before any Add = %v, want prior 100", got)
	}
}

func TestPeakEWMAFirstSampleDominatesPrior(t *testing.T) {
	e := newPeakEWMA(10*time.Minute, 100)
	e.Add(500, testEpoch)
	if got := e.Value(testEpoch); got != 500 {
		t.Fatalf("Value() immediately after first Add = %v, want sample 500", got)
	}
}

func TestPeakEWMAValueAfterOneHalfLifeIsHalfwayToPrior(t *testing.T) {
	const halfLife = 10 * time.Minute
	const prior, high = 100.0, 500.0
	e := newPeakEWMA(halfLife, prior)
	e.Add(high, testEpoch)

	got := e.Value(testEpoch.Add(halfLife))
	want := (high + prior) / 2
	if !almostEqual(got, want, 1e-9) {
		t.Fatalf("Value() after one half-life = %v, want %v (halfway between high and prior)", got, want)
	}
}

func TestPeakEWMAValueAfterManyHalfLivesApproachesPrior(t *testing.T) {
	const halfLife = 10 * time.Minute
	const prior, high = 100.0, 500.0
	e := newPeakEWMA(halfLife, prior)
	e.Add(high, testEpoch)

	got := e.Value(testEpoch.Add(40 * halfLife))
	if !almostEqual(got, prior, 1e-6) {
		t.Fatalf("Value() after 40 half-lives = %v, want ~prior %v", got, prior)
	}
}

func TestPeakEWMASnapsUpInstantlyOnSpike(t *testing.T) {
	const halfLife = 10 * time.Minute
	e := newPeakEWMA(halfLife, 100)
	e.Add(50, testEpoch)

	spikeTime := testEpoch.Add(time.Second)
	e.Add(1000, spikeTime)

	if got := e.Value(spikeTime); got != 1000 {
		t.Fatalf("Value() right after a spike = %v, want exact spike 1000 (peak snap-up, not a blend)", got)
	}
}

// TestPeakEWMASnapsUpAgainstDecayedNotRawStoredValue pins that the peak
// comparison is sample-vs-decayed-now, not sample-vs-last-raw-value: a
// moderate sample below the old peak but above today's decayed estimate
// must still snap up exactly, not blend down from the stale peak.
func TestPeakEWMASnapsUpAgainstDecayedNotRawStoredValue(t *testing.T) {
	const halfLife = 10 * time.Minute
	const prior, oldPeak = 100.0, 1000.0
	e := newPeakEWMA(halfLife, prior)
	e.Add(oldPeak, testEpoch)

	// 5 half-lives later, decayed ~= 100 + 900*2^-5 = 128.125, far below oldPeak.
	laterTime := testEpoch.Add(5 * halfLife)
	const moderateSample = 200.0 // > decayed(~128) but < oldPeak(1000)
	e.Add(moderateSample, laterTime)

	if got := e.Value(laterTime); got != moderateSample {
		t.Fatalf("Value() = %v, want exact sample %v (must compare against decayed value, not raw stored peak)", got, moderateSample)
	}
}

func TestPeakEWMARecoversGraduallyAfterLowSample(t *testing.T) {
	const halfLife = 10 * time.Minute
	const high, low = 1000.0, 50.0
	e := newPeakEWMA(halfLife, 100)
	e.Add(high, testEpoch)

	recoverTime := testEpoch.Add(time.Second) // close in time relative to a 10-minute half-life
	e.Add(low, recoverTime)
	got := e.Value(recoverTime)

	if got <= low {
		t.Fatalf("Value() after one low sample = %v, want > %v (must not drop straight to the low)", got, low)
	}
	if got >= high {
		t.Fatalf("Value() after one low sample = %v, want < %v (must have moved down from the high)", got, high)
	}
}

func TestPeakEWMANegativeElapsedGuardedToZero(t *testing.T) {
	const halfLife = 10 * time.Minute
	e := newPeakEWMA(halfLife, 100)
	e.Add(500, testEpoch)

	past := testEpoch.Add(-time.Hour) // now < lastTouch
	if got := e.Value(past); got != 500 {
		t.Fatalf("Value() with now before lastTouch = %v, want unchanged 500 (elapsed clamped to 0)", got)
	}

	// A downward Add with now < lastTouch must not blow up: without the
	// clamp, elapsed would be negative and w = 2^(-elapsed/halfLife) would
	// exceed 1, exploding the blend far outside [low, high].
	e.Add(10, past)
	got := e.Value(past)
	if math.IsNaN(got) || math.IsInf(got, 0) {
		t.Fatalf("Value() after Add with now < lastTouch = %v, want a finite number", got)
	}
	if got != 500 {
		t.Fatalf("Value() after Add with now < lastTouch = %v, want unchanged 500 (zero elapsed blends 0%% toward the sample)", got)
	}
}
