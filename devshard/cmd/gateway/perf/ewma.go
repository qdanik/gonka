package perf

import (
	"math"
	"time"
)

// peakEWMA snaps up instantly on a new peak and relaxes toward prior only
// gradually. Not goroutine-safe; the caller holds the tracker lock.
type peakEWMA struct {
	value     float64
	lastTouch time.Time
	halfLife  time.Duration
	prior     float64
	hasSample bool
}

func newPeakEWMA(halfLife time.Duration, prior float64) *peakEWMA {
	return &peakEWMA{halfLife: halfLife, prior: prior}
}

func decayWeight(elapsed, halfLife time.Duration) float64 {
	if elapsed < 0 {
		elapsed = 0
	}
	return math.Exp2(-float64(elapsed) / float64(halfLife))
}

func (e *peakEWMA) decayedValue(now time.Time) (value, weight float64) {
	if !e.hasSample {
		return e.prior, 0
	}
	w := decayWeight(now.Sub(e.lastTouch), e.halfLife)
	return e.prior + (e.value-e.prior)*w, w
}

// Value is a pure read of the estimate decayed to now.
func (e *peakEWMA) Value(now time.Time) float64 {
	v, _ := e.decayedValue(now)
	return v
}

func (e *peakEWMA) Add(sample float64, now time.Time) {
	if !e.hasSample {
		e.value, e.lastTouch, e.hasSample = sample, now, true
		return
	}
	decayed, w := e.decayedValue(now)
	if sample >= decayed {
		e.value = sample // snap to the peak
	} else {
		e.value = decayed + (sample-decayed)*(1-w)
	}
	e.lastTouch = now
}
