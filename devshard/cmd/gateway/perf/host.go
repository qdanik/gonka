package perf

import (
	"math"
	"slices"
	"time"
)

const (
	latencyWindowSize    = 64
	latencyWindowMinimum = 10
)

type hostKey struct {
	participant string
	model       string
}

type decayedCounter struct {
	count     float64
	lastTouch time.Time
	halfLife  time.Duration
}

func newDecayedCounter(halfLife time.Duration) decayedCounter {
	return decayedCounter{halfLife: halfLife}
}

func (c *decayedCounter) add(now time.Time) {
	c.count = c.value(now) + 1
	c.lastTouch = now
}

func (c *decayedCounter) value(now time.Time) float64 {
	if c.lastTouch.IsZero() {
		return 0
	}
	return c.count * decayWeight(now.Sub(c.lastTouch), c.halfLife)
}

func decayWeight(elapsed, halfLife time.Duration) float64 {
	if elapsed < 0 {
		elapsed = 0
	}
	return math.Exp2(-float64(elapsed) / float64(halfLife))
}

// hostPerf is not goroutine-safe; the Tracker locks around all access.
type hostPerf struct {
	success         decayedCounter
	fail            decayedCounter
	consecutiveFail int
	lastSeen        time.Time
	firstContent    latencyWindow
}

func newHostPerf(halfLife time.Duration) *hostPerf {
	return &hostPerf{
		success: newDecayedCounter(halfLife),
		fail:    newDecayedCounter(halfLife),
	}
}

func (h *hostPerf) recordSample(s Sample, now time.Time) {
	if s.Responsive {
		h.success.add(now)
		h.consecutiveFail = 0
	} else {
		h.fail.add(now)
		h.consecutiveFail++
	}
	h.lastSeen = now
	if s.FirstContent > 0 {
		h.firstContent.add(s.FirstContent)
	}
}

func (h *hostPerf) resetOutcomeCounters() {
	h.success = newDecayedCounter(h.success.halfLife)
	h.fail = newDecayedCounter(h.fail.halfLife)
}

func (h *hostPerf) failureRate(now time.Time) (rate, volume float64) {
	failValue := h.fail.value(now)
	volume = h.success.value(now) + failValue
	if volume <= 0 {
		return 0, volume
	}
	return failValue / volume, volume
}

// latencyWindow is a ring, not a decayed counter: the escalation needs a quantile, not a rate.
type latencyWindow struct {
	samples [latencyWindowSize]time.Duration
	next    int
	filled  int
}

func (w *latencyWindow) add(sample time.Duration) {
	w.samples[w.next] = sample
	w.next = (w.next + 1) % latencyWindowSize
	if w.filled < latencyWindowSize {
		w.filled++
	}
}

func (w *latencyWindow) p75(minimum int) (time.Duration, bool) {
	if w.filled < minimum {
		return 0, false
	}
	sorted := make([]time.Duration, w.filled)
	copy(sorted, w.samples[:w.filled])
	slices.Sort(sorted)
	return sorted[(len(sorted)*3)/4], true
}
