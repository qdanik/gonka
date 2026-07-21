package perf

import (
	"math"
	"time"
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

// hostPerf is not goroutine-safe; the Tracker locks around all access.
type hostPerf struct {
	ewmaReceipt     *peakEWMA
	ewmaCTTFL       *peakEWMA
	success         decayedCounter
	fail            decayedCounter
	consecutiveFail int
	lastSeen        time.Time
}

func newHostPerf(halfLife time.Duration, priorReceiptMs, priorCTTFL float64) *hostPerf {
	return &hostPerf{
		ewmaReceipt: newPeakEWMA(halfLife, priorReceiptMs),
		ewmaCTTFL:   newPeakEWMA(halfLife, priorCTTFL),
		success:     newDecayedCounter(halfLife),
		fail:        newDecayedCounter(halfLife),
	}
}

func (h *hostPerf) recordSample(s Sample, now time.Time) {
	if receiptMs := s.ReceiptMs(); receiptMs > 0 {
		h.ewmaReceipt.Add(receiptMs, now)
	}
	if cttfl := s.CTTFL(); cttfl > 0 && !math.IsNaN(cttfl) && !math.IsInf(cttfl, 0) {
		h.ewmaCTTFL.Add(cttfl, now)
	}
	if s.Responsive {
		h.success.add(now)
		h.consecutiveFail = 0
	} else {
		h.fail.add(now)
		h.consecutiveFail++
	}
	h.lastSeen = now
}

func (h *hostPerf) resetOutcomeCounters() {
	h.success = newDecayedCounter(h.success.halfLife)
	h.fail = newDecayedCounter(h.fail.halfLife)
}

func (h *hostPerf) estimate(inputTokens uint64, now time.Time) float64 {
	return h.ewmaReceipt.Value(now) + h.ewmaCTTFL.Value(now)*float64(inputTokens)
}

func (h *hostPerf) failureRate(now time.Time) (rate, volume float64) {
	failValue := h.fail.value(now)
	volume = h.success.value(now) + failValue
	if volume <= 0 {
		return 0, volume
	}
	return failValue / volume, volume
}
