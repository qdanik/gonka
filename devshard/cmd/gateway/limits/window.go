package limits

import (
	"time"

	"devshard/cmd/gateway/internal/safemath"
)

type key struct {
	participant string
	model       string
}

// window is one congestion window and what is in flight against it. Peak is the high-water mark since the last adjustment.
type window struct {
	tokens   float64
	inflight int64
	peak     int64
}

func fits(inflight, cost int64, window float64) bool {
	return float64(inflight)+float64(cost) <= window
}

func (w *window) take(tokens int64) {
	w.inflight = safemath.AddSaturating(w.inflight, tokens)
	if w.inflight > w.peak {
		w.peak = w.inflight
	}
}

func (w *window) give(tokens int64) {
	w.inflight = max(w.inflight-tokens, 0)
}

func (w *window) reopen(tokens int64) {
	w.tokens = float64(tokens)
	w.peak = w.inflight
}

type hostState struct {
	bounds                  ModelWindows
	input                   window
	output                  window
	consecutiveCutoffFaults int
	openUntil               time.Time
	backoffCount            int
	halfOpen                bool
	lastUsed                time.Time
}

func (s *hostState) idle() bool {
	return s.input.inflight == 0 && s.output.inflight == 0
}
