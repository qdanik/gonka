package perf

import "time"

// ejectionState is not goroutine-safe; the caller (Tracker) locks around all access.
type ejectionState struct {
	ejectedUntil  time.Time
	ejectionCount int
}

type ejectionPolicy struct {
	consecutiveFailThreshold int
	failureRateThreshold     float64
	failureRateMinVolume     float64
	base                     time.Duration
	max                      time.Duration
}

func newEjectionPolicy(consecutiveFailThreshold int, failureRateThreshold, failureRateMinVolume float64, base, max time.Duration) ejectionPolicy {
	return ejectionPolicy{
		consecutiveFailThreshold: consecutiveFailThreshold,
		failureRateThreshold:     failureRateThreshold,
		failureRateMinVolume:     failureRateMinVolume,
		base:                     base,
		max:                      max,
	}
}

func (p ejectionPolicy) evaluate(h *hostPerf, state *ejectionState, now time.Time) {
	rate, volume := h.failureRate(now)
	triggered := h.consecutiveFail >= p.consecutiveFailThreshold ||
		(volume >= p.failureRateMinVolume && rate >= p.failureRateThreshold)

	if triggered {
		if !state.ejected(now) {
			state.ejectionCount++
			state.ejectedUntil = now.Add(min(p.base*time.Duration(state.ejectionCount), p.max))
			h.resetOutcomeCounters()
		}
		return
	}
	if !state.ejected(now) {
		state.decayEjectionCount(p.max, now)
	}
}

func (s *ejectionState) ejected(now time.Time) bool {
	return now.Before(s.ejectedUntil)
}

// decayEjectionCount relaxes one rung per full healthy window since the last
// ejection ended, advancing the anchor so it can't cascade faster than that.
func (s *ejectionState) decayEjectionCount(window time.Duration, now time.Time) {
	for s.ejectionCount > 0 && !s.ejectedUntil.IsZero() && now.Sub(s.ejectedUntil) >= window {
		s.ejectionCount--
		s.ejectedUntil = s.ejectedUntil.Add(window)
	}
}
