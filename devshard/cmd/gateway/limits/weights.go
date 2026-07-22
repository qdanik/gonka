// Package limits provides capacity-scaled gateway admission and a per-host AIMD concurrency limiter.
package limits

import "math"

// weightConcurrencyLimit = floor(weight * per10000 / 10000); 0 when per10000<=0 or weight<=0.
func weightConcurrencyLimit(weight, per10000 float64) int64 {
	if weight <= 0 || per10000 <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) || math.IsNaN(per10000) || math.IsInf(per10000, 0) {
		return 0
	}
	limit := math.Floor(weight * per10000 / 10000)
	if limit >= float64(math.MaxInt64) {
		return math.MaxInt64
	}
	return int64(limit)
}

// scaleFactor = clamp(currentAvailable/full, 0, 1); returns 1.0 when full<=0 (baseline unlimited).
func scaleFactor(currentAvailable, full float64) float64 {
	if full <= 0 {
		return 1
	}
	ratio := currentAvailable / full
	switch {
	case math.IsNaN(ratio): // fail closed like clampUnit: a corrupt ratio grants no capacity (>1 and <0 already cover ±Inf)
		return 0
	case ratio < 0:
		return 0
	case ratio > 1:
		return 1
	default:
		return ratio
	}
}

// escrowWeight = Σ over hosts of currentWeight[h] * membershipShare[h] * (available[h] ? 1 : 0).
// membershipShare[h] = slots(h,escrow)/totalSlots(h), already normalized by the caller.
func escrowWeight(currentWeights, membershipShare map[string]float64, available func(host string) bool) float64 {
	var sum float64
	for host, share := range membershipShare {
		if available != nil && !available(host) {
			continue
		}
		sum += currentWeights[host] * share
	}
	return sum
}
