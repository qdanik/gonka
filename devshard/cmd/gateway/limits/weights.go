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
	return clampUnit(currentAvailable / full)
}

// escrowWeight = Σ over hosts of currentWeight[h] * hostShares[h] * (available[h] ? 1 : 0).
// hostShares[h] = slots(h,escrow)/totalSlots(h), normalized by the caller; raw slot counts here
// would count a participant once per escrow it serves instead of splitting it.
func escrowWeight(currentWeights, hostShares map[string]float64, available func(host string) bool) float64 {
	var sum float64
	for host, share := range hostShares {
		if available != nil && !available(host) {
			continue
		}
		sum += currentWeights[host] * share
	}
	return sum
}

// availableShare = Σ over available hosts of hostShares[h]: escrowWeight with every weight taken as one.
func availableShare(hostShares map[string]float64, available func(host string) bool) float64 {
	var sum float64
	for host, share := range hostShares {
		if available != nil && !available(host) {
			continue
		}
		sum += share
	}
	return sum
}
