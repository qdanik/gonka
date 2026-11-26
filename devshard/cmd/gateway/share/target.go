package share

import "math"

const (
	// FactorFloor keeps a host that serves little offered enough to recover.
	FactorFloor = 0.25
)

// Input is what one host brings to the targets of its model.
type Input struct {
	Weight float64
	Counts Decayed
	// Room is the number of requests the host's two windows admit now, +Inf when the host has no window.
	Room float64
}

// Share is the target of one host and the figures it is derived from.
type Share struct {
	WeightShare    float64
	ServeRatio     float64
	CapacityFactor float64
	TargetShare    float64
	OfferedShare   float64
	// Surplus is offered minus target times the total offered, in nonces, clamped to one window either way.
	Surplus float64
}

// Targets returns one Share per host. Without balance the target is the weight share; with it the weight share is scaled by the capacity factor and normalised. minVolume is the denominator below which a host's serve ratio reads 1.
func Targets(balance bool, hosts map[string]Input, minVolume float64) map[string]Share {
	var weightSum, totalOffered float64
	for _, input := range hosts {
		weightSum += input.Weight
		totalOffered += input.Counts.Offered
	}
	shares := make(map[string]Share, len(hosts))
	var scaledSum float64
	for name, input := range hosts {
		share := Share{CapacityFactor: 1, ServeRatio: 1}
		if weightSum > 0 {
			share.WeightShare = input.Weight / weightSum
		}
		if volume := input.Counts.Served + input.Counts.Failed + input.Counts.Passed; volume >= minVolume && volume > 0 {
			share.ServeRatio = input.Counts.Served / volume
		}
		if balance {
			share.CapacityFactor = math.Min(1, math.Max(FactorFloor, share.ServeRatio))
		}
		shares[name] = share
		scaledSum += share.WeightShare * share.CapacityFactor
	}
	for name, input := range hosts {
		share := shares[name]
		if scaledSum > 0 {
			share.TargetShare = share.WeightShare * share.CapacityFactor / scaledSum
		}
		if totalOffered > 0 {
			share.OfferedShare = input.Counts.Offered / totalOffered
		}
		if scaledSum > 0 {
			share.Surplus = math.Max(-input.Room, math.Min(input.Room, input.Counts.Offered-share.TargetShare*totalOffered))
		}
		shares[name] = share
	}
	return shares
}
