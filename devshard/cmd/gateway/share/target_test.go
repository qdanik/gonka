package share

import (
	"math"
	"testing"
)

func approx(got, want float64) bool { return math.Abs(got-want) < 1e-9 }

func fixtureHosts() map[string]Input {
	return map[string]Input{
		"alpha": {Weight: 60, Room: 30, Counts: Decayed{Assigned: 120, Offered: 100, Served: 40, Failed: 10, Passed: 50, Excused: 5}},
		"beta":  {Weight: 30, Room: 20, Counts: Decayed{Assigned: 70, Offered: 60, Served: 54, Failed: 6, Passed: 0.5, Excused: 3}},
		"gamma": {Weight: 10, Room: 10, Counts: Decayed{Assigned: 30, Offered: 40, Served: 2, Failed: 30, Passed: 60, Excused: 2}},
	}
}

// Test flow:
//  1. Compute weight-mode targets for three hosts with every count non-zero.
//  2. Assert targets equal weight shares, the offered share is each host's part of the offers, and the surplus is offered minus target times the total.
func TestWeightTargetsAreTheWeightShares(t *testing.T) {
	shares := Targets(false, fixtureHosts(), 20)
	for name, want := range map[string]float64{"alpha": 0.6, "beta": 0.3, "gamma": 0.1} {
		if !approx(shares[name].WeightShare, want) || !approx(shares[name].TargetShare, want) || shares[name].CapacityFactor != 1 {
			t.Errorf("%s = %+v, want weight share and target %v and factor 1", name, shares[name], want)
		}
	}
	if !approx(shares["alpha"].OfferedShare, 0.5) {
		t.Errorf("alpha offered share = %v, want 0.5", shares["alpha"].OfferedShare)
	}
	if !approx(shares["alpha"].Surplus, 100-0.6*200) || !approx(shares["gamma"].Surplus, 10) {
		t.Errorf("surplus alpha %v gamma %v, want -20 and 10 (gamma clamped to its room from 20)", shares["alpha"].Surplus, shares["gamma"].Surplus)
	}
}

// Test flow:
//  1. Compute balance-mode targets on the fixture: alpha serves 40 of 100, beta 54 of 60.5, gamma 2 of 92 which is under the floor.
//  2. Assert the serve ratios, the capacity factors with the 0.25 floor, the normalised targets summing to 1 and each target inside 0.25 to 4 times its weight share.
func TestBalanceTargetsScaleByTheServeRatioWithAFloor(t *testing.T) {
	shares := Targets(true, fixtureHosts(), 20)
	if !approx(shares["alpha"].ServeRatio, 0.4) || !approx(shares["beta"].ServeRatio, 54/60.5) || !approx(shares["gamma"].ServeRatio, 2.0/92) {
		t.Errorf("serve ratios = %v %v %v", shares["alpha"].ServeRatio, shares["beta"].ServeRatio, shares["gamma"].ServeRatio)
	}
	if !approx(shares["alpha"].CapacityFactor, 0.4) || !approx(shares["gamma"].CapacityFactor, FactorFloor) {
		t.Errorf("factors alpha %v gamma %v, want 0.4 and the floor", shares["alpha"].CapacityFactor, shares["gamma"].CapacityFactor)
	}
	scaled := 0.6*0.4 + 0.3*(54/60.5) + 0.1*FactorFloor
	var sum float64
	for name, share := range shares {
		sum += share.TargetShare
		if share.TargetShare < FactorFloor*share.WeightShare-1e-9 || share.TargetShare > 4*share.WeightShare+1e-9 {
			t.Errorf("%s target %v outside 0.25 to 4 times weight share %v", name, share.TargetShare, share.WeightShare)
		}
	}
	if !approx(sum, 1) || !approx(shares["alpha"].TargetShare, 0.6*0.4/scaled) {
		t.Errorf("targets sum %v alpha %v, want 1 and %v", sum, shares["alpha"].TargetShare, 0.6*0.4/scaled)
	}
}

// Test flow:
//  1. Give a host a serve volume below the minimum volume with all-failed counts, and one at the minimum.
//  2. Assert the first reads serve ratio 1 and factor 1, the second its measured ratio.
func TestBalanceReadsFullServeRatioBelowTheMinimumVolume(t *testing.T) {
	hosts := map[string]Input{
		"few":  {Weight: 1, Room: 5, Counts: Decayed{Assigned: 19, Offered: 19, Served: 0, Failed: 19, Passed: 0, Excused: 1}},
		"many": {Weight: 1, Room: 5, Counts: Decayed{Assigned: 20, Offered: 20, Served: 10, Failed: 5, Passed: 5, Excused: 1}},
	}
	shares := Targets(true, hosts, 20)
	if shares["few"].ServeRatio != 1 || shares["few"].CapacityFactor != 1 {
		t.Errorf("few = %+v, want ratio and factor 1 below the minimum volume", shares["few"])
	}
	if !approx(shares["many"].ServeRatio, 0.5) {
		t.Errorf("many serve ratio = %v, want 0.5", shares["many"].ServeRatio)
	}
}

// Test flow:
//  1. Iterate offer = target x total for a host with entitlement 400 of 800 and capacity 100 against a peer that serves everything, 40 steps.
//  2. Assert the host's offer settles at sqrt(c x K) of the normalised system without oscillating: the last ten steps differ by under 1%.
func TestBalanceConvergesWithoutOscillation(t *testing.T) {
	const total = 800.0
	offered := map[string]float64{"slow": 400, "fast": 400}
	var history []float64
	for range 40 {
		hosts := map[string]Input{}
		for name, weight := range map[string]float64{"slow": 1, "fast": 1} {
			served := offered[name]
			if name == "slow" {
				served = math.Min(offered[name], 100)
			}
			hosts[name] = Input{Weight: weight, Room: math.Inf(1), Counts: Decayed{Offered: offered[name], Served: served, Failed: offered[name] - served, Assigned: offered[name], Excused: 1, Passed: 1}}
		}
		shares := Targets(true, hosts, 20)
		for name := range offered {
			offered[name] = shares[name].TargetShare * total
		}
		history = append(history, offered["slow"])
	}
	last := history[len(history)-1]
	for _, value := range history[len(history)-10:] {
		if math.Abs(value-last) > 0.01*last {
			t.Fatalf("offer still moving: %v", history[len(history)-10:])
		}
	}
	if last >= total/2 || last <= 100 {
		t.Errorf("settled offer %v, want between the capacity 100 and the entitlement 400", last)
	}
}
