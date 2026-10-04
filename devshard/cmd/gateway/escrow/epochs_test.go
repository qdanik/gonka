package escrow

import (
	"testing"

	"devshard/cmd/gateway/chain"
)

// Test flow:
//  1. Table-driven: a snapshot before PoC, inside PoC, after the switch, and one whose effective epoch is unknown.
//  2. Assert effectiveEpoch reads the snapshot's effective epoch, or the latest when it is unknown, and bridgeLabel is one past it.
func TestTheBridgeLabelIsTheEpochAfterTheEffectiveOne(t *testing.T) {
	testCases := []struct {
		name          string
		snapshot      chain.PhaseSnapshot
		wantEffective uint64
	}{
		{name: "before PoC", snapshot: chain.PhaseSnapshot{EpochIndex: 7, EffectiveEpochIndex: 7}, wantEffective: 7},
		{name: "inside PoC", snapshot: chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 7}, wantEffective: 7},
		{name: "after the switch", snapshot: chain.PhaseSnapshot{EpochIndex: 8, EffectiveEpochIndex: 8}, wantEffective: 8},
		{name: "unknown reads the latest", snapshot: chain.PhaseSnapshot{EpochIndex: 8}, wantEffective: 8},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := effectiveEpoch(testCase.snapshot); got != testCase.wantEffective {
				t.Fatalf("effectiveEpoch(%s) = %d, want %d", testCase.name, got, testCase.wantEffective)
			}
			if got := bridgeLabel(testCase.snapshot); got != testCase.wantEffective+1 {
				t.Fatalf("bridgeLabel(%s) = %d, want %d", testCase.name, got, testCase.wantEffective+1)
			}
		})
	}
}
