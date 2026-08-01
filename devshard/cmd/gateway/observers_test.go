package main

import (
	"testing"

	"devshard/cmd/gateway/chain"
)

// The observer republishes every five seconds whether or not anything moved, so the narrator's whole
// job is deciding when there is something to say. Getting this wrong is either a flooded log or a
// silent one, and neither shows up anywhere else.
func TestThePhaseNarratorSpeaksOnlyOnChange(t *testing.T) {
	steady := chain.PhaseSnapshot{EpochIndex: 7, BlockHeight: 100}

	testCases := []struct {
		name string
		next chain.PhaseSnapshot
		want phaseChange
	}{
		{
			name: "the same snapshot again says nothing",
			next: chain.PhaseSnapshot{EpochIndex: 7, BlockHeight: 140},
			want: phaseChange{},
		},
		{
			name: "a new epoch is worth a line",
			next: chain.PhaseSnapshot{EpochIndex: 8, BlockHeight: 140},
			want: phaseChange{epoch: true},
		},
		{
			name: "requests becoming blocked is worth a line",
			next: chain.PhaseSnapshot{EpochIndex: 7, BlockHeight: 140, RequestsBlocked: true, BlockReason: chain.BlockReasonPoC},
			want: phaseChange{block: true},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			narrator := &phaseNarrator{}
			if first := narrator.advance(steady); !first.first {
				t.Fatal("the first snapshot was not reported as the first")
			}

			if got := narrator.advance(testCase.next); got != testCase.want {
				t.Fatalf("advance() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestThePhaseNarratorReportsABlockClearing(t *testing.T) {
	narrator := &phaseNarrator{}
	blocked := chain.PhaseSnapshot{EpochIndex: 7, RequestsBlocked: true, BlockReason: chain.BlockReasonPoC}
	narrator.advance(blocked)
	narrator.advance(blocked)

	cleared := narrator.advance(chain.PhaseSnapshot{EpochIndex: 7})

	if !cleared.block {
		t.Fatal("requests came back and the narrator said nothing, so the block looks permanent in the log")
	}
}
