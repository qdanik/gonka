package registry

import (
	"context"
	"testing"

	"devshard/types"
)

func sessionWithChallenges(open int) *fakeSession {
	inferences := map[uint64]*types.InferenceRecord{}
	for nonce := range open {
		inferences[uint64(nonce)+1] = &types.InferenceRecord{Status: types.StatusChallenged}
	}
	inferences[uint64(open)+1] = &types.InferenceRecord{Status: types.StatusFinished}
	return &fakeSession{escrowState: types.EscrowState{Inferences: inferences}}
}

// A dispute costs a nonce to carry votes for; an escrow holding none must never be asked to spend one.
func TestTheDrainCountsOnlyEscrowsHoldingADispute(t *testing.T) {
	disputed, settled := sessionWithChallenges(3), sessionWithChallenges(0)
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{
			"disputed": disputed,
			"settled":  settled,
		}).open,
		Membership: newRecordingMembership(),
		Now:        fixedClock(),
	})
	t.Cleanup(func() { registry.Close() })
	for _, escrowID := range []string{"disputed", "settled"} {
		if err := registry.Add(context.Background(), escrowID, "model-a"); err != nil {
			t.Fatalf("Add(%q): %v", escrowID, err)
		}
	}

	stalled, drained, failed := registry.DrainStalledChallenges(context.Background(), 8)

	if stalled != 1 || drained != 1 || failed != 0 {
		t.Fatalf("stalled/drained/failed = %d/%d/%d, want only the escrow holding a dispute drained", stalled, drained, failed)
	}
	if disputed.pendingDiffCalls.Load() != 1 {
		t.Fatalf("disputed escrow sent %d diffs, want the votes carried once", disputed.pendingDiffCalls.Load())
	}
	if settled.pendingDiffCalls.Load() != 0 {
		t.Fatalf("settled escrow sent %d diffs, want none", settled.pendingDiffCalls.Load())
	}
}

// A zero budget turns the drain off, the way it turns the timeout sweep off.
func TestAZeroBudgetDrainsNothing(t *testing.T) {
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{"disputed": sessionWithChallenges(2)}).open,
		Membership:      newRecordingMembership(),
		Now:             fixedClock(),
	})
	t.Cleanup(func() { registry.Close() })
	if err := registry.Add(context.Background(), "disputed", "model-a"); err != nil {
		t.Fatalf("Add(): %v", err)
	}

	stalled, _, _ := registry.DrainStalledChallenges(context.Background(), 0)

	if stalled != 0 {
		t.Fatalf("stalled = %d, want the drain to stay off at a zero budget", stalled)
	}
}
