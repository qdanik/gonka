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
	registry := New(Deps{
		ServingSessions: newSessions(map[string]*fakeSession{
			"disputed": sessionWithChallenges(3),
			"settled":  sessionWithChallenges(0),
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

	if stalled != 1 {
		t.Fatalf("stalled = %d, want only the escrow holding a dispute", stalled)
	}
	if drained != 0 || failed != 0 {
		t.Fatalf("drained/failed = %d/%d, want none: the fake session hands back no user session", drained, failed)
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
