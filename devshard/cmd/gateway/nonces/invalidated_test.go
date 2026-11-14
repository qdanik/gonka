package nonces

import (
	"slices"
	"testing"

	"devshard/types"
)

type invalidatedSpy struct{ told []uint64 }

func (s *invalidatedSpy) RecordInvalidated(_ string, nonce uint64) { s.told = append(s.told, nonce) }

// Test flow:
//  1. Wire a sink, then sweep an escrow whose records hold one invalidated, one challenged and one validated inference.
//  2. Sweep the same escrow again with a second invalidated record.
//  3. Assert the sink was told of each invalidated nonce once and of no other status.
func TestASweepTellsTheSinkOfEachInvalidatedNonceOnce(t *testing.T) {
	ledger := newLedgerForTest(t)
	spy := &invalidatedSpy{}
	ledger.SetInvalidatedRecords(spy)
	state := types.EscrowState{Inferences: map[uint64]*types.InferenceRecord{
		3: {Status: types.StatusInvalidated},
		4: {Status: types.StatusChallenged},
		5: {Status: types.StatusValidated},
	}}

	ledger.observeEscrowState("escrow-1", state)
	state.Inferences[6] = &types.InferenceRecord{Status: types.StatusInvalidated}
	ledger.observeEscrowState("escrow-1", state)

	slices.Sort(spy.told)
	if !slices.Equal(spy.told, []uint64{3, 6}) {
		t.Fatalf("sink told of %v, want [3 6] once each", spy.told)
	}
}

// Test flow:
//  1. Sweep an escrow with an invalidated record while no sink is wired.
//  2. Wire a sink, sweep again, retire the escrow and sweep a third time.
//  3. Assert nothing panicked and the sink was told of the nonce on the second and again on the third sweep, once the retired escrow was forgotten.
func TestAnInvalidatedRecordWithNoSinkIsIgnoredAndARetiredEscrowIsAnnouncedAgain(t *testing.T) {
	ledger := newLedgerForTest(t)
	state := types.EscrowState{Inferences: map[uint64]*types.InferenceRecord{3: {Status: types.StatusInvalidated}}}
	ledger.observeEscrowState("escrow-1", state)

	spy := &invalidatedSpy{}
	ledger.SetInvalidatedRecords(spy)
	ledger.observeEscrowState("escrow-1", state)
	ledger.forgetEscrow("escrow-1")
	ledger.observeEscrowState("escrow-1", state)

	if !slices.Equal(spy.told, []uint64{3, 3}) {
		t.Fatalf("sink told of %v, want [3 3]", spy.told)
	}
}
