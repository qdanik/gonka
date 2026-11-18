package accounting

import (
	"slices"
	"testing"

	"devshard/types"
)

// Test flow:
//  1. Open a book and observe the chain's latest nonce as 4.
//  2. Record nonce 4 as assigned under request "req-a".
//  3. Assert the slot-0 record reports 1 nonce and 1 request in flight.
//  4. Assert its unobserved count is 0, since a nonce out with a host is accounted for.
func TestANonceIsInFlightFromTheMomentItLeavesForAHost(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 4); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}

	if err := book.RecordAssigned(testEscrow, 4, "req-a"); err != nil {
		t.Fatalf("RecordAssigned(): %v", err)
	}

	records := book.Query(QueryFilter{Participant: participantFor(0)})
	if records[0].InFlight != 1 {
		t.Fatalf("in_flight = %d while the host is serving, want the nonce it is serving", records[0].InFlight)
	}
	if records[0].InFlightRequests != 1 {
		t.Fatalf("in_flight_requests = %d, want the request the nonce belongs to", records[0].InFlightRequests)
	}
	if records[0].Unobserved != 0 {
		t.Fatalf("unclassified = %d, want 0: a nonce out with a host is accounted for, not unseen", records[0].Unobserved)
	}
}

// Test flow:
//  1. Open a book and observe the chain's latest nonce as 4.
//  2. Record nonce 4 as assigned and assert overcounted and unobserved stay 0, with 1 nonce in flight.
//  3. Record the race for that nonce, mark it finished and assert overcounted and unobserved still stay 0.
func TestTheSlotIdentityHoldsFromHandOffToSettlement(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 4); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}

	assertIdentity := func(stage string) {
		t.Helper()
		record := book.Query(QueryFilter{Participant: participantFor(0)})[0]
		if record.Overcounted != 0 || record.Unobserved != 0 {
			t.Fatalf("%s: overclassified = %d, unclassified = %d, want the identity to hold exactly",
				stage, record.Overcounted, record.Unobserved)
		}
	}

	if err := book.RecordAssigned(testEscrow, 4, "req-a"); err != nil {
		t.Fatalf("RecordAssigned(): %v", err)
	}
	if record := book.Query(QueryFilter{Participant: participantFor(0)})[0]; record.InFlight != 1 {
		t.Fatalf("in_flight = %d out with a host, want the one nonce assigned", record.InFlight)
	}
	assertIdentity("out with a host")

	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 4, RequestID: "req-a", Sent: true}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	markFinished(t, book, testEscrow, 4)
	assertIdentity("the race reported")
}

// Test flow:
//  1. Record a race and then a timeout that settles nonce 4.
//  2. Record a late hand-off assignment for the same nonce, arriving after settlement.
//  3. Assert the slot-0 record's in-flight count stays 0.
func TestALateHandOffDoesNotReopenASettledNonce(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 4); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 4, RequestID: "req-a", Sent: true}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := book.RecordTimeout(testEscrow, 4, "execution", "applied", "execution"); err != nil {
		t.Fatalf("RecordTimeout(): %v", err)
	}

	if err := book.RecordAssigned(testEscrow, 4, "req-a"); err != nil {
		t.Fatalf("RecordAssigned(): %v", err)
	}

	records := book.Query(QueryFilter{Participant: participantFor(0)})
	if records[0].InFlight != 0 {
		t.Fatalf("in_flight = %d after the nonce settled, want 0", records[0].InFlight)
	}
}

// Test flow:
//  1. Assign nonce 4 to request "req-a" and report its race as won.
//  2. Observe the chain holding the record started, then finished, then sealed away.
//  3. Assert the nonce stays in flight while started, leaves flight once finished, and stays counted once after it is sealed.
func TestANonceLeavesFlightOnlyWhenTheChainAppliesItsFinish(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 4); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordAssigned(testEscrow, 4, "req-a"); err != nil {
		t.Fatalf("RecordAssigned(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 4, RequestID: "req-a", Sent: true, Usage: UsageWinner, Terminal: "won"}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	inFlight := make([]uint64, 0, 2)
	for _, status := range []types.InferenceStatus{types.StatusStarted, types.StatusFinished} {
		if err := book.ObserveInferences(testEscrow, map[uint64]*types.InferenceRecord{4: {Status: status}}); err != nil {
			t.Fatalf("ObserveInferences(): %v", err)
		}
		inFlight = append(inFlight, book.Query(QueryFilter{Participant: participantFor(0)})[0].InFlight)
	}
	if err := book.ObserveInferences(testEscrow, map[uint64]*types.InferenceRecord{}); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}
	sealed := book.Query(QueryFilter{Participant: participantFor(0)})[0]

	if !slices.Equal(inFlight, []uint64{1, 0}) {
		t.Fatalf("in_flight while started, then finished = %v, want [1 0]", inFlight)
	}
	if sealed.InFlight != 0 || sealed.Dispositions[DispositionFinishedUsed] != 1 {
		t.Fatalf("after sealing in_flight = %d, finished_used = %d, want 0 and 1", sealed.InFlight, sealed.Dispositions[DispositionFinishedUsed])
	}
}

// Test flow:
//  1. Open a live escrow and a second one that is then retired, both observed at latest nonce 8.
//  2. Assign slot-0 nonce 4 in each escrow and slot-0 nonce 8 in the retired one, and mark nonce 8 finished.
//  3. Assert the retired escrow's slot-0 record reports 1 in flight and 1 parked.
//  4. Assert the live escrow's slot-0 record reports 1 in flight and 0 parked.
//  5. Assert the epoch summary reports 2 in flight and 1 parked, since the finished nonce counts in neither.
func TestParkedInFlightIsTheRetiredEscrowsShareOfInFlight(t *testing.T) {
	book := newTestBook(t, 4)
	openTestEscrow(t, book, otherEscrow, testEpoch, 4)
	for _, escrowID := range []string{testEscrow, otherEscrow} {
		if err := book.ObserveLatestNonce(escrowID, 8); err != nil {
			t.Fatalf("ObserveLatestNonce(%s): %v", escrowID, err)
		}
	}
	book.RetireEscrow(otherEscrow)

	for escrowID, nonces := range map[string][]uint64{testEscrow: {4}, otherEscrow: {4, 8}} {
		for _, nonce := range nonces {
			if err := book.RecordAssigned(escrowID, nonce, "req-"+escrowID); err != nil {
				t.Fatalf("RecordAssigned(%s, %d): %v", escrowID, nonce, err)
			}
		}
	}
	markFinished(t, book, otherEscrow, 8)

	retired := book.Query(QueryFilter{Participant: participantFor(0), EscrowIDs: []string{otherEscrow}})[0]
	if retired.InFlight != 1 || retired.InFlightParked != 1 {
		t.Fatalf("retired escrow in_flight, in_flight_parked = %d, %d, want 1, 1", retired.InFlight, retired.InFlightParked)
	}
	live := book.Query(QueryFilter{Participant: participantFor(0), EscrowIDs: []string{testEscrow}})[0]
	if live.InFlight != 1 || live.InFlightParked != 0 {
		t.Fatalf("live escrow in_flight, in_flight_parked = %d, %d, want 1, 0", live.InFlight, live.InFlightParked)
	}
	summaries := book.Epochs(QueryFilter{})
	if len(summaries) != 1 || summaries[0].InFlight != 2 || summaries[0].InFlightParked != 1 {
		t.Fatalf("epoch summaries = %+v, want one with in_flight 2 and in_flight_parked 1", summaries)
	}
}
