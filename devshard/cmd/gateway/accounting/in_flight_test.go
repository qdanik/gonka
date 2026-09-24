package accounting

import "testing"

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
//  3. Record the race finishing that nonce and assert overcounted and unobserved still stay 0.
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

	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 4, RequestID: "req-a", Sent: true, Finished: true}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
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
