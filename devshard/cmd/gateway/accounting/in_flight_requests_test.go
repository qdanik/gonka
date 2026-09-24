package accounting

import "testing"

// Test flow:
//  1. Open a book and observe the chain's latest nonce as 8.
//  2. Record a race of nonces 4 and 8, both landing on slot 0 and both belonging to request "req-a".
//  3. Assert the slot-0 record reports 2 nonces in flight.
//  4. Assert it reports 1 in-flight request, since both nonces belong to the same request.
func TestOpenNoncesAndOpenRequestsAreCountedApart(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 8); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 4, RequestID: "req-a", Sent: true},
		{Nonce: 8, RequestID: "req-a", Sent: true},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	records := book.Query(QueryFilter{Participant: participantFor(0)})

	if len(records) != 1 {
		t.Fatalf("got %d participant records, want one", len(records))
	}
	if records[0].InFlight != 2 {
		t.Fatalf("in flight = %d, want the 2 nonces still open", records[0].InFlight)
	}
	if records[0].InFlightRequests != 1 {
		t.Fatalf("in-flight requests = %d, want the 1 request the two nonces belong to", records[0].InFlightRequests)
	}
}

// Test flow:
//  1. Record a race of nonces 4 (finished, the winner) and 8 (unfinished, the loser) under request "req-a".
//  2. Assert the record reports 1 nonce and 1 request still in flight while the loser is unfinished.
//  3. Mark nonce 8 finished.
//  4. Assert the record now reports 0 nonces and 0 requests in flight.
func TestARequestStaysCountedWhileALoserNonceIsUnfinished(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 8); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 4, RequestID: "req-a", Sent: true, Finished: true},
		{Nonce: 8, RequestID: "req-a", Sent: true},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	served := book.Query(QueryFilter{Participant: participantFor(0)})
	if served[0].InFlight != 1 || served[0].InFlightRequests != 1 {
		t.Fatalf("open = %d nonces / %d requests, want 1 and 1: the loser has not settled",
			served[0].InFlight, served[0].InFlightRequests)
	}

	if err := book.MarkFinished(testEscrow, []uint64{8}); err != nil {
		t.Fatalf("MarkFinished(): %v", err)
	}

	settled := book.Query(QueryFilter{Participant: participantFor(0)})
	if settled[0].InFlight != 0 || settled[0].InFlightRequests != 0 {
		t.Fatalf("open = %d nonces / %d requests after the loser settled, want none",
			settled[0].InFlight, settled[0].InFlightRequests)
	}
}
