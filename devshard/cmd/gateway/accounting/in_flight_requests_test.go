package accounting

import "testing"

// A race spends several nonces for one client, and a loser's nonce stays open long after the winner
// answered. Open nonces therefore say how much work is outstanding, not how many clients are waiting.
func TestOpenNoncesAndOpenRequestsAreCountedApart(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 8); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	// Nonces 4 and 8 both land on slot 0 in a group of four, and both belong to one client request.
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

// The winner answered the client and its nonce settled; the loser's did not. The request stays counted
// while that work is outstanding on the chain, which is what the number measures -- not a live client.
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
