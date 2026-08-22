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
		t.Fatalf("in-flight requests = %d, want the 1 client actually waiting", records[0].InFlightRequests)
	}
}
