package accounting

import (
	"testing"

	"devshard/types"
)

func TestTheEpochSummaryCarriesWhatIsInFlight(t *testing.T) {
	book := NewBook(nil)
	slots := make([]types.SlotAssignment, 0, 4)
	for slotID := range uint32(4) {
		slots = append(slots, types.SlotAssignment{SlotID: slotID, ValidatorAddress: participantFor(int(slotID))})
	}
	if err := book.OpenEscrow(EscrowMetadata{EscrowID: "e1", CreationEpoch: 9, Model: "m", Slots: slots}); err != nil {
		t.Fatalf("OpenEscrow(): %v", err)
	}
	if err := book.ObserveLatestNonce("e1", 8); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace("e1", []Attempt{
		{Nonce: 1, RequestID: "a", Sent: true},
		{Nonce: 2, RequestID: "a", Sent: true},
		{Nonce: 3, RequestID: "b", Sent: true},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	summaries := book.Epochs(QueryFilter{})

	if len(summaries) != 1 {
		t.Fatalf("got %d epoch summaries, want one", len(summaries))
	}
	if summaries[0].InFlight != 3 {
		t.Fatalf("in_flight = %d, want the 3 nonces out with hosts", summaries[0].InFlight)
	}
	if summaries[0].InFlightRequests != 2 {
		t.Fatalf("in_flight_requests = %d, want the 2 clients waiting: one of them is racing two hosts",
			summaries[0].InFlightRequests)
	}
}

func TestTheEstimateSeparatesWhatFinishedFromWhatDidNot(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 12); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}

	if err := book.ObserveInferences(testEscrow, map[uint64]*types.InferenceRecord{
		1: {Status: types.StatusFinished, InputLength: 4_000, MaxTokens: 256, InputTokens: 900, OutputTokens: 100},
		2: {Status: types.StatusTimedOut, InputLength: 8_000, MaxTokens: 256},
		3: {Status: types.StatusStarted, InputLength: 12_000, MaxTokens: 256},
	}); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}

	var totals nonceTotals
	for _, record := range book.Query(QueryFilter{}) {
		totals.add(record.nonceTotals)
	}

	if totals.EstimatedInput != 1_000 {
		t.Fatalf("estimated_input_tokens = %d, want the finished nonce's 4000 bytes", totals.EstimatedInput)
	}
	if totals.EstimatedError != 5_000 {
		t.Fatalf("estimated_error_tokens = %d, want the 8000 and 12000 bytes that never came back", totals.EstimatedError)
	}
	if totals.CountedNonces != 1 {
		t.Fatalf("counted_nonces = %d, want the one the chain counted tokens for", totals.CountedNonces)
	}
}
