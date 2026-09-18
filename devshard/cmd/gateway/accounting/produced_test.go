package accounting

import (
	"testing"

	"devshard/types"
)

func outputOf(t *testing.T, book *Book) uint64 {
	t.Helper()
	var output uint64
	for _, record := range book.Query(QueryFilter{}) {
		output += record.OutputTokens
	}
	return output
}

// The chain's output count is the host's own claim, and a host whose runtime reports usage before it has
// generated anything claims nothing. The gateway read the answer off the wire, so it is the gateway that
// says how long the answer was.
func TestTheAnswerIsCountedByTheGatewayNotByTheHostsClaim(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveInferences(testEscrow, map[uint64]*types.InferenceRecord{
		5: {Status: types.StatusFinished, ExecutorSlot: 1, InputLength: 400, InputTokens: 39642, OutputTokens: 0},
	}); err != nil {
		t.Fatalf("ObserveInferences(): %v", err)
	}

	recordProduced(t, book, testEscrow, 5, 834)

	if got := outputOf(t, book); got != 834 {
		t.Fatalf("output_tokens = %d, want the 834 the gateway counted off the stream", got)
	}
}

// An attempt that lost its race still cost the escrow and still produced tokens; the host's finish for it
// carries nothing, so dropping the gateway's count would lose them entirely.
func TestALostAttemptStillReportsWhatItProduced(t *testing.T) {
	book := newTestBook(t, 4)

	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 5, RequestID: "request-1", Sent: true, Finished: true, Usage: UsageLoser, OutputTokens: 834},
		{Nonce: 6, RequestID: "request-1", Sent: true, Finished: true, Usage: UsageWinner, OutputTokens: 4096},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	if got := outputOf(t, book); got != 4_930 {
		t.Fatalf("output_tokens = %d, want both attempts of the race counted", got)
	}
}

func TestAnAttemptReportedTwiceIsCountedOnce(t *testing.T) {
	book := newTestBook(t, 4)

	for range 3 {
		recordProduced(t, book, testEscrow, 5, 834)
	}

	if got := outputOf(t, book); got != 834 {
		t.Fatalf("output_tokens = %d, want 834 after the same attempt was reported three times", got)
	}
}

// Nothing re-reads an attempt the gateway already streamed, so the count it made has to be the one it saves.
func TestWhatTheGatewayCountedSurvivesARestart(t *testing.T) {
	book := newTestBook(t, 4)
	recordProduced(t, book, testEscrow, 5, 834)
	recordProduced(t, book, testEscrow, 6, 4096)

	restored := saveAndReload(t, book, openTestStore(t))

	if got := outputOf(t, restored); got != 4_930 {
		t.Fatalf("output_tokens = %d after the restart, want the 4930 the gateway had counted", got)
	}
	for _, record := range restored.Query(QueryFilter{}) {
		if record.Participant == participantFor(1) && record.OutputTokens != 834 {
			t.Fatalf("slot 1 came back with %d output tokens, want the 834 it produced", record.OutputTokens)
		}
		if record.Participant == participantFor(2) && record.OutputTokens != 4_096 {
			t.Fatalf("slot 2 came back with %d output tokens, want the 4096 it produced", record.OutputTokens)
		}
	}
}

// A sweep re-reads the chain over and over; it must not carry the gateway's own count away with it.
func TestAChainReadingDoesNotDisturbWhatTheGatewayCounted(t *testing.T) {
	book := newTestBook(t, 4)
	recordProduced(t, book, testEscrow, 5, 834)

	for range 3 {
		if err := book.ObserveInferences(testEscrow, map[uint64]*types.InferenceRecord{
			5: {Status: types.StatusFinished, ExecutorSlot: 1, InputLength: 400, InputTokens: 39642},
		}); err != nil {
			t.Fatalf("ObserveInferences(): %v", err)
		}
	}

	if got := outputOf(t, book); got != 834 {
		t.Fatalf("output_tokens = %d after three sweeps, want the 834 the gateway counted", got)
	}
}
