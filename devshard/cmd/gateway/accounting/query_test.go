package accounting

import (
	"maps"
	"testing"
)

const secondTestEscrow = "escrow-2"

// TestEscrowsFromDifferentEpochsAreNotMerged pins the aggregation key. A rotation leaves escrows of
// adjacent epochs live at once and one participant holds a slot in both; merging them would report
// one epoch's work under the other's index.
func TestEscrowsFromDifferentEpochsAreNotMerged(t *testing.T) {
	book := newTestBook(t, 2)
	openTestEscrow(t, book, secondTestEscrow, testEpoch+1, 2)

	if err := book.RecordGhost(testEscrow, 0, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(%s): %v", testEscrow, err)
	}
	if err := book.RecordGhost(secondTestEscrow, 0, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(%s): %v", secondTestEscrow, err)
	}

	ghostsByEpoch := make(map[uint64]uint64)
	for _, record := range book.Query(QueryFilter{}) {
		if record.Participant == participantFor(0) {
			ghostsByEpoch[record.EpochIndex] = record.Dispositions[DispositionGhost]
		}
	}

	want := map[uint64]uint64{testEpoch: 1, testEpoch + 1: 1}
	if !maps.Equal(ghostsByEpoch, want) {
		t.Fatalf("ghosts by epoch = %v, want %v", ghostsByEpoch, want)
	}
}

func twoEpochBook(t *testing.T) *Book {
	t.Helper()
	book := newTestBook(t, 2)
	openTestEscrow(t, book, secondTestEscrow, testEpoch+1, 2)
	if err := book.RecordGhost(testEscrow, 0, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(%s): %v", testEscrow, err)
	}
	if err := book.RecordGhost(secondTestEscrow, 0, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(%s): %v", secondTestEscrow, err)
	}
	return book
}

func TestQueryNarrowsToOneEpoch(t *testing.T) {
	book := twoEpochBook(t)

	records := book.Query(QueryFilter{EpochIndex: testEpoch})

	for _, record := range records {
		if record.EpochIndex != testEpoch {
			t.Fatalf("epoch %d present under filter for epoch %d", record.EpochIndex, testEpoch)
		}
	}
	if len(records) == 0 {
		t.Fatal("filtering to a populated epoch returned nothing")
	}
}

func TestQueryNarrowsToOneParticipant(t *testing.T) {
	book := twoEpochBook(t)

	records := book.Query(QueryFilter{Participant: participantFor(1)})

	for _, record := range records {
		if record.Participant != participantFor(1) {
			t.Fatalf("participant %q present under filter for %q", record.Participant, participantFor(1))
		}
	}
	if len(records) != 2 {
		t.Fatalf("got %d records, want one per epoch for a participant holding a slot in both", len(records))
	}
}

// TestQueryWithoutFilterIsTheWholeLedger keeps the zero filter meaning "no constraint", which is what
// lets every caller take one path into the ledger.
func TestQueryWithoutFilterIsTheWholeLedger(t *testing.T) {
	book := twoEpochBook(t)

	if got, want := len(book.Query(QueryFilter{})), 4; got != want {
		t.Fatalf("got %d records, want %d: two participants in each of two epochs", got, want)
	}
}

func TestEpochsSummariseEveryParticipantOfTheEpoch(t *testing.T) {
	book := twoEpochBook(t)

	summaries := book.Epochs(QueryFilter{})

	if len(summaries) != 2 {
		t.Fatalf("got %d summaries, want one per epoch", len(summaries))
	}
	if summaries[0].EpochIndex != testEpoch || summaries[1].EpochIndex != testEpoch+1 {
		t.Fatalf("summaries came back as epochs %d, %d, want ascending %d, %d",
			summaries[0].EpochIndex, summaries[1].EpochIndex, testEpoch, testEpoch+1)
	}
	if summaries[0].Participants != 2 {
		t.Fatalf("epoch %d covered %d participants, want both slot holders", testEpoch, summaries[0].Participants)
	}
	if got := summaries[0].Dispositions[DispositionGhost]; got != 1 {
		t.Fatalf("epoch %d summed %d ghosts, want the one it recorded", testEpoch, got)
	}
}

// A participant holds the same slot id in several escrows at once, so a slot row that does not name
// its escrow is unreadable: two rows differ only by numbers the reader cannot attribute.
func TestSlotRowsNameTheirEscrow(t *testing.T) {
	book := newTestBook(t, 2)
	openTestEscrow(t, book, secondTestEscrow, testEpoch, 2)

	slots := book.Query(QueryFilter{Participant: participantFor(0)})[0].Slots

	escrows := make(map[string]int)
	for _, slot := range slots {
		escrows[slot.EscrowID]++
	}
	if len(escrows) != 2 || escrows[testEscrow] != 1 || escrows[secondTestEscrow] != 1 {
		t.Fatalf("slot rows by escrow = %v, want one row from each escrow the participant holds", escrows)
	}
}

// The ledger's own honesty check has to be able to fire, or a gap in instrumentation reads as a clean
// host. These are the fallbacks the engine and the scheduler emit when they cannot name a cause.
func TestTheUnknownReasonCheckCatchesWhatNothingCouldName(t *testing.T) {
	tests := []struct {
		name string
		key  CounterKey
		want bool
	}{
		{"a terminal the engine has no name for", CounterKey{Terminal: TerminalUnnamed}, true},
		{"an attempt the race never classified", CounterKey{Terminal: TerminalUnclassified}, true},
		{"a burn kind with no reason", CounterKey{Disposition: DispositionGhost}, true},
		{"a race that reported nothing", CounterKey{Terminal: TerminalUnreported}, false},
		{"a burn that named itself", CounterKey{Disposition: DispositionGhost, GhostReason: "poc_unavailable_host"}, false},
		{"an ordinary answer", CounterKey{Disposition: DispositionFinishedUsed, Terminal: "won"}, false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := namesNoReason(testCase.key); got != testCase.want {
				t.Errorf("namesNoReason() = %v, want %v", got, testCase.want)
			}
		})
	}
}
