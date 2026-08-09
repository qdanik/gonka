package accounting

import (
	"context"
	"errors"
	"testing"
	"time"

	"devshard/types"
)

const (
	testEscrow = "escrow-1"
	testModel  = "model-a"
	testEpoch  = 7
)

func newTestBook(t *testing.T, groupSize int) *Book {
	t.Helper()
	book := NewBook(func() time.Time { return time.Unix(0, 0).UTC() })
	openTestEscrow(t, book, testEscrow, testEpoch, groupSize)
	return book
}

// openTestEscrow gives every escrow the same slot-to-participant mapping, so a test that opens a
// second one varies the epoch and nothing else.
func openTestEscrow(t *testing.T, book *Book, escrowID string, epoch uint64, groupSize int) {
	t.Helper()
	slots := make([]types.SlotAssignment, 0, groupSize)
	for slotID := range groupSize {
		slots = append(slots, types.SlotAssignment{
			SlotID:           uint32(slotID),
			ValidatorAddress: participantFor(slotID),
		})
	}
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: escrowID, Model: testModel, CreationEpoch: epoch, Slots: slots,
	}); err != nil {
		t.Fatalf("OpenEscrow(%s): %v", escrowID, err)
	}
}

func participantFor(slotID int) string {
	return string(rune('A' + slotID))
}

// slotOfNonce mirrors the chain's convention in the test so an expectation names the slot it means
// rather than repeating the arithmetic under test.
func slotOfNonce(nonce uint64, groupSize int) uint32 { return uint32(nonce % uint64(groupSize)) }

func dispositionsOfSlot(t *testing.T, book *Book, slotID uint32) map[Disposition]uint64 {
	t.Helper()
	for _, record := range book.Query(QueryFilter{}) {
		for _, slot := range record.Slots {
			if slot.SlotID == slotID {
				return slot.Dispositions
			}
		}
	}
	t.Fatalf("slot %d absent from the ledger", slotID)
	return nil
}

func assertDisposition(t *testing.T, book *Book, nonce uint64, groupSize int, want Disposition) {
	t.Helper()
	dispositions := dispositionsOfSlot(t, book, slotOfNonce(nonce, groupSize))
	if dispositions[want] != 1 {
		t.Fatalf("dispositions = %v, want exactly one %s", dispositions, want)
	}
	var total uint64
	for _, count := range dispositions {
		total += count
	}
	if total != 1 {
		t.Fatalf("dispositions = %v, want one nonce classified once", dispositions)
	}
}

func TestGhostIsCountedWithItsReason(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.RecordGhost(testEscrow, 5, "participant_throttled_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	assertDisposition(t, book, 5, 4, DispositionGhost)

	records := book.Query(QueryFilter{})
	for _, record := range records {
		for _, counter := range record.Counters {
			if counter.Disposition == DispositionGhost && counter.GhostReason != "participant_throttled_no_send" {
				t.Fatalf("ghost reason = %q, want the scheduler's own label", counter.GhostReason)
			}
		}
	}
}

func TestFinishedNoncesSplitByWhoUsedTheAnswer(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		usage Usage
		want  Disposition
	}{
		{name: "winner", usage: UsageWinner, want: DispositionFinishedUsed},
		{name: "loser", usage: UsageLoser, want: DispositionFinishedUnused},
		{name: "unknown", usage: UsageUnknown, want: DispositionFinishedUsageUnknown},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			book := newTestBook(t, 4)
			attempt := Attempt{Nonce: 9, Sent: true, Finished: true, Usage: testCase.usage}
			if err := book.RecordRace(testEscrow, []Attempt{attempt}); err != nil {
				t.Fatalf("RecordRace(): %v", err)
			}
			assertDisposition(t, book, 9, 4, testCase.want)
		})
	}
}

// An unfinished nonce is worth nothing until its timeout settles: classifying it early would name a
// disposition the settlement is still free to change.
func TestUnfinishedNonceStaysPendingUntilItsTimeoutSettles(t *testing.T) {
	book := newTestBook(t, 4)
	attempt := Attempt{Nonce: 6, Sent: true, Finished: false}
	if err := book.RecordRace(testEscrow, []Attempt{attempt}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	slot := slotOfNonce(6, 4)
	if dispositions := dispositionsOfSlot(t, book, slot); len(dispositions) != 0 {
		t.Fatalf("dispositions = %v, want none while the timeout is unsettled", dispositions)
	}
	if pending := pendingOfSlot(t, book, slot); pending != 1 {
		t.Fatalf("pending = %d, want the nonce awaiting its timeout", pending)
	}

	if err := book.RecordTimeout(testEscrow, 6, "execution", "completed", "none"); err != nil {
		t.Fatalf("RecordTimeout(): %v", err)
	}
	assertDisposition(t, book, 6, 4, DispositionUnfinishedExecution)
	if pending := pendingOfSlot(t, book, slot); pending != 0 {
		t.Fatalf("pending = %d, want the nonce classified", pending)
	}
}

func TestUnfinishedNonceThatWasNeverAcknowledgedCountsAsRefused(t *testing.T) {
	book := newTestBook(t, 4)
	attempt := Attempt{Nonce: 6, Sent: true, Finished: false}
	if err := book.RecordRace(testEscrow, []Attempt{attempt}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := book.RecordTimeout(testEscrow, 6, TimeoutKindRefused, "completed", "none"); err != nil {
		t.Fatalf("RecordTimeout(): %v", err)
	}
	assertDisposition(t, book, 6, 4, DispositionUnfinishedRefused)
}

// The counters describe the present, so a nonce that changes disposition must move rather than be
// counted twice. This is the property that keeps the ledger's total equal to the nonces it saw.
func TestReclassificationMovesANonceRatherThanDuplicatingIt(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 8, Sent: true, Finished: true, Usage: UsageLoser}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	assertDisposition(t, book, 8, 4, DispositionFinishedUnused)

	// The same race is reported again with the answer now known to have reached the client.
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 8, Sent: true, Finished: true, Usage: UsageWinner}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	assertDisposition(t, book, 8, 4, DispositionFinishedUsed)
}

func TestAFactForAnUnopenedEscrowIsRefusedAndCounted(t *testing.T) {
	book := newTestBook(t, 4)
	err := book.RecordGhost("escrow-unknown", 1, "poc_unavailable_host")
	if !errors.Is(err, ErrUnknownEscrow) {
		t.Fatalf("RecordGhost() = %v, want ErrUnknownEscrow", err)
	}
	if rejected := book.Rejected(); rejected != 1 {
		t.Fatalf("Rejected() = %d, want the dropped fact counted", rejected)
	}
}

func TestAssignedFollowsTheChainsModuloConvention(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		latest    uint64
		groupSize uint32
		slotID    uint32
		want      uint64
	}{
		{name: "slot zero takes the multiples", latest: 10, groupSize: 4, slotID: 0, want: 2},
		{name: "first slot", latest: 10, groupSize: 4, slotID: 1, want: 3},
		{name: "last slot", latest: 10, groupSize: 4, slotID: 3, want: 2},
		{name: "slot beyond the latest nonce", latest: 2, groupSize: 4, slotID: 3, want: 0},
		{name: "nothing spent", latest: 0, groupSize: 4, slotID: 1, want: 0},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := assignedForSlot(testCase.latest, testCase.groupSize, testCase.slotID); got != testCase.want {
				t.Fatalf("assignedForSlot(%d, %d, %d) = %d, want %d",
					testCase.latest, testCase.groupSize, testCase.slotID, got, testCase.want)
			}
		})
	}
}

// Every assigned nonce is accounted for exactly once: classified, pending, or never seen.
func TestEveryAssignedNonceIsAccountedForExactlyOnce(t *testing.T) {
	const groupSize = 4
	book := newTestBook(t, groupSize)
	if err := book.ObserveLatestNonce(testEscrow, 12); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordGhost(testEscrow, 5, "participant_throttled_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 9, Sent: true, Finished: true, Usage: UsageWinner},
		{Nonce: 6, Sent: true, Finished: false},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	for _, record := range book.Query(QueryFilter{}) {
		for _, slot := range record.Slots {
			var classified uint64
			for _, count := range slot.Dispositions {
				classified += count
			}
			total := classified + slot.Pending + slot.Unobserved
			if slot.Overcounted != 0 {
				t.Fatalf("slot %d overcounted %d nonces", slot.SlotID, slot.Overcounted)
			}
			if total != slot.Assigned {
				t.Fatalf("slot %d: classified %d + pending %d + unobserved %d = %d, want assigned %d",
					slot.SlotID, classified, slot.Pending, slot.Unobserved, total, slot.Assigned)
			}
		}
	}
}

func TestChainTalliesTravelBesideTheLedgersOwn(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveHostStats(testEscrow, 2, types.HostStats{Missed: 3, Invalid: 1}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}
	for _, record := range book.Query(QueryFilter{}) {
		if record.Participant != participantFor(2) {
			continue
		}
		if record.ChainMissed != 3 || record.ChainInvalid != 1 {
			t.Fatalf("chain tallies = missed %d, invalid %d; want 3 and 1", record.ChainMissed, record.ChainInvalid)
		}
	}
}

func pendingOfSlot(t *testing.T, book *Book, slotID uint32) uint64 {
	t.Helper()
	for _, record := range book.Query(QueryFilter{}) {
		for _, slot := range record.Slots {
			if slot.SlotID == slotID {
				return slot.Pending
			}
		}
	}
	t.Fatalf("slot %d absent from the ledger", slotID)
	return 0
}

// The per-epoch view groups by the escrow's epoch, so re-opening a live escrow every sweep must not
// drag its history into whichever epoch is current now.
func TestReopeningAnEscrowKeepsTheEpochItWasFirstSeenIn(t *testing.T) {
	book := newTestBook(t, 2)
	if err := book.RecordGhost(testEscrow, 1, "participant_throttled_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}

	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: testEscrow, Model: testModel, CreationEpoch: testEpoch + 5,
		Slots: []types.SlotAssignment{
			{SlotID: 0, ValidatorAddress: participantFor(0)},
			{SlotID: 1, ValidatorAddress: participantFor(1)},
		},
	}); err != nil {
		t.Fatalf("OpenEscrow() on a known escrow: %v", err)
	}

	for _, record := range book.Query(QueryFilter{}) {
		if record.EpochIndex != testEpoch {
			t.Fatalf("epoch = %d, want the epoch of the first sighting", record.EpochIndex)
		}
	}
}

// Every race fact must survive the trip into the bucket; a dropped one is invisible in the totals and
// only shows up as a finding that never fires.
func TestRaceFactsReachTheCounters(t *testing.T) {
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 1); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}

	if err := book.RecordRace(testEscrow, []Attempt{{
		Nonce: 1, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner,
		Terminal: "won", Phase: PhasePoC, SlowReceipt: true, SlowChunk: true, ClockDrifted: true,
	}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	records := book.Query(QueryFilter{})
	if len(records) != 1 || len(records[0].Counters) != 1 {
		t.Fatalf("got %d records, want one participant holding one bucket", len(records))
	}
	key := records[0].Counters[0].CounterKey
	if key.Terminal != "won" || key.Phase != PhasePoC ||
		!key.SlowReceipt || !key.SlowChunk || !key.ClockDrifted {
		t.Fatalf("counter key = %+v, want every race fact carried onto the bucket", key)
	}
}

// A nonce classified without the race ever reporting on it must still name a terminal: a blank one
// reads as missing data and silently drops the bucket from any grouping by terminal.
func TestANonceWithoutRaceFactsStillNamesATerminal(t *testing.T) {
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 1); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 1, Sent: true, Usage: UsageWinner}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	if err := book.MarkFinished(testEscrow, []uint64{1}); err != nil {
		t.Fatalf("MarkFinished(): %v", err)
	}

	counters := book.Query(QueryFilter{})[0].Counters
	if len(counters) != 1 {
		t.Fatalf("got %d buckets, want one", len(counters))
	}
	if counters[0].Terminal != TerminalUnreported {
		t.Fatalf("terminal = %q, want it named %q", counters[0].Terminal, TerminalUnreported)
	}
}

func TestResetEmptiesTheLedger(t *testing.T) {
	book := twoEpochBook(t)
	if len(book.Query(QueryFilter{})) == 0 {
		t.Fatal("fixture produced no records to clear")
	}

	book.Reset()

	if records := book.Query(QueryFilter{}); len(records) != 0 {
		t.Fatalf("got %d records after a reset, want none", len(records))
	}
	if escrows := book.EscrowIDs(); len(escrows) != 0 {
		t.Fatalf("got escrows %v after a reset, want none", escrows)
	}
}

// A reset that is not written out is undone by the next restart, which is the one moment an operator
// is least likely to be watching. The service is exercised rather than the book, because writing it
// out is the service's half of the job.
func TestResetIsWrittenOutSoARestartCannotUndoIt(t *testing.T) {
	store := openTestStore(t)
	service, err := NewService(Settings{Store: store, Now: func() time.Time { return time.Unix(0, 0).UTC() }})
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	openTestEscrow(t, service.Book, testEscrow, testEpoch, 2)
	if err := service.Book.RecordGhost(testEscrow, 1, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	// Written out first, so the store holds something a reset that forgot to flush would leave behind.
	if err := service.Flush(); err != nil {
		t.Fatalf("Flush(): %v", err)
	}

	if err := service.Reset(); err != nil {
		t.Fatalf("Reset(): %v", err)
	}

	snapshot, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	reloaded := NewBook(func() time.Time { return time.Unix(0, 0).UTC() })
	if err := reloaded.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if records := reloaded.Query(QueryFilter{}); len(records) != 0 {
		t.Fatalf("got %d records after restarting over a cleared ledger, want none", len(records))
	}
}

// The chain's latest nonce is polled on a sweep while races report continuously, so between sweeps a
// busy escrow classifies nonces beyond the last polled range. Counting that as overcounted blames the
// chain for the gateway's own polling interval.
func TestNoncesSeenBetweenSweepsDoNotReadAsADisagreement(t *testing.T) {
	book := newTestBook(t, 2)

	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 10, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner},
		{Nonce: 12, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	for _, record := range book.Query(QueryFilter{}) {
		if record.Overcounted != 0 {
			t.Fatalf("participant %s overcounted %d nonces it was told about before the next sweep",
				record.Participant, record.Overcounted)
		}
	}
}
