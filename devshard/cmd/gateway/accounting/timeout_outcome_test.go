package accounting

import (
	"testing"

	"devshard/types"
)

func TestTimeoutOutcomeOfSpeaksTheOldLedgersVocabulary(t *testing.T) {
	cases := []struct {
		action, reason string
		want           TimeoutOutcome
		settled        bool
	}{
		{action: "skipped", reason: "nonce_already_finished", want: TimeoutSkipped, settled: true},
		{action: "completed", reason: "none", want: TimeoutApplied, settled: true},
		{action: "failed", reason: "timeout_not_applied", want: TimeoutInsufficientVotes, settled: true},
		{action: "failed", reason: "timeout_collection_error", want: TimeoutVoteCollectionFailed, settled: true},
		{action: "failed", reason: "escrow_gone_from_hosts", want: TimeoutVoteCollectionFailed, settled: true},
		{action: "failed", reason: "something new", want: TimeoutVoteCollectionFailed, settled: true},
		{action: "started", reason: "none", settled: false},
		{action: "", reason: "", settled: false},
	}
	for _, testCase := range cases {
		got, settled := timeoutOutcomeOf(testCase.action, testCase.reason)
		if settled != testCase.settled {
			t.Errorf("timeoutOutcomeOf(%q, %q) settled = %v, want %v", testCase.action, testCase.reason, settled, testCase.settled)
			continue
		}
		if settled && got != testCase.want {
			t.Errorf("timeoutOutcomeOf(%q, %q) = %q, want %q", testCase.action, testCase.reason, got, testCase.want)
		}
	}
}

func TestTimeoutOutcomesReachTheParticipantRecord(t *testing.T) {
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID:      "e1",
		CreationEpoch: 9,
		Model:         "m",
		Slots:         []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "p0"}, {SlotID: 1, ValidatorAddress: "p1"}},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	if err := book.RecordTimeout("e1", 1, "refused", "completed", "none"); err != nil {
		t.Fatalf("RecordTimeout: %v", err)
	}
	if err := book.RecordTimeout("e1", 3, "refused", "failed", "timeout_not_applied"); err != nil {
		t.Fatalf("RecordTimeout: %v", err)
	}

	outcomes := map[TimeoutOutcome]uint64{}
	for _, record := range book.Query(QueryFilter{EpochIndex: 9}) {
		for outcome, count := range record.TimeoutOutcomes {
			outcomes[outcome] += count
		}
	}
	if outcomes[TimeoutApplied] != 1 {
		t.Errorf("applied = %d, want 1", outcomes[TimeoutApplied])
	}
	if outcomes[TimeoutInsufficientVotes] != 1 {
		t.Errorf("insufficient_votes = %d, want 1", outcomes[TimeoutInsufficientVotes])
	}
}
