package accounting

import (
	"testing"

	"devshard/types"
)

func schemaTestBook(t *testing.T) *Book {
	t.Helper()
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: "e1", CreationEpoch: 9, Model: "m",
		Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: "p0"}, {SlotID: 1, ValidatorAddress: "p1"}},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	return book
}

func recordFor(t *testing.T, book *Book, participant string) ParticipantRecord {
	t.Helper()
	for _, record := range book.Query(QueryFilter{EpochIndex: 9}) {
		if record.Participant == participant {
			return record
		}
	}
	t.Fatalf("no record for %s", participant)
	return ParticipantRecord{}
}

// A nonce a host is working on is not the same as one nothing has decided yet. Folding both into
// pending hides how much of the unclassified tail is live work.
func TestInFlightIsSeparateFromPending(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.RecordRace("e1", []Attempt{{Nonce: 1, Sent: true}}); err != nil {
		t.Fatalf("RecordRace: %v", err)
	}

	record := recordFor(t, book, "p1")

	if record.InFlight != 1 {
		t.Errorf("in_flight = %d, want the sent-and-unfinished nonce", record.InFlight)
	}
	if record.Pending != 0 {
		t.Errorf("pending = %d, want the sent nonce counted as in flight instead", record.Pending)
	}
}

// A raised timeout that has settled on nothing is owed one; the old ledger reports that separately
// from a nonce whose round is over.
func TestTimeoutPendingCountsRoundsStillOwedAnOutcome(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.RecordTimeout("e1", 1, "refused", "started", "none"); err != nil {
		t.Fatalf("RecordTimeout: %v", err)
	}

	record := recordFor(t, book, "p1")

	if record.TimeoutPending != 1 {
		t.Errorf("timeout_pending = %d, want the round still owed an outcome", record.TimeoutPending)
	}
	if len(record.TimeoutOutcomes) != 0 {
		t.Errorf("timeout_outcomes = %v, want none: the round has not settled", record.TimeoutOutcomes)
	}
}

// Both sides of every count the chain also keeps, so a reader can see them disagree.
func TestCrossChecksCarryBothSides(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.ObserveHostStats("e1", 1, types.HostStats{Missed: 5, Invalid: 2}); err != nil {
		t.Fatalf("ObserveHostStats: %v", err)
	}
	if err := book.RecordTimeout("e1", 1, "refused", "completed", "none"); err != nil {
		t.Fatalf("RecordTimeout: %v", err)
	}

	record := recordFor(t, book, "p1")

	if record.CrossChecks.HostMissed != 5 || record.CrossChecks.HostInvalid != 2 {
		t.Errorf("chain side = %d/%d, want 5/2", record.CrossChecks.HostMissed, record.CrossChecks.HostInvalid)
	}
	if record.CrossChecks.TimeoutApplied != 1 {
		t.Errorf("ledger side = %d, want the one applied timeout", record.CrossChecks.TimeoutApplied)
	}
}

// A participant holding several slots of one escrow must not have that escrow listed once per slot.
func TestLatestNoncesListEachEscrowOnce(t *testing.T) {
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: "e1", CreationEpoch: 9, Model: "m",
		Slots: []types.SlotAssignment{
			{SlotID: 0, ValidatorAddress: "p0"},
			{SlotID: 1, ValidatorAddress: "p0"},
			{SlotID: 2, ValidatorAddress: "p1"},
		},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	if err := book.ObserveLatestNonce("e1", 42); err != nil {
		t.Fatalf("ObserveLatestNonce: %v", err)
	}

	record := recordFor(t, book, "p0")

	if len(record.LatestNonces) != 1 {
		t.Fatalf("latest_nonces = %v, want one entry for the one escrow", record.LatestNonces)
	}
	if got := record.LatestNonces[0]; got.EscrowID != "e1" || got.LatestNonce != 42 {
		t.Errorf("latest_nonces[0] = %+v, want e1 at 42", got)
	}
}

func TestLatestNoncesMarkARetiredEscrow(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.ObserveLatestNonce("e1", 7); err != nil {
		t.Fatalf("ObserveLatestNonce: %v", err)
	}
	book.RetireEscrow("e1")

	record := recordFor(t, book, "p1")

	if len(record.LatestNonces) != 1 || !record.LatestNonces[0].Retired {
		t.Errorf("latest_nonces = %+v, want the escrow marked retired", record.LatestNonces)
	}
}

func TestUnresolvedChallengesAreAGaugePerSlot(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.ObserveChallenges("e1", map[uint32]uint64{1: 3}); err != nil {
		t.Fatalf("ObserveChallenges: %v", err)
	}

	if got := recordFor(t, book, "p1").UnresolvedChallenges; got != 3 {
		t.Errorf("unresolved_challenges = %d, want 3", got)
	}
	if got := recordFor(t, book, "p0").UnresolvedChallenges; got != 0 {
		t.Errorf("the other slot carries %d, want none", got)
	}

	if err := book.ObserveChallenges("e1", map[uint32]uint64{1: 1}); err != nil {
		t.Fatalf("ObserveChallenges: %v", err)
	}
	if got := recordFor(t, book, "p1").UnresolvedChallenges; got != 1 {
		t.Errorf("unresolved_challenges = %d after two resolved, want 1: this is a gauge, not a total", got)
	}
}

func TestValidationsCreditTheSlotThatChecked(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.RecordValidation("e1", 0); err != nil {
		t.Fatalf("RecordValidation: %v", err)
	}
	if err := book.RecordValidation("e1", 0); err != nil {
		t.Fatalf("RecordValidation: %v", err)
	}

	if got := recordFor(t, book, "p0").ValidationsPerformed; got != 2 {
		t.Errorf("validations_performed = %d, want 2", got)
	}
	if got := recordFor(t, book, "p1").ValidationsPerformed; got != 0 {
		t.Errorf("the checked slot was credited %d, want none", got)
	}
}

func TestValidationsRejectASlotOutsideTheGroup(t *testing.T) {
	book := schemaTestBook(t)

	if err := book.RecordValidation("e1", 9); err == nil {
		t.Fatal("a slot outside the group was accepted")
	}
}

// The chain counts a miss on the executor slot, and nonce % groupSize is that binding.
func TestAppliedTimeoutsCreditTheExecutorSlot(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.RecordAppliedTimeout("e1", 3); err != nil {
		t.Fatalf("RecordAppliedTimeout: %v", err)
	}

	if got := recordFor(t, book, "p1").TimeoutsApplied; got != 1 {
		t.Errorf("timeouts_applied = %d for slot 1, want 1", got)
	}
	if got := recordFor(t, book, "p0").TimeoutsApplied; got != 0 {
		t.Errorf("slot 0 was credited %d, want none", got)
	}
}

// Disagreement is measured per slot: one participant holding two slots must not have a surplus on
// one cancel a shortfall on the other, which is what summing both sides first would do.
func TestCrossCheckErrorDoesNotLetSlotsCancelEachOther(t *testing.T) {
	book := NewBook(nil)
	if err := book.OpenEscrow(EscrowMetadata{
		EscrowID: "e1", CreationEpoch: 9, Model: "m",
		Slots: []types.SlotAssignment{
			{SlotID: 0, ValidatorAddress: "both"},
			{SlotID: 1, ValidatorAddress: "both"},
		},
	}); err != nil {
		t.Fatalf("OpenEscrow: %v", err)
	}
	if err := book.ObserveHostStats("e1", 0, types.HostStats{Missed: 4}); err != nil {
		t.Fatalf("ObserveHostStats: %v", err)
	}
	for _, nonce := range []uint64{1, 3, 5, 7} {
		if err := book.RecordAppliedTimeout("e1", nonce); err != nil {
			t.Fatalf("RecordAppliedTimeout: %v", err)
		}
	}

	record := recordFor(t, book, "both")

	if record.TimeoutsApplied != 4 || record.CrossChecks.HostMissed != 4 {
		t.Fatalf("sides = %d ledger / %d chain, want 4/4 so the totals agree while the slots do not",
			record.TimeoutsApplied, record.CrossChecks.HostMissed)
	}
	if record.CrossChecks.ErrorCount != 8 {
		t.Errorf("error_count = %d, want 8: slot 0 is short four and slot 1 has four the chain never counted",
			record.CrossChecks.ErrorCount)
	}
}

// A validator rejects someone else's answer, so the two halves of one message land on two slots.
func TestARejectedAnswerChargesTheExecutorNotTheValidator(t *testing.T) {
	book := schemaTestBook(t)
	const executedBySlotOne = uint64(3)

	if err := book.RecordValidation("e1", 0); err != nil {
		t.Fatalf("RecordValidation: %v", err)
	}
	if err := book.RecordInvalidVerdict("e1", executedBySlotOne); err != nil {
		t.Fatalf("RecordInvalidVerdict: %v", err)
	}

	if got := recordFor(t, book, "p1").CrossChecks.RecordedInvalid; got != 1 {
		t.Errorf("recorded_invalid_transitions = %d for the executor, want 1", got)
	}
	if got := recordFor(t, book, "p0").CrossChecks.RecordedInvalid; got != 0 {
		t.Errorf("the validator that rejected was charged %d, want none", got)
	}
}

// The per-escrow rows carried these as null while the host row above them was populated, so an escrow
// could not be told apart from a quiet one.
func TestASlotCarriesTheTimeoutsOfItsOwnEscrow(t *testing.T) {
	book := schemaTestBook(t)
	if err := book.RecordTimeout("e1", 1, "refused", "started", "none"); err != nil {
		t.Fatalf("RecordTimeout: %v", err)
	}

	record := recordFor(t, book, "p1")
	if len(record.Slots) != 1 {
		t.Fatalf("the host holds %d escrow rows, want 1", len(record.Slots))
	}
	if got := record.Slots[0].TimeoutPending; got != 1 {
		t.Errorf("the escrow row reports timeout_pending = %d, want 1", got)
	}
	if got := record.TimeoutPending; got != 1 {
		t.Errorf("the host row reports timeout_pending = %d, want the same 1", got)
	}
}
