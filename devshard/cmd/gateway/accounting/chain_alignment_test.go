package accounting

import (
	"slices"
	"testing"

	"devshard/types"
)

func TestAWipedLedgerDoesNotInventADisagreementWithTheChain(t *testing.T) {
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 40); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}

	if err := book.ObserveHostStats(testEscrow, 0, types.HostStats{Missed: 30}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	record := book.Query(QueryFilter{})[0]
	if codes := codesOf(findingsFor(record)); slices.Contains(codes, FindingChainDisagreement) {
		t.Fatalf("findings %v disagree with the chain over history the ledger never watched", codes)
	}
	if record.CrossChecks.ErrorCount != 0 {
		t.Fatalf("error_count = %d, want 0: both halves start on the same window", record.CrossChecks.ErrorCount)
	}
}

func TestADriftAfterTheLedgerStartedWatchingIsStillReported(t *testing.T) {
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 40); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.ObserveHostStats(testEscrow, 0, types.HostStats{Missed: 30}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	if err := book.ObserveHostStats(testEscrow, 0, types.HostStats{Missed: 35}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	record := book.Query(QueryFilter{})[0]
	if record.CrossChecks.ErrorCount != 5 {
		t.Fatalf("error_count = %d, want the 5 misses the chain counted while the ledger was watching",
			record.CrossChecks.ErrorCount)
	}
}

func TestADissentingValidatorIsNotADisagreementWithTheChain(t *testing.T) {
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 40); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.ObserveHostStats(testEscrow, 0, types.HostStats{}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	for nonce := uint64(1); nonce <= 4; nonce++ {
		if err := book.RecordInvalidVerdict(testEscrow, nonce); err != nil {
			t.Fatalf("RecordInvalidVerdict(%d): %v", nonce, err)
		}
	}

	record := book.Query(QueryFilter{})[0]
	if record.CrossChecks.RecordedInvalid != 4 {
		t.Fatalf("recorded_invalid_transitions = %d, want the 4 challenges opened", record.CrossChecks.RecordedInvalid)
	}
	if record.CrossChecks.ErrorCount != 0 {
		t.Fatalf("error_count = %d, want 0: a validator opening a challenge is not the chain invalidating a nonce",
			record.CrossChecks.ErrorCount)
	}
}

func TestARestoredLedgerKeepsItsOwnHalfRatherThanReseeding(t *testing.T) {
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 40); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	for nonce := uint64(1); nonce <= 30; nonce++ {
		if err := book.RecordAppliedTimeout(testEscrow, nonce); err != nil {
			t.Fatalf("RecordAppliedTimeout(%d): %v", nonce, err)
		}
	}
	if err := book.ObserveHostStats(testEscrow, 0, types.HostStats{Missed: 30}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	restored := saveAndReload(t, book, openTestStore(t))
	if err := restored.ObserveHostStats(testEscrow, 0, types.HostStats{Missed: 30}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	record := restored.Query(QueryFilter{})[0]
	if record.TimeoutsApplied != 30 {
		t.Fatalf("timeouts_applied = %d after a restore, want the 30 it had recorded", record.TimeoutsApplied)
	}
	if record.CrossChecks.ErrorCount != 0 {
		t.Fatalf("error_count = %d, want 0: a restored half is not re-seeded", record.CrossChecks.ErrorCount)
	}
}
