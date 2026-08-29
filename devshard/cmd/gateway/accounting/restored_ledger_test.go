package accounting

import (
	"reflect"
	"slices"
	"testing"

	"devshard/types"
)

// Every recording path writes into a map and has no error to return if that map is nil, so a restored
// escrow that lost one panics on the first validation, timeout or challenge after a restart — inside a
// goroutine, which takes the gateway with it.
func TestARestoredEscrowAcceptsEveryRecordingPath(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 4); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	restored := saveAndReload(t, book, openTestStore(t))

	if err := restored.RecordValidation(testEscrow, 1); err != nil {
		t.Fatalf("RecordValidation(): %v", err)
	}
	if err := restored.RecordAppliedTimeout(testEscrow, 2); err != nil {
		t.Fatalf("RecordAppliedTimeout(): %v", err)
	}
	if err := restored.ObserveChallenges(testEscrow, map[uint32]uint64{1: 3}); err != nil {
		t.Fatalf("ObserveChallenges(): %v", err)
	}
}

// The two construction sites drifted once already. A ledger built by hand somewhere else would pass
// every test above while still carrying a nil map nothing exercised yet.
func TestEveryMapOfAnEscrowLedgerIsBuilt(t *testing.T) {
	ledger := reflect.ValueOf(newEscrowLedger(EscrowMetadata{EscrowID: "e1"})).Elem()
	for i := range ledger.NumField() {
		field := ledger.Field(i)
		if field.Kind() == reflect.Map && field.IsNil() {
			t.Errorf("escrowLedger.%s is nil: the first write to it panics", ledger.Type().Field(i).Name)
		}
	}
}

// The chain's side of the cross-check is restored from host stats; the gateway's side is these four
// per-slot counts. Restoring one without the other reads every applied timeout as a nonce the chain
// counted and the gateway did not, so a restart alone raises a disagreement no host behaviour produces.
func TestARestartDoesNotInventADisagreementWithTheChain(t *testing.T) {
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
	if codes := codesOf(findingsFor(book.Query(QueryFilter{})[0])); slices.Contains(codes, FindingChainDisagreement) {
		t.Fatalf("findings %v disagree with the chain before any restart", codes)
	}

	restored := saveAndReload(t, book, openTestStore(t))

	records := restored.Query(QueryFilter{})
	if len(records) != 1 {
		t.Fatalf("Query() returned %d records, want the one participant", len(records))
	}
	if codes := codesOf(findingsFor(records[0])); slices.Contains(codes, FindingChainDisagreement) {
		t.Errorf("findings %v report a disagreement the restart invented", codes)
	}
}
