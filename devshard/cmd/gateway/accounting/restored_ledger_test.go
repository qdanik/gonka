package accounting

import (
	"reflect"
	"testing"
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
