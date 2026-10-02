package accounting

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

const otherEscrow = "escrow-2"

func storedLatestNonce(t *testing.T, store *Store, escrowID string) uint64 {
	t.Helper()
	var latest uint64
	if err := store.db.QueryRowContext(t.Context(),
		`SELECT latest_nonce FROM accounting_escrows WHERE escrow_id = ?`, escrowID).Scan(&latest); err != nil {
		t.Fatalf("reading latest_nonce of %s: %v", escrowID, err)
	}
	return latest
}

// Test flow:
//  1. Open two escrows and save the book.
//  2. Overwrite the first escrow's stored watermark behind the book's back, then change only the second escrow.
//  3. Save again and assert the first escrow's row was left alone while the second escrow's change landed.
func TestASaveWritesOnlyTheEscrowsChangedSinceTheLastSave(t *testing.T) {
	book := newTestBook(t, 4)
	openTestEscrow(t, book, otherEscrow, testEpoch, 4)
	store := openTestStore(t)
	if err := store.Save(t.Context(), book); err != nil {
		t.Fatalf("Save() = %v, want nil", err)
	}
	if _, err := store.db.ExecContext(t.Context(),
		`UPDATE accounting_escrows SET latest_nonce = 999 WHERE escrow_id = ?`, testEscrow); err != nil {
		t.Fatalf("marking the stored row: %v", err)
	}
	if err := book.ObserveLatestNonce(otherEscrow, 7); err != nil {
		t.Fatalf("ObserveLatestNonce() = %v, want nil", err)
	}

	if err := store.Save(t.Context(), book); err != nil {
		t.Fatalf("Save() = %v, want nil", err)
	}

	if got := storedLatestNonce(t, store, testEscrow); got != 999 {
		t.Fatalf("stored latest_nonce of the unchanged escrow = %d, want 999", got)
	}
	if got := storedLatestNonce(t, store, otherEscrow); got != 7 {
		t.Fatalf("stored latest_nonce of the changed escrow = %d, want 7", got)
	}
}

// Test flow:
//  1. Open two escrows in different epochs and save the book.
//  2. Reset the first escrow's epoch, save, and reload.
//  3. Assert only the second escrow comes back.
func TestAnEscrowDroppedFromTheBookLeavesTheStore(t *testing.T) {
	book := newTestBook(t, 4)
	openTestEscrow(t, book, otherEscrow, testEpoch+1, 4)
	store := openTestStore(t)
	if err := store.Save(t.Context(), book); err != nil {
		t.Fatalf("Save() = %v, want nil", err)
	}
	book.ResetEpoch(testEpoch)

	restored := saveAndReload(t, book, store)

	if got := restored.EscrowIDs(); len(got) != 1 || got[0] != otherEscrow {
		t.Fatalf("restored escrows = %v, want [%s]", got, otherEscrow)
	}
}

// Test flow:
//  1. Open an escrow, save, and change its watermark.
//  2. Save with a cancelled context, then save again with a live one.
//  3. Assert the change the failed save carried reached the store.
func TestAFailedSaveKeepsItsChangesForTheNextSave(t *testing.T) {
	book := newTestBook(t, 4)
	store := openTestStore(t)
	if err := store.Save(t.Context(), book); err != nil {
		t.Fatalf("Save() = %v, want nil", err)
	}
	if err := book.ObserveLatestNonce(testEscrow, 11); err != nil {
		t.Fatalf("ObserveLatestNonce() = %v, want nil", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := store.Save(cancelled, book); err == nil {
		t.Fatal("Save(cancelled) = nil, want an error")
	}

	if err := store.Save(t.Context(), book); err != nil {
		t.Fatalf("Save() = %v, want nil", err)
	}

	if got := storedLatestNonce(t, store, testEscrow); got != 11 {
		t.Fatalf("stored latest_nonce = %d, want 11", got)
	}
}

// Test flow:
//  1. Save a book with one escrow and restore it into a fresh book.
//  2. Save the restored book into a second, empty store.
//  3. Assert the second store holds the escrow, because a restore is itself a change to write.
func TestARestoredBookWritesEverythingOnItsFirstSave(t *testing.T) {
	book := newTestBook(t, 4)
	restored := saveAndReload(t, book, openTestStore(t))

	second := openTestStore(t)
	again := saveAndReload(t, restored, second)

	if got := again.EscrowIDs(); len(got) != 1 || got[0] != testEscrow {
		t.Fatalf("escrows in the second store = %v, want [%s]", got, testEscrow)
	}
}

// Test flow:
//  1. Build a service over a store and open an escrow in its book.
//  2. Request a flush and wait for the service's own goroutine to go idle.
//  3. Assert the escrow reached the store without the caller writing it.
func TestARequestedFlushIsWrittenByTheServiceItself(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := openTestStore(t)
		service, err := NewService(Settings{Store: store, Now: func() time.Time { return time.Unix(0, 0).UTC() }})
		if err != nil {
			t.Fatalf("NewService() = %v, want nil", err)
		}
		t.Cleanup(func() { _ = service.Close() })
		openTestEscrow(t, service.Book, testEscrow, testEpoch, 4)
		if err := service.Book.ObserveLatestNonce(testEscrow, 5); err != nil {
			t.Fatalf("ObserveLatestNonce() = %v, want nil", err)
		}

		service.RequestFlush()
		synctest.Wait()

		if got := storedLatestNonce(t, store, testEscrow); got != 5 {
			t.Fatalf("stored latest_nonce = %d, want 5", got)
		}
	})
}
