package accounting

import (
	"context"
	"database/sql"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/types"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "accounting.db"))
	if err != nil {
		t.Fatalf("OpenStore(): %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// saveAndReload writes the book and reads it into a fresh one, which is what a restart does.
func saveAndReload(t *testing.T, book *Book, store *Store) *Book {
	t.Helper()
	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save(): %v", err)
	}
	snapshot, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	restored := NewBook(func() time.Time { return time.Unix(0, 0).UTC() })
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	return restored
}

func TestCountersSurviveARestart(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 12); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordGhost(testEscrow, 5, "participant_throttled_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	if err := book.ObserveHostStats(testEscrow, 1, types.HostStats{Missed: 2}); err != nil {
		t.Fatalf("ObserveHostStats(): %v", err)
	}

	restored := saveAndReload(t, book, openTestStore(t))

	before, after := book.Query(QueryFilter{}), restored.Query(QueryFilter{})
	if len(before) != len(after) {
		t.Fatalf("restored %d participants, want %d", len(after), len(before))
	}
	for index, want := range before {
		got := after[index]
		if got.Participant != want.Participant || got.Assigned != want.Assigned {
			t.Fatalf("restored %+v, want participant %s with %d assigned", got, want.Participant, want.Assigned)
		}
		if got.Dispositions[DispositionGhost] != want.Dispositions[DispositionGhost] {
			t.Fatalf("restored %d ghosts for %s, want %d",
				got.Dispositions[DispositionGhost], want.Participant, want.Dispositions[DispositionGhost])
		}
		if got.ChainMissed != want.ChainMissed {
			t.Fatalf("restored %d chain misses for %s, want %d", got.ChainMissed, want.Participant, want.ChainMissed)
		}
	}
}

// A nonce awaiting its timeout will never be told it, because the race that would settle it died with
// the process. Naming that is worth more than leaving it pending for ever: the nonce still settles as
// a completed inference, and nobody checked it.
func TestANonceLeftPendingByARestartIsNamedRatherThanLost(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.ObserveLatestNonce(testEscrow, 12); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 6, Sent: true}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if pending := unclassifiedOfSlot(t, book, slotOfNonce(6, 4)); pending != 1 {
		t.Fatalf("pending = %d before the restart, want the unfinished nonce", pending)
	}

	restored := saveAndReload(t, book, openTestStore(t))

	if pending := unclassifiedOfSlot(t, restored, slotOfNonce(6, 4)); pending != 0 {
		t.Fatalf("pending = %d after the restart, want the nonce classified", pending)
	}
	// No receipt ever arrived for it, so it is a refusal rather than a failed execution.
	assertDisposition(t, restored, 6, 4, DispositionUnfinishedRefused)
	for _, record := range restored.Query(QueryFilter{}) {
		if record.Participant != participantFor(2) {
			continue
		}
		for _, counter := range record.Counters {
			if counter.TimeoutAction != engine.TimeoutActionAbandoned {
				t.Fatalf("timeout action = %q, want the abandonment named", counter.TimeoutAction)
			}
		}
	}
}

func TestAnEmptyStoreStartsAnEmptyLedger(t *testing.T) {
	snapshot, err := openTestStore(t).Load(context.Background())
	if err != nil {
		t.Fatalf("Load() on a first run: %v", err)
	}
	book := NewBook(nil)
	if err := book.Restore(snapshot); err != nil {
		t.Fatalf("Restore(): %v", err)
	}
	if escrows := book.EscrowIDs(); len(escrows) != 0 {
		t.Fatalf("EscrowIDs() = %v, want none", escrows)
	}
}

func TestAStoreFromAnotherSchemaIsRefused(t *testing.T) {
	store := openTestStore(t)
	if _, err := store.db.Exec(
		`INSERT INTO accounting_meta (key, value) VALUES ('schema_version', '99')`); err != nil {
		t.Fatalf("writing the fixture: %v", err)
	}
	if _, err := store.Load(context.Background()); err == nil {
		t.Fatal("Load() accepted a ledger from another schema")
	}
}

// A failed write must leave the previous ledger whole. The failure has to land after the tables are
// emptied, which is the only moment a missing rollback would cost the ledger everything.
func TestAFailedWriteLeavesThePreviousLedgerInPlace(t *testing.T) {
	store := openTestStore(t)
	book := newTestBook(t, 4)
	if err := book.RecordGhost(testEscrow, 5, "participant_throttled_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save(): %v", err)
	}

	// Two slots claiming the same id collide on the slot table's primary key, so the write fails with
	// every table already emptied.
	broken := NewBook(func() time.Time { return time.Unix(0, 0).UTC() })
	if err := broken.OpenEscrow(EscrowMetadata{
		EscrowID: testEscrow, Model: testModel, CreationEpoch: testEpoch,
		Slots: []types.SlotAssignment{
			{SlotID: 0, ValidatorAddress: participantFor(0)},
			{SlotID: 0, ValidatorAddress: participantFor(1)},
		},
	}); err != nil {
		t.Fatalf("OpenEscrow(): %v", err)
	}
	if err := store.Save(context.Background(), broken); err == nil {
		t.Fatal("Save() reported success writing two slots with one id")
	}

	snapshot, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() after the failed write: %v", err)
	}
	if len(snapshot.Escrows) != 1 || len(snapshot.Escrows[0].Counters) != 1 {
		t.Fatalf("ledger after the failed write = %+v, want the previous one whole", snapshot.Escrows)
	}
}

// A nonce the protocol finishes after its race gave up on it must leave the unfinished bucket: that
// bucket is what settlement reads as work the participant failed to do.
func TestALateFinishLiftsANonceOutOfTheUnfinishedBucket(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 6, Sent: true, Usage: UsageLoser}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := book.RecordTimeout(testEscrow, 6, "execution", "failed", "timeout_collection_error"); err != nil {
		t.Fatalf("RecordTimeout(): %v", err)
	}
	assertDisposition(t, book, 6, 4, DispositionUnfinishedExecution)

	unfinished := book.UnfinishedNonces(testEscrow)
	if len(unfinished) != 1 || unfinished[0] != 6 {
		t.Fatalf("UnfinishedNonces() = %v, want the one nonce worth re-asking about", unfinished)
	}
	if err := book.MarkFinished(testEscrow, unfinished); err != nil {
		t.Fatalf("MarkFinished(): %v", err)
	}

	assertDisposition(t, book, 6, 4, DispositionFinishedUnused)
	if left := book.UnfinishedNonces(testEscrow); len(left) != 0 {
		t.Fatalf("UnfinishedNonces() = %v, want none once the finish landed", left)
	}
}

// Only nonces that can still move are written down. A restart that carried every nonce would grow the
// file with the escrow's whole history to no purpose.
func TestOnlyRevisableNoncesAreWrittenDown(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.RecordGhost(testEscrow, 5, "participant_throttled_no_send"); err != nil {
		t.Fatalf("RecordGhost(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 9, Sent: true, Finished: true, Usage: UsageWinner},
		{Nonce: 6, Sent: true, Usage: UsageLoser},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := book.RecordTimeout(testEscrow, 6, "execution", "failed", "timeout_collection_error"); err != nil {
		t.Fatalf("RecordTimeout(): %v", err)
	}

	stored := book.Snapshot().Escrows[0].Nonces
	if len(stored) != 1 || stored[0].Nonce != 6 {
		t.Fatalf("stored nonces = %+v, want only the unfinished one", stored)
	}
}

// A restored nonce must move out of its bucket, not into a second one.
func TestARestoredUnfinishedNonceIsLiftedRatherThanCountedTwice(t *testing.T) {
	book := newTestBook(t, 4)
	if err := book.RecordRace(testEscrow, []Attempt{{Nonce: 6, Sent: true, Usage: UsageLoser}}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := book.RecordTimeout(testEscrow, 6, "execution", "failed", "timeout_collection_error"); err != nil {
		t.Fatalf("RecordTimeout(): %v", err)
	}

	restored := saveAndReload(t, book, openTestStore(t))
	if err := restored.MarkFinished(testEscrow, restored.UnfinishedNonces(testEscrow)); err != nil {
		t.Fatalf("MarkFinished(): %v", err)
	}

	assertDisposition(t, restored, 6, 4, DispositionFinishedUnused)
}

// Retention is counted back from the current epoch: 2 keeps the current one and the two before it.
func TestRetentionKeepsTheCurrentEpochAndTheOnesBeforeIt(t *testing.T) {
	const currentEpoch = 10
	service, err := NewService(Settings{
		RetentionEpochs: 2,
		CurrentEpoch:    func(context.Context) (uint64, error) { return currentEpoch, nil },
		Now:             func() time.Time { return time.Unix(0, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })

	for _, epoch := range []uint64{7, 8, 9, 10} {
		escrowID := "escrow-" + strconv.FormatUint(epoch, 10)
		if err := service.Book.OpenEscrow(EscrowMetadata{
			EscrowID: escrowID, Model: testModel, CreationEpoch: epoch,
			Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: participantFor(0)}},
		}); err != nil {
			t.Fatalf("OpenEscrow(): %v", err)
		}
		service.Book.RetireEscrow(escrowID)
	}

	service.prune(context.Background())

	kept := service.Book.EscrowIDs()
	want := []string{"escrow-10", "escrow-8", "escrow-9"}
	if !slices.Equal(kept, want) {
		t.Fatalf("kept %v, want %v — the current epoch and the two before it", kept, want)
	}
}

// A live escrow is not dropped however old it is: it is still the one serving traffic.
func TestRetentionNeverDropsAnEscrowStillBeingServed(t *testing.T) {
	service, err := NewService(Settings{
		RetentionEpochs: 2,
		CurrentEpoch:    func(context.Context) (uint64, error) { return 10, nil },
		Now:             func() time.Time { return time.Unix(0, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })

	if err := service.Book.OpenEscrow(EscrowMetadata{
		EscrowID: "escrow-ancient", Model: testModel, CreationEpoch: 1,
		Slots: []types.SlotAssignment{{SlotID: 0, ValidatorAddress: participantFor(0)}},
	}); err != nil {
		t.Fatalf("OpenEscrow(): %v", err)
	}

	service.prune(context.Background())

	if kept := service.Book.EscrowIDs(); len(kept) != 1 {
		t.Fatalf("kept %v, want the live escrow untouched", kept)
	}
}

// Two buckets that differ only by a race fact are distinct keys in memory. Until the table knew those
// columns they collided on its primary key, and every snapshot failed on the constraint while the
// gateway kept running: the ledger was live in memory and empty on disk.
func TestCountersDifferingOnlyByARaceFactBothPersist(t *testing.T) {
	store := openTestStore(t)
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 2); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 1, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner, Terminal: "won", Phase: PhaseNormal},
		{Nonce: 2, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner, Terminal: "won", Phase: PhasePoC, SlowChunk: true},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save(): %v", err)
	}

	restored := saveAndReload(t, book, store)
	phases := make(map[Phase]bool)
	for _, counter := range restored.Query(QueryFilter{})[0].Counters {
		phases[counter.Phase] = true
	}
	if !phases[PhaseNormal] || !phases[PhasePoC] {
		t.Fatalf("reloaded phases %v, want both buckets to survive the round trip", phases)
	}
}

// A store written under an earlier schema has tables no current writer can fill. Opening it must
// discard them rather than leave a shape whose every snapshot fails on a constraint.
func TestAStoreFromAnEarlierSchemaIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounting.db")
	stale, err := sql.Open("sqlite", path+connectionPragmas)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if _, err := stale.Exec(`
		CREATE TABLE accounting_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO accounting_meta (key, value) VALUES ('schema_version', '1');
		CREATE TABLE accounting_counters (
			escrow_id TEXT NOT NULL, slot_id INTEGER NOT NULL, disposition TEXT NOT NULL,
			ghost_reason TEXT NOT NULL, timeout_kind TEXT NOT NULL, timeout_action TEXT NOT NULL,
			timeout_reason TEXT NOT NULL, count INTEGER NOT NULL,
			PRIMARY KEY (escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason));`); err != nil {
		t.Fatalf("seeding an older store: %v", err)
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore() over an older schema: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	book := newTestBook(t, 1)
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 1, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner, Terminal: "won"},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save() over a discarded older schema: %v", err)
	}
}

// Every persisted field is set to a distinguishable value and compared after a write and a read. A
// column the table lacks, an INSERT that skips it or a SELECT that forgets it all fail here, which is
// the only check that does not depend on someone re-reading four lists in sync. The stored snapshot is
// compared rather than the restored book, because Restore deliberately reclassifies a nonce whose race
// died with the process.
func TestEveryPersistedFieldSurvivesTheRoundTrip(t *testing.T) {
	store := openTestStore(t)
	book := newTestBook(t, 1)
	if err := book.ObserveLatestNonce(testEscrow, 2); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	if err := book.RecordRace(testEscrow, []Attempt{
		{
			Nonce: 1, Sent: true, Acknowledged: true, Finished: true, Usage: UsageLoser,
			Terminal: "lost", Phase: PhasePoC, SlowReceipt: true, SlowChunk: true, ClockDrifted: true,
		},
		{
			Nonce: 2, Sent: true, Acknowledged: true, Usage: UsageWinner,
			Terminal: "stalled", Phase: PhaseNormal, SlowChunk: true,
		},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}

	written := book.Snapshot()
	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save(): %v", err)
	}
	read, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}

	if len(read.Escrows) != len(written.Escrows) {
		t.Fatalf("read %d escrows, wrote %d", len(read.Escrows), len(written.Escrows))
	}
	for index := range written.Escrows {
		if !slices.Equal(read.Escrows[index].Counters, written.Escrows[index].Counters) {
			t.Fatalf("counters differ:\n wrote %+v\n read  %+v",
				written.Escrows[index].Counters, read.Escrows[index].Counters)
		}
		if !slices.Equal(read.Escrows[index].Nonces, written.Escrows[index].Nonces) {
			t.Fatalf("nonces differ:\n wrote %+v\n read  %+v",
				written.Escrows[index].Nonces, read.Escrows[index].Nonces)
		}
	}
}

// The case that actually happened: a build added columns to the Go structs, bumped the version and
// left the table alone, so the file on disk carried the current version beside a table no write could
// fill. Comparing versions agreed; every snapshot failed.
func TestAStoreWhoseVersionAgreesButShapeDoesNotIsDiscarded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounting.db")
	stale, err := sql.Open("sqlite", path+connectionPragmas)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	if _, err := stale.Exec(`
		CREATE TABLE accounting_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO accounting_meta (key, value) VALUES ('schema_version', '` + strconv.Itoa(SchemaVersion) + `');
		CREATE TABLE accounting_counters (
			escrow_id TEXT NOT NULL, slot_id INTEGER NOT NULL, disposition TEXT NOT NULL,
			ghost_reason TEXT NOT NULL, timeout_kind TEXT NOT NULL, timeout_action TEXT NOT NULL,
			timeout_reason TEXT NOT NULL, count INTEGER NOT NULL,
			PRIMARY KEY (escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason));`); err != nil {
		t.Fatalf("seeding a store of the right version and the wrong shape: %v", err)
	}
	if err := stale.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore(): %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	book := newTestBook(t, 1)
	if err := book.RecordRace(testEscrow, []Attempt{
		{Nonce: 1, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner, Terminal: "won"},
	}); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	if err := store.Save(context.Background(), book); err != nil {
		t.Fatalf("Save() over a store whose shape this build cannot write: %v", err)
	}
}

// A store carrying tables this build cannot write, with no version to say so. Trusting the missing
// version leaves the old tables in place and every later write fails on a column they lack.
func TestAStoreWhoseVersionCannotBeReadIsDiscarded(t *testing.T) {
	staleCounters := `
		CREATE TABLE accounting_counters (
			escrow_id TEXT NOT NULL, slot_id INTEGER NOT NULL, disposition TEXT NOT NULL,
			ghost_reason TEXT NOT NULL, timeout_kind TEXT NOT NULL, timeout_action TEXT NOT NULL,
			timeout_reason TEXT NOT NULL, count INTEGER NOT NULL,
			PRIMARY KEY (escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason));`

	cases := []struct {
		name string
		seed string
	}{
		{
			name: "the version table exists and holds no row",
			seed: `CREATE TABLE accounting_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);` + staleCounters,
		},
		{
			name: "the version table was never written",
			seed: staleCounters,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "accounting.db")
			stale, err := sql.Open("sqlite", path+connectionPragmas)
			if err != nil {
				t.Fatalf("Open(): %v", err)
			}
			if _, err := stale.Exec(testCase.seed); err != nil {
				t.Fatalf("seeding a store with no readable version: %v", err)
			}
			if err := stale.Close(); err != nil {
				t.Fatalf("Close(): %v", err)
			}

			store, err := OpenStore(path)
			if err != nil {
				t.Fatalf("OpenStore(): %v", err)
			}
			t.Cleanup(func() { _ = store.Close() })

			book := newTestBook(t, 1)
			if err := book.RecordRace(testEscrow, []Attempt{
				{Nonce: 1, Sent: true, Acknowledged: true, Finished: true, Usage: UsageWinner, Terminal: "won"},
			}); err != nil {
				t.Fatalf("RecordRace(): %v", err)
			}
			if err := store.Save(context.Background(), book); err != nil {
				t.Fatalf("Save() over a store with no readable version: %v", err)
			}
		})
	}
}
