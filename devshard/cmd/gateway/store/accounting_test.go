package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/internal/leakcheck"
)

type testClock struct {
	mu      sync.Mutex
	current time.Time
}

func countAccountingRows(t *testing.T, testStore *Store) int64 {
	t.Helper()
	var count int64
	if err := testStore.db.QueryRow(`SELECT COUNT(*) FROM request_accounting`).Scan(&count); err != nil {
		t.Fatalf("counting accounting rows: %v", err)
	}
	return count
}

func newTestClock(at time.Time) *testClock { return &testClock{current: at} }

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

func (c *testClock) Advance(by time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = c.current.Add(by)
}

func generousRetention() Retention {
	return Retention{MaxAge: 30 * 24 * time.Hour, MaxRows: 1_000}
}

func sampleRecord(requestID string) RequestRecord {
	return RequestRecord{
		RequestID:          requestID,
		EscrowID:           "escrow-7",
		Model:              "qwen",
		Outcome:            RequestSettled,
		Decision:           "hedged",
		Stream:             true,
		WinnerNonce:        42,
		WinnerParticipant:  "gonka1participant",
		WinnerHost:         "host-3.gonka.ai",
		WinnerHostIdx:      3,
		Attempts:           2,
		InputTokens:        128,
		WinnerOutputTokens: 256,
		TotalOutputTokens:  381,
		BalanceExhausted:   true,
		StartedAt:          time.Unix(1700000000, 0).UTC(),
		CompletedAt:        time.Unix(1700000004, 0).UTC(),
		FirstTokenMS:       450,
		DurationMS:         4_000,
	}
}

func openLedger(t *testing.T, testStore *Store, retention Retention, clock *testClock) *Ledger {
	t.Helper()
	ledger, err := testStore.NewLedger(retention, clock.Now)
	if err != nil {
		t.Fatalf("NewLedger(): %v", err)
	}
	return ledger
}

func TestLedgerWritesEveryFieldItWasGiven(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000010, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)

	written := sampleRecord("request-1")
	ledger.Record(written)
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	loaded, found, err := testStore.FindRequest(context.Background(), "request-1")
	if err != nil {
		t.Fatalf("FindRequest(): %v", err)
	}
	if !found {
		t.Fatal("FindRequest() found = false, want true")
	}
	expected := written
	expected.RecordedAt = clock.Now()
	if loaded != expected {
		t.Fatalf("FindRequest() = %+v, want %+v", loaded, expected)
	}
	if stats := ledger.Stats(); stats.Written != 1 || stats.Dropped != 0 || stats.Failed != 0 {
		t.Fatalf("Stats() = %+v, want 1 written and no losses", stats)
	}
}

func TestLedgerWritesExactlyOneRowPerRequestID(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000010, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)

	ledger.Record(sampleRecord("request-1"))
	ledger.Record(sampleRecord("request-1"))
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	count := countAccountingRows(t, testStore)
	if count != 1 {
		t.Fatalf("accounting rows = %d, want 1", count)
	}
}

func TestLedgerFindReportsUnknownAndBlankIDs(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000010, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)
	t.Cleanup(func() { ledger.Close() })

	ledger.Record(RequestRecord{Outcome: RequestFailed})

	for _, requestID := range []string{"missing", ""} {
		_, found, err := ledger.Find(context.Background(), requestID)
		if err != nil {
			t.Fatalf("Find(%q): %v", requestID, err)
		}
		if found {
			t.Fatalf("Find(%q) found = true, want false", requestID)
		}
	}
}

func TestRecordDoesNotBlockWhileTheWriterIsStalled(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000010, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)

	// Holding the single connection in an open transaction stalls every ledger insert behind
	// busy_timeout; Record must still return at once and shed rows rather than wait.
	blocking, err := testStore.db.Begin()
	if err != nil {
		t.Fatalf("Begin(): %v", err)
	}
	if _, err := blocking.Exec(`INSERT INTO request_accounting (request_id, recorded_at) VALUES ('blocker', '')`); err != nil {
		t.Fatalf("Exec(): %v", err)
	}

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		for index := range ledgerQueueDepth * 3 {
			ledger.Record(sampleRecord(string(rune('a'+index%26)) + string(rune('a'+index/26))))
		}
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Record() blocked behind the stalled writer")
	}
	if dropped := ledger.Stats().Dropped; dropped == 0 {
		t.Fatal("Stats().Dropped = 0, want the shed rows counted")
	}
	if err := blocking.Rollback(); err != nil {
		t.Fatalf("Rollback(): %v", err)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil: a shed row is a load condition, and failing the close exits the process 1", err)
	}
	if dropped := ledger.Stats().Dropped; dropped == 0 {
		t.Fatal("Stats().Dropped after Close() = 0, want the shed rows still counted for the exposition")
	}
}

func TestRetentionEvictsRowsPastTheMaxAge(t *testing.T) {
	leakcheck.VerifyNone(t)

	storageDir := t.TempDir()
	retention := Retention{MaxAge: 24 * time.Hour, MaxRows: 1_000}

	first, err := Open(storageDir)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	oldClock := newTestClock(time.Unix(1700000000, 0).UTC())
	oldLedger := openLedger(t, first, retention, oldClock)
	oldLedger.Record(sampleRecord("stale"))
	if err := first.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	second, err := Open(storageDir)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	defer second.Close()
	freshClock := newTestClock(time.Unix(1700000000, 0).UTC().Add(48 * time.Hour))
	freshLedger := openLedger(t, second, retention, freshClock)
	freshLedger.Record(sampleRecord("fresh"))
	if err := freshLedger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	if _, found, _ := second.FindRequest(context.Background(), "stale"); found {
		t.Fatal("stale row survived its max age")
	}
	if _, found, _ := second.FindRequest(context.Background(), "fresh"); !found {
		t.Fatal("fresh row was evicted")
	}
}

func TestRetentionEvictsTheOldestRowsPastMaxRows(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000000, 0).UTC())
	ledger := openLedger(t, testStore, Retention{MaxAge: 24 * time.Hour, MaxRows: 2}, clock)

	for _, requestID := range []string{"oldest", "middle", "newest"} {
		ledger.Record(sampleRecord(requestID))
		clock.Advance(time.Second)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	count := countAccountingRows(t, testStore)
	if count != 2 {
		t.Fatalf("accounting rows = %d, want 2", count)
	}
	if _, found, _ := testStore.FindRequest(context.Background(), "oldest"); found {
		t.Fatal("oldest row survived the row-count bound")
	}
	if _, found, _ := testStore.FindRequest(context.Background(), "newest"); !found {
		t.Fatal("newest row was evicted")
	}
}

// A sweep that cannot delete leaves the ledger growing past both of its bounds, so the two deletes
// report rather than pass. They are attempted independently, which is why both are counted.
func TestAFailedRetentionSweepIsCountedPerBound(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000000, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)
	t.Cleanup(func() {
		if err := ledger.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
	})
	if _, err := testStore.db.Exec(`DROP TABLE request_accounting`); err != nil {
		t.Fatalf("dropping the table the sweep deletes from: %v", err)
	}

	ledger.sweep(clock.Now())

	if got := ledger.Stats().SweepFailed; got != 2 {
		t.Fatalf("SweepFailed = %d, want 2 (the age bound and the row bound)", got)
	}
}

func TestNewLedgerRejectsAnUnboundedOrClocklessLedger(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000000, 0).UTC())
	testCases := []struct {
		name      string
		retention Retention
		now       func() time.Time
	}{
		{name: "no clock", retention: generousRetention()},
		{name: "no age bound", retention: Retention{MaxRows: 10}, now: clock.Now},
		{name: "no row bound", retention: Retention{MaxAge: time.Hour}, now: clock.Now},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := testStore.NewLedger(testCase.retention, testCase.now); err == nil {
				t.Fatal("NewLedger() error = nil, want a rejection")
			}
		})
	}
}

func TestStoreCloseDrainsAPendingLedgerWrite(t *testing.T) {
	leakcheck.VerifyNone(t)

	storageDir := t.TempDir()
	testStore, err := Open(storageDir)
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	clock := newTestClock(time.Unix(1700000000, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)
	ledger.Record(sampleRecord("request-1"))

	if err := testStore.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	reopened, err := Open(storageDir)
	if err != nil {
		t.Fatalf("reopening: %v", err)
	}
	defer reopened.Close()
	if _, found, _ := reopened.FindRequest(context.Background(), "request-1"); !found {
		t.Fatal("row queued at shutdown was lost")
	}
}

func TestRecordAfterCloseIsCountedNotPanicked(t *testing.T) {
	leakcheck.VerifyNone(t)

	testStore := openTestStore(t)
	clock := newTestClock(time.Unix(1700000000, 0).UTC())
	ledger := openLedger(t, testStore, generousRetention(), clock)
	if err := ledger.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	ledger.Record(sampleRecord("late"))
	if dropped := ledger.Stats().Dropped; dropped != 1 {
		t.Fatalf("Stats().Dropped = %d, want 1", dropped)
	}
	if err := ledger.Close(); err != nil {
		t.Fatalf("second Close(): %v", err)
	}
}

// Retention compares and orders these timestamps as text, so byte order has to agree with time order.
// RFC3339Nano trims a trailing zero fraction, which puts a whole second after the same second plus a
// tenth and prunes the wrong rows.
func TestStoredTimestampsSortInTimeOrder(t *testing.T) {
	base := time.Date(2026, 8, 2, 3, 0, 5, 0, time.UTC)
	ascending := []time.Time{
		base,
		base.Add(time.Nanosecond),
		base.Add(100 * time.Millisecond),
		base.Add(time.Second),
		base.Add(time.Minute),
	}

	for index := 1; index < len(ascending); index++ {
		earlier, later := FormatTime(ascending[index-1]), FormatTime(ascending[index])
		if earlier >= later {
			t.Fatalf("%q does not sort before %q, so a text-ordered sweep prunes out of order", earlier, later)
		}
	}
}
