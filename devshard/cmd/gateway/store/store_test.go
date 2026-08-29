package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	testStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := testStore.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return testStore
}

func TestOpenCreatesDatabaseFileAndDirectory(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "nested", "storage")
	testStore, err := Open(baseDir)
	if err != nil {
		t.Fatalf("Open() with missing directory: %v", err)
	}
	defer testStore.Close()
	if _, err := os.Stat(filepath.Join(baseDir, "gateway.db")); err != nil {
		t.Fatalf("gateway.db not created: %v", err)
	}
}

func TestOverridesRoundTripAndEmptyLoad(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	empty, err := testStore.LoadOverrides(ctx)
	if err != nil {
		t.Fatalf("LoadOverrides() on fresh store: %v", err)
	}
	if empty.DefaultMaxTokens != nil {
		t.Fatal("fresh store must return zero-value overrides")
	}

	maxTokens := int64(1234)
	saved := config.Overrides{DefaultMaxTokens: &maxTokens}
	if err := testStore.SaveOverrides(ctx, saved); err != nil {
		t.Fatalf("SaveOverrides(): %v", err)
	}
	loaded, err := testStore.LoadOverrides(ctx)
	if err != nil {
		t.Fatalf("LoadOverrides() after save: %v", err)
	}
	if loaded.DefaultMaxTokens == nil || *loaded.DefaultMaxTokens != 1234 {
		t.Fatalf("loaded overrides = %+v, want DefaultMaxTokens 1234", loaded)
	}

	// Second save replaces, not appends.
	newTokens := int64(999)
	if err := testStore.SaveOverrides(ctx, config.Overrides{DefaultMaxTokens: &newTokens}); err != nil {
		t.Fatalf("second SaveOverrides(): %v", err)
	}
	replaced, err := testStore.LoadOverrides(ctx)
	if err != nil {
		t.Fatalf("LoadOverrides() after replace: %v", err)
	}
	if replaced.DefaultMaxTokens == nil || *replaced.DefaultMaxTokens != 999 {
		t.Fatalf("replaced overrides = %+v, want 999", replaced)
	}
}

func TestDevshardCRUDLifecycle(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	record := DevshardRecord{
		EscrowID:      "escrow-7",
		PrivateKeyEnv: "GATEWAY_KEY_ESCROW_7",
		Model:         "model-a",
		Active:        true,
		RotationRole:  "regular",
		RotationEpoch: 12,
	}
	if err := testStore.UpsertDevshard(ctx, record); err != nil {
		t.Fatalf("UpsertDevshard(): %v", err)
	}

	listed, err := testStore.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}
	if len(listed) != 1 || listed[0] != record {
		t.Fatalf("ListDevshards() = %+v, want exactly the upserted record", listed)
	}

	if err := testStore.SetDevshardActive(ctx, "escrow-7", false); err != nil {
		t.Fatalf("SetDevshardActive(): %v", err)
	}
	if err := testStore.SetDevshardSettlementPending(ctx, "escrow-7", true); err != nil {
		t.Fatalf("SetDevshardSettlementPending(): %v", err)
	}
	listed, err = testStore.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards() after updates: %v", err)
	}
	if listed[0].Active || !listed[0].SettlementPending {
		t.Fatalf("after updates got %+v, want inactive + settlement pending", listed[0])
	}

	// Upsert replaces fields for the same escrow.
	record.Model = "model-b"
	record.Active = false
	if err := testStore.UpsertDevshard(ctx, record); err != nil {
		t.Fatalf("UpsertDevshard() replace: %v", err)
	}
	listed, _ = testStore.ListDevshards(ctx)
	if listed[0].Model != "model-b" {
		t.Fatalf("after replace got model %q, want model-b", listed[0].Model)
	}

	if err := testStore.DeleteDevshard(ctx, "escrow-7"); err != nil {
		t.Fatalf("DeleteDevshard(): %v", err)
	}
	listed, _ = testStore.ListDevshards(ctx)
	if len(listed) != 0 {
		t.Fatalf("after delete list = %+v, want empty", listed)
	}
}

// Re-importing a parked escrow builds the record with SettlementPending at its zero value. Clearing
// the marker with it would strand the escrow: no traffic, never settled, no error.
func TestUpsertDevshardKeepsAQueuedSettlementWhileReplacingEveryOtherField(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	parked := DevshardRecord{EscrowID: "escrow-9", PrivateKeyEnv: "GATEWAY_KEY_9", Model: "model-a", RotationRole: "regular"}
	if err := testStore.UpsertDevshard(ctx, parked); err != nil {
		t.Fatalf("UpsertDevshard(): %v", err)
	}
	if err := testStore.SetDevshardSettlementPending(ctx, "escrow-9", true); err != nil {
		t.Fatalf("SetDevshardSettlementPending(): %v", err)
	}

	reimported := parked
	reimported.Model = "model-b"
	reimported.Active = true
	if err := testStore.UpsertDevshard(ctx, reimported); err != nil {
		t.Fatalf("UpsertDevshard() re-import: %v", err)
	}

	listed, err := testStore.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("ListDevshards() = %+v, want exactly one row", listed)
	}
	if !listed[0].SettlementPending {
		t.Fatal("the re-import cleared the queued settlement; nothing will ever settle that escrow")
	}
	if listed[0].Model != "model-b" || !listed[0].Active {
		t.Fatalf("re-imported row = %+v, want model-b and active (every other field replaced)", listed[0])
	}
}

func TestMissingDevshardReturnsSentinel(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()
	if err := testStore.SetDevshardActive(ctx, "ghost", true); !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("SetDevshardActive(ghost) = %v, want ErrDevshardNotFound", err)
	}
	if err := testStore.SetDevshardSettlementPending(ctx, "ghost", true); !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("SetDevshardSettlementPending(ghost) = %v, want ErrDevshardNotFound", err)
	}
	if err := testStore.DeleteDevshard(ctx, "ghost"); !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("DeleteDevshard(ghost) = %v, want ErrDevshardNotFound", err)
	}
}

func TestOpenIsIdempotentAcrossRestarts(t *testing.T) {
	baseDir := t.TempDir()
	first, err := Open(baseDir)
	if err != nil {
		t.Fatalf("first Open(): %v", err)
	}
	ctx := context.Background()
	if err := first.UpsertDevshard(ctx, DevshardRecord{EscrowID: "escrow-1", Model: "m", RotationRole: "regular"}); err != nil {
		t.Fatalf("UpsertDevshard(): %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	second, err := Open(baseDir) // migrations must be re-runnable
	if err != nil {
		t.Fatalf("second Open(): %v", err)
	}
	defer second.Close()
	listed, err := second.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards() after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].EscrowID != "escrow-1" {
		t.Fatalf("data lost across reopen: %+v", listed)
	}
}

// The DDL below is devshardctl's own, reduced to the colliding tables; see operations.md.
func TestOpenRefusesADevshardctlDatabase(t *testing.T) {
	dir := t.TempDir()

	seed, err := sql.Open("sqlite", filepath.Join(dir, gatewayDatabaseFileName))
	if err != nil {
		t.Fatalf("opening seed db: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE gateway_settings (id INTEGER PRIMARY KEY CHECK (id = 1), chain_rest TEXT NOT NULL)`,
		`CREATE TABLE gateway_devshards (escrow_id TEXT PRIMARY KEY, model TEXT NOT NULL)`,
		`CREATE TABLE gateway_rotation_status (escrow_id TEXT PRIMARY KEY, state TEXT NOT NULL)`,
		`INSERT INTO gateway_devshards (escrow_id, model) VALUES ('escrow-1', 'model-a')`,
	} {
		if _, err := seed.Exec(statement); err != nil {
			t.Fatalf("seeding a devshardctl db with %q: %v", statement, err)
		}
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("closing seed db: %v", err)
	}

	opened, err := Open(dir)
	if err == nil {
		opened.Close()
		t.Fatal("Open() accepted a devshardctl database, want a refusal")
	}
	if !errors.Is(err, ErrLegacyDatabase) {
		t.Fatalf("Open() error = %v, want ErrLegacyDatabase", err)
	}
}

func TestOpenUpgradesExistingV1Database(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	seed, err := sql.Open("sqlite", filepath.Join(dir, gatewayDatabaseFileName))
	if err != nil {
		t.Fatalf("opening seed db: %v", err)
	}
	for _, statement := range []string{
		`CREATE TABLE schema_version (version INTEGER NOT NULL)`,
		migrations[0],
		`INSERT INTO schema_version (version) VALUES (1)`,
		`INSERT INTO devshards (escrow_id, model) VALUES ('legacy-1', 'm')`,
	} {
		if _, err := seed.Exec(statement); err != nil {
			t.Fatalf("seeding v1 db with %q: %v", statement, err)
		}
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("closing seed db: %v", err)
	}

	upgraded, err := Open(dir)
	if err != nil {
		t.Fatalf("Open() upgrading a v1 db: %v", err)
	}
	t.Cleanup(func() { upgraded.Close() })

	devshards, err := upgraded.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards() after upgrade: %v", err)
	}
	if len(devshards) != 1 || devshards[0].EscrowID != "legacy-1" {
		t.Fatalf("v1 data lost on upgrade to v2: %+v", devshards)
	}

	created := time.Unix(0, 123456789).UTC()
	if err := upgraded.SaveCommitment(ctx, Commitment{TxHash: "tx-1", Model: "m", CreatedAt: created}); err != nil {
		t.Fatalf("SaveCommitment() into upgraded db: %v", err)
	}
	commitments, err := upgraded.LoadCommitments(ctx)
	if err != nil {
		t.Fatalf("LoadCommitments() after upgrade: %v", err)
	}
	if len(commitments) != 1 || !commitments[0].CreatedAt.Equal(created) {
		t.Fatalf("v2 table unusable after upgrade: %+v", commitments)
	}
}

// Parking is one statement because a crash between two would leave the row inactive and not pending,
// and no recovery path picks that up: settlement looks for pending rows, so the escrow would be out of
// service and never settled.
func TestParkingForSettlementSetsBothFieldsAtOnce(t *testing.T) {
	gatewayStore := openTestStore(t)
	ctx := context.Background()
	if err := gatewayStore.UpsertDevshard(ctx, DevshardRecord{EscrowID: "7", Model: "qwen", Active: true}); err != nil {
		t.Fatalf("UpsertDevshard: %v", err)
	}

	if err := gatewayStore.ParkForSettlement(ctx, "7"); err != nil {
		t.Fatalf("ParkForSettlement: %v", err)
	}

	records, err := gatewayStore.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	if records[0].Active {
		t.Fatal("a parked escrow is still routable")
	}
	if !records[0].SettlementPending {
		t.Fatal("a parked escrow is not pending, so nothing will ever settle it")
	}
}

func TestParkingAnUnknownEscrowIsReported(t *testing.T) {
	gatewayStore := openTestStore(t)

	err := gatewayStore.ParkForSettlement(context.Background(), "404")

	if !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("ParkForSettlement = %v, want ErrDevshardNotFound", err)
	}
}

// The pragmas are per-connection. Carried in the DSN they hold for every connection the pool opens;
// applied as statements afterwards they hold only for the one connection that ran them, and a
// recreated connection would come back with busy_timeout at 0 and synchronous back at FULL.
func TestEveryConnectionCarriesThePragmas(t *testing.T) {
	gatewayStore := openTestStore(t)

	for _, expected := range []struct {
		pragma string
		want   string
	}{
		{pragma: "journal_mode", want: "wal"},
		{pragma: "synchronous", want: "1"},
		{pragma: "busy_timeout", want: "5000"},
	} {
		var got string
		if err := gatewayStore.db.QueryRow("PRAGMA " + expected.pragma).Scan(&got); err != nil {
			t.Fatalf("PRAGMA %s: %v", expected.pragma, err)
		}
		if !strings.EqualFold(got, expected.want) {
			t.Fatalf("%s = %q, want %q", expected.pragma, got, expected.want)
		}
	}
}
