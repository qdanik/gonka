package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
	"devshard/host"
	"devshard/user"

	"github.com/jackc/pgx/v5"
)

type backend struct {
	name string
	open func(t *testing.T) *Store
}

var backends = []backend{
	{"sqlite", openTestStore},
	{"postgres", openPostgresStore},
}

func forEachBackend(t *testing.T, body func(t *testing.T, gatewayStore *Store)) {
	t.Helper()
	for _, candidate := range backends {
		t.Run(candidate.name, func(t *testing.T) { body(t, candidate.open(t)) })
	}
}

func requirePostgres(t *testing.T) {
	t.Helper()
	if os.Getenv("PGHOST") == "" {
		t.Skip("PGHOST is not set")
	}
}

func postgresSchema(t *testing.T) string {
	t.Helper()
	requirePostgres(t)
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("rand.Read(): %v", err)
	}
	schema := "gateway_test_" + hex.EncodeToString(suffix)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatalf("connecting with the PG* variables: %v", err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatalf("creating schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); err != nil {
			t.Errorf("dropping schema %s: %v", schema, err)
		}
		admin.Close(ctx)
	})
	t.Setenv("PGOPTIONS", "-c search_path="+schema)
	return schema
}

func openPostgresStore(t *testing.T) *Store {
	t.Helper()
	postgresSchema(t)
	gatewayStore, err := OpenPostgres(context.Background())
	if err != nil {
		t.Fatalf("OpenPostgres(): %v", err)
	}
	t.Cleanup(func() {
		if err := gatewayStore.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return gatewayStore
}

// Test flow:
//  1. On each backend, upsert three devshards whose ids differ only by case and read them back.
//  2. Assert the list is in byte order and a single read returns the row as written.
//  3. Walk one row through deactivate, activate, park, settle hash and chain facts with an amount above 32 bits.
//  4. Assert each step's flags, that a zero chain fact keeps its column, and that a parked row refuses the guarded writes.
//  5. Delete the row and assert a second delete reports it missing.
func TestBackendsDevshardRegistry(t *testing.T) {
	forEachBackend(t, func(t *testing.T, gatewayStore *Store) {
		ctx := context.Background()
		for _, id := range []string{"b", "a", "A"} {
			record := DevshardRecord{EscrowID: id, PrivateKeyEnv: "KEY_" + id, Model: "qwen", Active: true, RotationRole: RoleRegular, RotationEpoch: 7, RoutePrefix: "/" + id}
			if err := gatewayStore.UpsertDevshard(ctx, record); err != nil {
				t.Fatalf("UpsertDevshard(%s): %v", id, err)
			}
		}
		listed, err := gatewayStore.ListDevshards(ctx)
		if err != nil {
			t.Fatalf("ListDevshards(): %v", err)
		}
		var ids []string
		for _, record := range listed {
			ids = append(ids, record.EscrowID)
		}
		if !slices.Equal(ids, []string{"A", "a", "b"}) {
			t.Fatalf("ListDevshards() ids = %v, want [A a b]", ids)
		}
		got, found, err := gatewayStore.Devshard(ctx, "a")
		want := DevshardRecord{EscrowID: "a", PrivateKeyEnv: "KEY_a", Model: "qwen", Active: true, RotationRole: RoleRegular, RotationEpoch: 7, RoutePrefix: "/a"}
		if err != nil || !found || got != want {
			t.Fatalf("Devshard(a) = %+v, %v, %v; want %+v", got, found, err, want)
		}

		if err := gatewayStore.SetDevshardChainFacts(ctx, "a", 9, 5_000_000_000); err != nil {
			t.Fatalf("SetDevshardChainFacts(): %v", err)
		}
		if err := gatewayStore.SetDevshardChainFacts(ctx, "a", 0, 0); err != nil {
			t.Fatalf("SetDevshardChainFacts(zero): %v", err)
		}
		if err := gatewayStore.MarkDevshardGoneFromChain(ctx, "a"); err != nil {
			t.Fatalf("MarkDevshardGoneFromChain(): %v", err)
		}
		got, _, _ = gatewayStore.Devshard(ctx, "a")
		if got.ChainEpoch != 9 || got.Amount != 5_000_000_000 || got.Active || !got.GoneFromChain {
			t.Fatalf("after chain facts and gone: %+v, want epoch 9, amount 5000000000, inactive and gone", got)
		}
		if err := gatewayStore.SetDevshardActive(ctx, "a", true); err != nil {
			t.Fatalf("SetDevshardActive(true): %v", err)
		}
		got, _, _ = gatewayStore.Devshard(ctx, "a")
		if !got.Active || got.GoneFromChain {
			t.Fatalf("after activate: %+v, want active and back on chain", got)
		}
		parked, err := gatewayStore.ParkForSettlementIfActive(ctx, "a")
		if err != nil || !parked {
			t.Fatalf("ParkForSettlementIfActive() = %v, %v, want true", parked, err)
		}
		activated, err := gatewayStore.ActivateDevshardUnlessParked(ctx, "a")
		if err != nil || activated {
			t.Fatalf("ActivateDevshardUnlessParked() = %v, %v, want false for a parked row", activated, err)
		}
		wrote, err := gatewayStore.UpsertDevshardUnlessParked(ctx, want)
		if err != nil || wrote {
			t.Fatalf("UpsertDevshardUnlessParked() = %v, %v, want false for a parked row", wrote, err)
		}
		if err := gatewayStore.SetDevshardSettleTxHash(ctx, "a", "ABC123"); err != nil {
			t.Fatalf("SetDevshardSettleTxHash(): %v", err)
		}
		hash, at, err := gatewayStore.DevshardSettleTxHash(ctx, "a")
		if err != nil || hash != "ABC123" || time.Since(at) > time.Minute {
			t.Fatalf("DevshardSettleTxHash() = %q, %v, %v, want ABC123 stamped now", hash, at, err)
		}

		if err := gatewayStore.DeleteDevshard(ctx, "a"); err != nil {
			t.Fatalf("DeleteDevshard(): %v", err)
		}
		if err := gatewayStore.DeleteDevshard(ctx, "a"); !errors.Is(err, ErrDevshardNotFound) {
			t.Fatalf("second DeleteDevshard() = %v, want ErrDevshardNotFound", err)
		}
	})
}

// Test flow:
//  1. On each backend, save two commitments in reverse creation order, then save one again with new fields.
//  2. Assert the load is ordered by creation time and carries the replaced fields.
//  3. Delete one and assert only the other remains.
func TestBackendsCommitments(t *testing.T) {
	forEachBackend(t, func(t *testing.T, gatewayStore *Store) {
		ctx := context.Background()
		first := Commitment{TxHash: "TX-B", Model: "qwen", Role: RoleRegular, PrivateKeyEnv: "K", Epoch: 4, BlockHeight: 100, CreatedAt: time.Unix(1_700_000_000, 0).UTC()}
		second := Commitment{TxHash: "TX-A", Model: "qwen", Role: RoleReserve, Epoch: 5, BlockHeight: 200, CreatedAt: time.Unix(1_700_000_100, 0).UTC()}
		for _, commitment := range []Commitment{second, first} {
			if err := gatewayStore.SaveCommitment(ctx, commitment); err != nil {
				t.Fatalf("SaveCommitment(%s): %v", commitment.TxHash, err)
			}
		}
		second.BlockHeight = 250
		if err := gatewayStore.SaveCommitment(ctx, second); err != nil {
			t.Fatalf("SaveCommitment(again): %v", err)
		}
		loaded, err := gatewayStore.LoadCommitments(ctx)
		if err != nil || !slices.Equal(loaded, []Commitment{first, second}) {
			t.Fatalf("LoadCommitments() = %+v, %v; want %+v", loaded, err, []Commitment{first, second})
		}
		if err := gatewayStore.DeleteCommitment(ctx, "TX-B"); err != nil {
			t.Fatalf("DeleteCommitment(): %v", err)
		}
		loaded, _ = gatewayStore.LoadCommitments(ctx)
		if !slices.Equal(loaded, []Commitment{second}) {
			t.Fatalf("LoadCommitments() after delete = %+v, want only %+v", loaded, second)
		}
	})
}

// Test flow:
//  1. On each backend, save and replace the admin overrides, a rotation status, a model filter and a suspicious host.
//  2. Assert each read returns the replaced value, and that a second suspicious-host add is a no-op.
//  3. Remove the filter and the host and assert both lists are empty.
func TestBackendsSmallTables(t *testing.T) {
	forEachBackend(t, func(t *testing.T, gatewayStore *Store) {
		ctx := context.Background()
		for _, tokens := range []int64{1234, 999} {
			if err := gatewayStore.SaveOverrides(ctx, config.Overrides{Tunables: env.Tunables{DefaultMaxTokens: &tokens}}); err != nil {
				t.Fatalf("SaveOverrides(%d): %v", tokens, err)
			}
		}
		overrides, err := gatewayStore.LoadOverrides(ctx)
		if err != nil || overrides.DefaultMaxTokens == nil || *overrides.DefaultMaxTokens != 999 {
			t.Fatalf("LoadOverrides() = %+v, %v, want DefaultMaxTokens 999", overrides, err)
		}

		status := RotationStatus{Model: "qwen", Role: RoleRegular, Stage: "create", Epoch: 3, UpdatedAt: time.Unix(1_700_000_000, 0).UTC()}
		for _, completed := range []bool{false, true} {
			status.Completed = completed
			if err := gatewayStore.SaveRotationStatus(ctx, status); err != nil {
				t.Fatalf("SaveRotationStatus(): %v", err)
			}
		}
		statuses, err := gatewayStore.LoadRotationStatuses(ctx)
		if err != nil || !slices.Equal(statuses, []RotationStatus{status}) {
			t.Fatalf("LoadRotationStatuses() = %+v, %v; want %+v", statuses, err, status)
		}

		filter := ModelFilterRow{Model: "qwen", Preset: "strict", FiltersJSON: `{"a":1}`, UpdatedAt: "2026-01-01T00:00:00Z"}
		if err := gatewayStore.UpsertModelFilter(ctx, filter); err != nil {
			t.Fatalf("UpsertModelFilter(): %v", err)
		}
		filter.Preset = "loose"
		if err := gatewayStore.UpsertModelFilter(ctx, filter); err != nil {
			t.Fatalf("UpsertModelFilter(again): %v", err)
		}
		filters, err := gatewayStore.ListModelFilters(ctx)
		if err != nil || !slices.Equal(filters, []ModelFilterRow{filter}) {
			t.Fatalf("ListModelFilters() = %+v, %v; want %+v", filters, err, filter)
		}

		for range 2 {
			if err := gatewayStore.AddSuspiciousHost(ctx, "host-1"); err != nil {
				t.Fatalf("AddSuspiciousHost(): %v", err)
			}
		}
		hosts, err := gatewayStore.ListSuspiciousHosts(ctx)
		if err != nil || !slices.Equal(hosts, []string{"host-1"}) {
			t.Fatalf("ListSuspiciousHosts() = %v, %v; want [host-1]", hosts, err)
		}

		if err := gatewayStore.DeleteModelFilter(ctx, "qwen"); err != nil {
			t.Fatalf("DeleteModelFilter(): %v", err)
		}
		if err := gatewayStore.RemoveSuspiciousHost(ctx, "host-1"); err != nil {
			t.Fatalf("RemoveSuspiciousHost(): %v", err)
		}
		filters, _ = gatewayStore.ListModelFilters(ctx)
		hosts, _ = gatewayStore.ListSuspiciousHosts(ctx)
		if len(filters) != 0 || len(hosts) != 0 {
			t.Fatalf("after removal filters = %+v, hosts = %v, want none", filters, hosts)
		}
	})
}

// Test flow:
//  1. On each backend, open a ledger that keeps two rows and record three requests a minute apart.
//  2. Close the ledger, which sweeps.
//  3. Assert the oldest request is gone and the other two read back with every field.
func TestBackendsLedgerKeepsTheNewestRows(t *testing.T) {
	forEachBackend(t, func(t *testing.T, gatewayStore *Store) {
		clock := newTestClock(time.Unix(1_700_000_010, 0).UTC())
		ledger := openLedger(t, gatewayStore, Retention{MaxAge: 24 * time.Hour, MaxRows: 2}, clock)
		var written []RequestRecord
		for _, id := range []string{"request-1", "request-2", "request-3"} {
			record := sampleRecord(id)
			ledger.Record(record)
			record.RecordedAt = clock.Now()
			written = append(written, record)
			clock.Advance(time.Minute)
		}
		if err := ledger.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
		if _, found, _ := gatewayStore.FindRequest(context.Background(), "request-1"); found {
			t.Fatal("request-1 survived the row cap, want it swept")
		}
		for _, want := range written[1:] {
			got, found, err := gatewayStore.FindRequest(context.Background(), want.RequestID)
			if err != nil || !found || got != want {
				t.Fatalf("FindRequest(%s) = %+v, %v, %v; want %+v", want.RequestID, got, found, err, want)
			}
		}
	})
}

// Test flow:
//  1. On each backend, owe three votes and pay the second.
//  2. Drain the writer and load.
//  3. Assert the first and third come back with their prompts, ordered by nonce.
func TestBackendsOwedVotes(t *testing.T) {
	forEachBackend(t, func(t *testing.T, gatewayStore *Store) {
		owedVotes, err := gatewayStore.NewOwedVotes()
		if err != nil {
			t.Fatalf("NewOwedVotes(): %v", err)
		}
		for _, nonce := range []uint64{1, 2, 3} {
			owedVotes.Owe(owedVote(nonce, 64))
		}
		owedVotes.Paid("301", 2)
		votes := drainedOwedVotes(t, owedVotes)
		if len(votes) != 2 || votes[0].Nonce != 1 || votes[1].Nonce != 3 || len(votes[1].Prompt) != 64 {
			t.Fatalf("Load() = %+v, want nonces 1 and 3 with 64-byte prompts", votes)
		}
	})
}

// Test flow:
//  1. On each backend, save a completion, save it again unchanged, then save a different payload at its nonce.
//  2. Assert the repeat is accepted and the different payload is a conflict.
//  3. Save an earlier nonce and list, asserting nonce order; assert another escrow lists nothing.
//  4. Delete one entry and assert a second delete is a no-op.
func TestBackendsCompletionJournal(t *testing.T) {
	forEachBackend(t, func(t *testing.T, gatewayStore *Store) {
		entry := func(nonce uint64, prompt string) user.InferenceCompletion {
			return user.InferenceCompletion{Nonce: nonce, PreparedAt: 1_700_000_000, Payload: host.InferencePayload{Prompt: []byte(prompt), Model: "qwen", InputLength: 3, MaxTokens: 9, StartedAt: 5}}
		}
		if err := gatewayStore.SaveInferenceCompletion("301", entry(5, "p")); err != nil {
			t.Fatalf("SaveInferenceCompletion(): %v", err)
		}
		if err := gatewayStore.SaveInferenceCompletion("301", entry(5, "p")); err != nil {
			t.Fatalf("SaveInferenceCompletion(same): %v", err)
		}
		if err := gatewayStore.SaveInferenceCompletion("301", entry(5, "other")); !errors.Is(err, user.ErrInferenceCompletionConflict) {
			t.Fatalf("SaveInferenceCompletion(different) = %v, want ErrInferenceCompletionConflict", err)
		}
		if err := gatewayStore.SaveInferenceCompletion("301", entry(2, "q")); err != nil {
			t.Fatalf("SaveInferenceCompletion(2): %v", err)
		}
		listed, err := gatewayStore.ListInferenceCompletions("301")
		if err != nil || len(listed) != 2 || listed[0].Nonce != 2 || listed[1].Nonce != 5 || string(listed[1].Payload.Prompt) != "p" {
			t.Fatalf("ListInferenceCompletions() = %+v, %v; want nonces 2 and 5", listed, err)
		}
		if other, err := gatewayStore.ListInferenceCompletions("302"); err != nil || len(other) != 0 {
			t.Fatalf("ListInferenceCompletions(302) = %+v, %v; want none", other, err)
		}
		for range 2 {
			if err := gatewayStore.DeleteInferenceCompletion("301", 2); err != nil {
				t.Fatalf("DeleteInferenceCompletion(): %v", err)
			}
		}
		listed, _ = gatewayStore.ListInferenceCompletions("301")
		if len(listed) != 1 || listed[0].Nonce != 5 {
			t.Fatalf("after delete = %+v, want only nonce 5", listed)
		}
	})
}

// Test flow:
//  1. Open a Postgres store in an empty schema and read its schema version and marker row.
//  2. Open the same schema again with a row in place.
//  3. Assert the version is the migration count, the marker exists, and the row and version survive the second open.
func TestPostgresBootIsIdempotent(t *testing.T) {
	requirePostgres(t)
	postgresSchema(t)
	ctx := context.Background()
	first, err := OpenPostgres(ctx)
	if err != nil {
		t.Fatalf("OpenPostgres(): %v", err)
	}
	if err := first.AddSuspiciousHost(ctx, "host-1"); err != nil {
		t.Fatalf("AddSuspiciousHost(): %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	second, err := OpenPostgres(ctx)
	if err != nil {
		t.Fatalf("second OpenPostgres(): %v", err)
	}
	defer second.Close()
	var version, versions, markers int
	if err := second.db.QueryRow(`SELECT MAX(version), COUNT(*) FROM schema_version`).Scan(&version, &versions); err != nil {
		t.Fatalf("reading schema_version: %v", err)
	}
	if err := second.db.QueryRow(`SELECT COUNT(*) FROM ` + markerTable).Scan(&markers); err != nil {
		t.Fatalf("reading the marker: %v", err)
	}
	hosts, _ := second.ListSuspiciousHosts(ctx)
	if version != len(postgresMigrations) || versions != len(postgresMigrations) || markers != 1 || !slices.Equal(hosts, []string{"host-1"}) {
		t.Fatalf("version %d of %d rows, %d markers, hosts %v; want %d, one marker, [host-1]", version, versions, markers, hosts, len(postgresMigrations))
	}
}

// Test flow:
//  1. Create a schema holding a table the gateway did not create, with no marker.
//  2. Open a Postgres store on it.
//  3. Assert the open is refused with ErrForeignDatabase and the gateway created no table.
func TestPostgresRefusesADatabaseItDidNotCreate(t *testing.T) {
	requirePostgres(t)
	postgresSchema(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, "")
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, `CREATE TABLE devshard_sessions (id BIGINT)`); err != nil {
		t.Fatalf("creating a foreign table: %v", err)
	}
	if _, err := OpenPostgres(ctx); !errors.Is(err, ErrForeignDatabase) {
		t.Fatalf("OpenPostgres() = %v, want ErrForeignDatabase", err)
	}
	var tables int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = current_schema()`).Scan(&tables); err != nil || tables != 1 {
		t.Fatalf("tables after the refusal = %d, %v; want only the foreign one", tables, err)
	}
}

// Test flow:
//  1. Start four opens of one empty schema at the same time.
//  2. Assert every open succeeds and the schema version equals the migration count once.
func TestPostgresConcurrentBootsSerialize(t *testing.T) {
	requirePostgres(t)
	postgresSchema(t)
	results := make(chan error, 4)
	opened := make(chan *Store, 4)
	for range 4 {
		go func() {
			gatewayStore, err := OpenPostgres(context.Background())
			results <- err
			opened <- gatewayStore
		}()
	}
	var stores []*Store
	for range 4 {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("concurrent OpenPostgres(): %v", err)
			}
		case <-time.After(time.Minute):
			t.Fatal("timed out waiting for the concurrent boots to finish")
		}
		if gatewayStore := <-opened; gatewayStore != nil {
			stores = append(stores, gatewayStore)
		}
	}
	for _, gatewayStore := range stores {
		defer gatewayStore.Close()
	}
	if len(stores) == 0 {
		return
	}
	var versions int
	if err := stores[0].db.QueryRow(`SELECT COUNT(*) FROM schema_version`).Scan(&versions); err != nil || versions != len(postgresMigrations) {
		t.Fatalf("schema_version rows = %d, %v; want %d", versions, err, len(postgresMigrations))
	}
}

// Test flow:
//  1. Open both backends and list every table's columns, SQLite through pragma_table_info and Postgres through information_schema.
//  2. Assert both list the same tables with the same column names, apart from the marker table that only Postgres has.
func TestBackendSchemasHaveTheSameColumns(t *testing.T) {
	requirePostgres(t)
	sqliteColumns := columnsByTable(t, openTestStore(t), `SELECT m.name, p.name FROM sqlite_master m, pragma_table_info(m.name) p WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite_%'`)
	postgresColumns := columnsByTable(t, openPostgresStore(t), `SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = current_schema()`)
	delete(postgresColumns, markerTable)
	if len(sqliteColumns) == 0 || len(postgresColumns) != len(sqliteColumns) {
		t.Fatalf("tables: sqlite %d, postgres %d", len(sqliteColumns), len(postgresColumns))
	}
	for table, columns := range sqliteColumns {
		if !slices.Equal(columns, postgresColumns[table]) {
			t.Errorf("table %s: sqlite columns %v, postgres columns %v", table, columns, postgresColumns[table])
		}
	}
}

func columnsByTable(t *testing.T, gatewayStore *Store, query string) map[string][]string {
	t.Helper()
	rows, err := gatewayStore.db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("listing columns: %v", err)
	}
	defer rows.Close()
	columns := map[string][]string{}
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatalf("scanning columns: %v", err)
		}
		columns[table] = append(columns[table], column)
	}
	for _, names := range columns {
		slices.Sort(names)
	}
	return columns
}
