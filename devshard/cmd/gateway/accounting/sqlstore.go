package accounting

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"time"

	_ "modernc.org/sqlite"

	"devshard/types"
)

// The pragmas ride the connection string rather than a first statement, because the pool recreates
// connections and a recreated one would come back without them. One connection, because the ledger is
// written whole inside a single transaction and has no concurrent writer to gain from more.
const connectionPragmas = "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"

// Nothing queries this database except the ledger's own load at start-up: every question an operator
// asks is answered from memory. The tables therefore mirror the in-memory shape one for one, and a
// write is one transaction that empties and refills them. That is what makes a half-written ledger
// impossible -- a reader sees the previous contents until the commit, and a crash leaves them.
const schema = `
CREATE TABLE IF NOT EXISTS accounting_meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS accounting_escrows (
	escrow_id      TEXT PRIMARY KEY,
	model          TEXT    NOT NULL,
	creation_epoch INTEGER NOT NULL,
	latest_nonce   INTEGER NOT NULL,
	retired        INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS accounting_slots (
	escrow_id         TEXT    NOT NULL,
	slot_id           INTEGER NOT NULL,
	validator_address TEXT    NOT NULL,
	PRIMARY KEY (escrow_id, slot_id)
);
CREATE TABLE IF NOT EXISTS accounting_host_stats (
	escrow_id             TEXT    NOT NULL,
	slot_id               INTEGER NOT NULL,
	missed                INTEGER NOT NULL,
	invalid               INTEGER NOT NULL,
	cost                  INTEGER NOT NULL,
	required_validations  INTEGER NOT NULL,
	completed_validations INTEGER NOT NULL,
	PRIMARY KEY (escrow_id, slot_id)
);
CREATE TABLE IF NOT EXISTS accounting_counters (
	escrow_id      TEXT    NOT NULL,
	slot_id        INTEGER NOT NULL,
	disposition    TEXT    NOT NULL,
	ghost_reason   TEXT    NOT NULL,
	timeout_kind   TEXT    NOT NULL,
	timeout_action TEXT    NOT NULL,
	timeout_reason TEXT    NOT NULL,
	terminal       TEXT    NOT NULL,
	phase          TEXT    NOT NULL,
	slow_receipt     INTEGER NOT NULL,
	slow_chunk       INTEGER NOT NULL,
	clock_drifted    INTEGER NOT NULL,
	slow_decode      INTEGER NOT NULL,
	logprobs_decoded INTEGER NOT NULL,
	count            INTEGER NOT NULL,
	PRIMARY KEY (escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason,
	             terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded)
);
CREATE TABLE IF NOT EXISTS accounting_nonces (
	escrow_id      TEXT    NOT NULL,
	nonce          INTEGER NOT NULL,
	sent           INTEGER NOT NULL,
	acknowledged   INTEGER NOT NULL,
	usage          TEXT    NOT NULL,
	timeout_kind   TEXT    NOT NULL,
	timeout_action TEXT    NOT NULL,
	timeout_reason TEXT    NOT NULL,
	terminal       TEXT    NOT NULL,
	phase          TEXT    NOT NULL,
	slow_receipt     INTEGER NOT NULL,
	slow_chunk       INTEGER NOT NULL,
	clock_drifted    INTEGER NOT NULL,
	slow_decode      INTEGER NOT NULL,
	logprobs_decoded INTEGER NOT NULL,
	PRIMARY KEY (escrow_id, nonce)
);
`

var clearedTables = []string{
	"accounting_nonces",
	"accounting_counters",
	"accounting_host_stats",
	"accounting_slots",
	"accounting_escrows",
	"accounting_meta",
}

const (
	metaSchemaVersion = "schema_version"
	metaUpdatedAt     = "updated_at"
)

type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+connectionPragmas)
	if err != nil {
		return nil, fmt.Errorf("opening accounting store %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	if err := discardOutdatedSchema(db); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, errors.Join(fmt.Errorf("creating accounting schema: %w", err), db.Close())
	}
	return &Store{db: db}, nil
}

func discardOutdatedSchema(db *sql.DB) error {
	if currentSchema(db) {
		return nil
	}
	for _, table := range clearedTables {
		if _, err := db.Exec(`DROP TABLE IF EXISTS ` + table); err != nil {
			return fmt.Errorf("dropping %s of an accounting store this build cannot write: %w", table, err)
		}
	}
	return nil
}

// currentSchema reports whether this build can write what is on disk. An unreadable version says nothing
// about the tables beside it, so it is discarded rather than trusted -- on an empty store, six no-op drops.
func currentSchema(db *sql.DB) bool {
	var stored string
	if err := db.QueryRow(`SELECT value FROM accounting_meta WHERE key = ?`, metaSchemaVersion).Scan(&stored); err != nil {
		return false
	}
	if stored != strconv.Itoa(SchemaVersion) {
		return false
	}
	for _, probe := range []string{
		`SELECT terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded FROM accounting_counters LIMIT 0`,
		`SELECT terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded FROM accounting_nonces LIMIT 0`,
	} {
		if _, err := db.Exec(probe); err != nil {
			return false
		}
	}
	return true
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Save(ctx context.Context, book *Book) (err error) {
	if s == nil || s.db == nil {
		return nil
	}
	snapshot := book.Snapshot()
	transaction, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning accounting write: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, transaction.Rollback())
		}
	}()

	for _, table := range clearedTables {
		if _, err = transaction.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("clearing %s: %w", table, err)
		}
	}
	if _, err = transaction.ExecContext(ctx,
		`INSERT INTO accounting_meta (key, value) VALUES (?, ?), (?, ?)`,
		metaSchemaVersion, strconv.Itoa(SchemaVersion),
		metaUpdatedAt, snapshot.UpdatedAt.Format(time.RFC3339Nano),
	); err != nil {
		return fmt.Errorf("writing accounting meta: %w", err)
	}
	for _, escrow := range snapshot.Escrows {
		if err = writeEscrow(ctx, transaction, escrow); err != nil {
			return err
		}
	}
	if err = transaction.Commit(); err != nil {
		return fmt.Errorf("committing accounting write: %w", err)
	}
	return nil
}

func writeEscrow(ctx context.Context, transaction *sql.Tx, escrow EscrowSnapshot) error {
	identity := escrow.Metadata.EscrowID
	if _, err := transaction.ExecContext(ctx,
		`INSERT INTO accounting_escrows (escrow_id, model, creation_epoch, latest_nonce, retired)
		 VALUES (?, ?, ?, ?, ?)`,
		identity, escrow.Metadata.Model, escrow.Metadata.CreationEpoch, escrow.LatestNonce, escrow.Retired,
	); err != nil {
		return fmt.Errorf("writing escrow %s: %w", identity, err)
	}
	for _, slot := range escrow.Metadata.Slots {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_slots (escrow_id, slot_id, validator_address) VALUES (?, ?, ?)`,
			identity, slot.SlotID, slot.ValidatorAddress,
		); err != nil {
			return fmt.Errorf("writing slot %d of %s: %w", slot.SlotID, identity, err)
		}
	}
	for _, slotID := range slices.Sorted(maps.Keys(escrow.HostStats)) {
		stats := escrow.HostStats[slotID]
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_host_stats
			 (escrow_id, slot_id, missed, invalid, cost, required_validations, completed_validations)
			 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			identity, slotID, stats.Missed, stats.Invalid, stats.Cost,
			stats.RequiredValidations, stats.CompletedValidations,
		); err != nil {
			return fmt.Errorf("writing host stats for slot %d of %s: %w", slotID, identity, err)
		}
	}
	for _, counter := range escrow.Counters {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_counters
			 (escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason,
			  terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded, count)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			identity, counter.SlotID, counter.Disposition, counter.GhostReason,
			counter.TimeoutKind, counter.TimeoutAction, counter.TimeoutReason,
			counter.Terminal, counter.Phase, counter.SlowReceipt, counter.SlowChunk, counter.ClockDrifted,
			counter.SlowDecode, counter.LogprobsDecoded, counter.Count,
		); err != nil {
			return fmt.Errorf("writing a counter of %s: %w", identity, err)
		}
	}
	for _, stored := range escrow.Nonces {
		if _, err := transaction.ExecContext(ctx,
			`INSERT INTO accounting_nonces
			 (escrow_id, nonce, sent, acknowledged, usage, timeout_kind, timeout_action, timeout_reason,
			  terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			identity, stored.Nonce, stored.Sent, stored.Acknowledged, stored.Usage,
			stored.TimeoutKind, stored.TimeoutAction, stored.TimeoutReason,
			stored.Terminal, stored.Phase, stored.SlowReceipt, stored.SlowChunk, stored.ClockDrifted,
			stored.SlowDecode, stored.LogprobsDecoded,
		); err != nil {
			return fmt.Errorf("writing nonce %d of %s: %w", stored.Nonce, identity, err)
		}
	}
	return nil
}

func (s *Store) Load(ctx context.Context) (Snapshot, error) {
	snapshot := Snapshot{SchemaVersion: SchemaVersion}
	if s == nil || s.db == nil {
		return snapshot, nil
	}
	storedVersion, updatedAt, err := s.readMeta(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if storedVersion == 0 {
		return snapshot, nil
	}
	if storedVersion != SchemaVersion {
		return Snapshot{}, fmt.Errorf("accounting: stored schema %d is not %d", storedVersion, SchemaVersion)
	}
	snapshot.UpdatedAt = updatedAt

	escrows, order, err := s.readEscrows(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	for _, read := range []func(context.Context, map[string]*EscrowSnapshot) error{
		s.readSlots, s.readHostStats, s.readCounters, s.readNonces,
	} {
		if err := read(ctx, escrows); err != nil {
			return Snapshot{}, err
		}
	}
	for _, escrowID := range order {
		snapshot.Escrows = append(snapshot.Escrows, *escrows[escrowID])
	}
	return snapshot, nil
}

func (s *Store) readMeta(ctx context.Context) (int, time.Time, error) {
	var version int
	var updatedAt time.Time
	err := s.eachRow(ctx, `SELECT key, value FROM accounting_meta`, "accounting meta",
		func(rows *sql.Rows) error {
			var key, value string
			if err := rows.Scan(&key, &value); err != nil {
				return err
			}
			var parseErr error
			switch key {
			case metaSchemaVersion:
				version, parseErr = strconv.Atoi(value)
			case metaUpdatedAt:
				updatedAt, parseErr = time.Parse(time.RFC3339Nano, value)
			}
			if parseErr != nil {
				return fmt.Errorf("%s is malformed: %w", key, parseErr)
			}
			return nil
		})
	return version, updatedAt, err
}

func (s *Store) readEscrows(ctx context.Context) (map[string]*EscrowSnapshot, []string, error) {
	escrows := make(map[string]*EscrowSnapshot)
	var order []string
	err := s.eachRow(ctx,
		`SELECT escrow_id, model, creation_epoch, latest_nonce, retired
		 FROM accounting_escrows ORDER BY escrow_id`, "escrows",
		func(rows *sql.Rows) error {
			var stored EscrowSnapshot
			if err := rows.Scan(&stored.Metadata.EscrowID, &stored.Metadata.Model,
				&stored.Metadata.CreationEpoch, &stored.LatestNonce, &stored.Retired); err != nil {
				return err
			}
			stored.HostStats = make(map[uint32]types.HostStats)
			escrows[stored.Metadata.EscrowID] = &stored
			order = append(order, stored.Metadata.EscrowID)
			return nil
		})
	return escrows, order, err
}

func (s *Store) readSlots(ctx context.Context, escrows map[string]*EscrowSnapshot) error {
	return s.eachRow(ctx,
		`SELECT escrow_id, slot_id, validator_address FROM accounting_slots ORDER BY escrow_id, slot_id`,
		"slots",
		func(rows *sql.Rows) error {
			var escrowID string
			var slot types.SlotAssignment
			if err := rows.Scan(&escrowID, &slot.SlotID, &slot.ValidatorAddress); err != nil {
				return err
			}
			if escrow, known := escrows[escrowID]; known {
				escrow.Metadata.Slots = append(escrow.Metadata.Slots, slot)
			}
			return nil
		})
}

func (s *Store) readHostStats(ctx context.Context, escrows map[string]*EscrowSnapshot) error {
	return s.eachRow(ctx,
		`SELECT escrow_id, slot_id, missed, invalid, cost, required_validations, completed_validations
		 FROM accounting_host_stats`, "host stats",
		func(rows *sql.Rows) error {
			var escrowID string
			var slotID uint32
			var stats types.HostStats
			if err := rows.Scan(&escrowID, &slotID, &stats.Missed, &stats.Invalid, &stats.Cost,
				&stats.RequiredValidations, &stats.CompletedValidations); err != nil {
				return err
			}
			if escrow, known := escrows[escrowID]; known {
				escrow.HostStats[slotID] = stats
			}
			return nil
		})
}

func (s *Store) readCounters(ctx context.Context, escrows map[string]*EscrowSnapshot) error {
	return s.eachRow(ctx,
		`SELECT escrow_id, slot_id, disposition, ghost_reason, timeout_kind, timeout_action, timeout_reason,
		        terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded, count
		 FROM accounting_counters ORDER BY escrow_id, slot_id, disposition`, "counters",
		func(rows *sql.Rows) error {
			var escrowID string
			var counter PersistedCounter
			if err := rows.Scan(&escrowID, &counter.SlotID, &counter.Disposition, &counter.GhostReason,
				&counter.TimeoutKind, &counter.TimeoutAction, &counter.TimeoutReason,
				&counter.Terminal, &counter.Phase, &counter.SlowReceipt, &counter.SlowChunk, &counter.ClockDrifted,
				&counter.SlowDecode, &counter.LogprobsDecoded, &counter.Count); err != nil {
				return err
			}
			if escrow, known := escrows[escrowID]; known {
				escrow.Counters = append(escrow.Counters, counter)
			}
			return nil
		})
}

func (s *Store) readNonces(ctx context.Context, escrows map[string]*EscrowSnapshot) error {
	return s.eachRow(ctx,
		`SELECT escrow_id, nonce, sent, acknowledged, usage, timeout_kind, timeout_action, timeout_reason,
		        terminal, phase, slow_receipt, slow_chunk, clock_drifted, slow_decode, logprobs_decoded
		 FROM accounting_nonces ORDER BY escrow_id, nonce`, "nonces",
		func(rows *sql.Rows) error {
			var escrowID string
			var stored PersistedNonce
			if err := rows.Scan(&escrowID, &stored.Nonce, &stored.Sent, &stored.Acknowledged, &stored.Usage,
				&stored.TimeoutKind, &stored.TimeoutAction, &stored.TimeoutReason,
				&stored.Terminal, &stored.Phase, &stored.SlowReceipt, &stored.SlowChunk, &stored.ClockDrifted,
				&stored.SlowDecode, &stored.LogprobsDecoded); err != nil {
				return err
			}
			if escrow, known := escrows[escrowID]; known {
				escrow.Nonces = append(escrow.Nonces, stored)
			}
			return nil
		})
}

func (s *Store) eachRow(ctx context.Context, query, what string, scan func(*sql.Rows) error) error {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("reading %s: %w", what, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		if err := scan(rows); err != nil {
			return fmt.Errorf("reading %s: %w", what, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("reading %s: %w", what, err)
	}
	return nil
}
