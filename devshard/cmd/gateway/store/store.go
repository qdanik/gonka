// Package store persists gateway control-plane state in SQLite
// (<storageDir>/gateway.db): admin config overrides, the devshard registry,
// escrow rotation commitments, rotation status, the operator's suspicious-host
// pins, and the per-request accounting ledger.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps the gateway.db handle; access is serialized via a single connection. See
// gateway-escrow-lifecycle.md, "What the store holds".
type Store struct {
	db           *sql.DB
	retryBackoff time.Duration
	retryable    func(error) bool

	mu     sync.Mutex
	ledger *Ledger
}

const gatewayDatabaseFileName = "gateway.db"

// migrations run in order inside one transaction per version; the schema
// version table records progress so Open is idempotent.
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS config_overrides (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		overrides_json TEXT NOT NULL,
		updated_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE TABLE IF NOT EXISTS devshards (
		escrow_id TEXT PRIMARY KEY,
		private_key_env TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		rotation_role TEXT NOT NULL DEFAULT 'regular',
		rotation_epoch INTEGER NOT NULL DEFAULT 0,
		settlement_pending INTEGER NOT NULL DEFAULT 0,
		settle_tx_hash TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL DEFAULT (datetime('now')),
		updated_at TEXT NOT NULL DEFAULT (datetime('now'))
	);`,
	`CREATE TABLE IF NOT EXISTS escrow_rotation_commitments (
		tx_hash TEXT PRIMARY KEY,
		model TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT '',
		private_key_env TEXT NOT NULL DEFAULT '',
		epoch INTEGER NOT NULL DEFAULT 0,
		block_height INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS gateway_rotation_status (
		model TEXT NOT NULL,
		role TEXT NOT NULL DEFAULT '',
		stage TEXT NOT NULL DEFAULT '',
		epoch INTEGER NOT NULL DEFAULT 0,
		completed INTEGER NOT NULL DEFAULT 0,
		create_error TEXT NOT NULL DEFAULT '',
		updated_at TEXT NOT NULL,
		PRIMARY KEY (model, role)
	);`,
	`CREATE TABLE IF NOT EXISTS request_accounting (
		request_id TEXT PRIMARY KEY,
		escrow_id TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL DEFAULT '',
		outcome TEXT NOT NULL DEFAULT '',
		decision TEXT NOT NULL DEFAULT '',
		stream INTEGER NOT NULL DEFAULT 0,
		winner_nonce INTEGER NOT NULL DEFAULT 0,
		winner_participant TEXT NOT NULL DEFAULT '',
		winner_host TEXT NOT NULL DEFAULT '',
		winner_host_idx INTEGER NOT NULL DEFAULT 0,
		attempts INTEGER NOT NULL DEFAULT 0,
		input_tokens INTEGER NOT NULL DEFAULT 0,
		winner_output_tokens INTEGER NOT NULL DEFAULT 0,
		total_output_tokens INTEGER NOT NULL DEFAULT 0,
		escrow_missing INTEGER NOT NULL DEFAULT 0,
		balance_exhausted INTEGER NOT NULL DEFAULT 0,
		started_at TEXT NOT NULL DEFAULT '',
		completed_at TEXT NOT NULL DEFAULT '',
		first_token_ms INTEGER NOT NULL DEFAULT 0,
		duration_ms INTEGER NOT NULL DEFAULT 0,
		recorded_at TEXT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS request_accounting_recorded_at ON request_accounting (recorded_at);`,
	`CREATE TABLE IF NOT EXISTS suspicious_hosts (
		participant_key TEXT PRIMARY KEY,
		created_at TEXT NOT NULL DEFAULT (datetime('now'))
	);`,
	`ALTER TABLE devshards ADD COLUMN route_prefix TEXT NOT NULL DEFAULT ''`,
}

// The legacy-database guard is kept out of the migrations block above because it decides whether
// this storage dir may be migrated at all, and because those entries are raw SQL whose indentation
// is part of the literal.
var (
	// ErrLegacyDatabase reports a gateway.db written by devshardctl. Two table names collide at
	// different columns, so migrating would adopt the legacy shape instead of failing.
	ErrLegacyDatabase = errors.New("storage dir holds a devshardctl database; point the gateway at an empty storage dir")

	// legacyOnlyTables are created by devshardctl and never here, so their presence without a
	// schema_version is proof rather than a guess.
	legacyOnlyTables = []string{"gateway_settings", "gateway_devshards", "gateway_suspicious_hosts", "participant_throttle_state"}
)

// connectionPragmas is appended to the database path so every connection opens with them applied.
const connectionPragmas = "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"

func Open(storageDir string) (*Store, error) {
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating storage dir: %w", err)
	}
	databasePath := filepath.Join(storageDir, gatewayDatabaseFileName)
	// The pragmas travel in the DSN, not as statements after the open: they are per-connection, and a
	// connection the pool recreates would come back without them -- silently, with busy_timeout at 0
	// and synchronous back at FULL. In the DSN every connection carries them by construction.
	db, err := sql.Open("sqlite", databasePath+connectionPragmas)
	if err != nil {
		return nil, fmt.Errorf("opening gateway store: %w", err)
	}
	// One writer, kept for the life of the process: SQLite serializes writes anyway, and a second
	// connection would only add the lock contention the single connection makes impossible.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	if err := refuseLegacyDatabase(db, databasePath); err != nil {
		db.Close()
		return nil, err
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, retryBackoff: 200 * time.Millisecond, retryable: isLockedError}, nil
}

func refuseLegacyDatabase(db *sql.DB, databasePath string) error {
	present, err := tableExists(db, "schema_version")
	if err != nil || present {
		return err
	}
	for _, table := range legacyOnlyTables {
		switch present, err := tableExists(db, table); {
		case err != nil:
			return err
		case present:
			return fmt.Errorf("opening %s: %w", databasePath, ErrLegacyDatabase)
		}
	}
	return nil
}

func tableExists(db *sql.DB, table string) (bool, error) {
	var name string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("inspecting table %q: %w", table, err)
	}
	return true, nil
}

func migrate(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("creating schema_version: %w", err)
	}
	var currentVersion int
	row := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`)
	if err := row.Scan(&currentVersion); err != nil {
		return fmt.Errorf("reading schema version: %w", err)
	}
	for index := currentVersion; index < len(migrations); index++ {
		transaction, err := db.Begin()
		if err != nil {
			return fmt.Errorf("beginning migration %d: %w", index+1, err)
		}
		if _, err := transaction.Exec(migrations[index]); err != nil {
			transaction.Rollback()
			return fmt.Errorf("applying migration %d: %w", index+1, err)
		}
		if _, err := transaction.Exec(`INSERT INTO schema_version (version) VALUES (?)`, index+1); err != nil {
			transaction.Rollback()
			return fmt.Errorf("recording migration %d: %w", index+1, err)
		}
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("committing migration %d: %w", index+1, err)
		}
	}
	return nil
}

// Close drains the accounting ledger first, so no queued row outlives the connection it needs.
func (s *Store) Close() error {
	s.mu.Lock()
	ledger := s.ledger
	s.mu.Unlock()
	var ledgerErr error
	if ledger != nil {
		ledgerErr = ledger.Close()
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing gateway store: %w", err)
	}
	return ledgerErr
}
