// Package store persists gateway control-plane state in SQLite
// (<storageDir>/gateway.db): admin config overrides, the devshard registry,
// escrow rotation commitments, and rotation status.
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps the gateway.db handle. Access is serialized via a single
// connection; contention waits on busy_timeout instead of failing.
type Store struct {
	db           *sql.DB
	retryBackoff time.Duration
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
}

// Open creates storageDir if needed, opens gateway.db and migrates it.
func Open(storageDir string) (*Store, error) {
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating storage dir: %w", err)
	}
	databasePath := filepath.Join(storageDir, gatewayDatabaseFileName)
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, fmt.Errorf("opening gateway store: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("applying pragma %q: %w", pragma, err)
		}
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, retryBackoff: 200 * time.Millisecond}, nil
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

// Close releases the database handle.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing gateway store: %w", err)
	}
	return nil
}
