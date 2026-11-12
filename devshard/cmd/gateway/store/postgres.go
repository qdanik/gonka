package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"common/storage/pgpool"
	"common/storage/pgtimeouts"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

// ErrForeignDatabase reports a Postgres database this gateway did not create: it holds tables and no marker. See README.md, "Postgres".
var ErrForeignDatabase = errors.New("postgres database holds tables this gateway did not create; point the gateway at a dedicated, empty database")

const (
	postgresBootTimeout = 30 * time.Second
	markerTable         = "gateway_database_marker"
	bootLockName        = "gateway_store_boot"
)

// OpenPostgres connects with the libpq PG* variables, checks the database is the gateway's own and applies the migrations; it fails closed, with no SQLite fallback. See README.md, "Postgres".
func OpenPostgres(ctx context.Context) (*Store, error) {
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("reading Postgres settings: %w", err)
	}
	pgtimeouts.ApplyConnConfig(cfg.ConnConfig)
	if err := pgpool.ConfigureMaxConns(cfg); err != nil {
		return nil, err
	}
	db := stdlib.OpenDB(*cfg.ConnConfig)
	db.SetMaxOpenConns(int(cfg.MaxConns))
	db.SetMaxIdleConns(int(cfg.MaxConns))
	bootCtx, cancel := context.WithTimeout(ctx, postgresBootTimeout)
	defer cancel()
	if err := db.PingContext(bootCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connecting to the gateway Postgres database: %w", err)
	}
	if err := bootPostgres(bootCtx, db); err != nil {
		db.Close()
		return nil, err
	}
	return newStore(db, &postgresDialect{}, pgtimeouts.OperationTimeout()), nil
}

// bootPostgres is one transaction under an advisory lock, so two processes starting together cannot both create the marker or apply a version.
func bootPostgres(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning the Postgres boot: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, bootLockName); err != nil {
		return fmt.Errorf("locking the Postgres boot: %w", err)
	}
	if err := claimDatabase(ctx, tx); err != nil {
		return err
	}
	if err := migratePostgres(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing the Postgres boot: %w", err)
	}
	return nil
}

// claimDatabase leaves a database with the marker as it is, writes the marker into an empty one and refuses any other.
func claimDatabase(ctx context.Context, tx *sql.Tx) error {
	var marked bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = current_schema() AND tablename = $1::text)`, markerTable).Scan(&marked); err != nil {
		return fmt.Errorf("looking for the gateway marker table: %w", err)
	}
	if marked {
		return nil
	}
	var tables int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = current_schema()`).Scan(&tables); err != nil {
		return fmt.Errorf("counting tables of the Postgres database: %w", err)
	}
	if tables > 0 {
		return ErrForeignDatabase
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE `+markerTable+` (
		id BIGINT PRIMARY KEY CHECK (id = 1),
		created_at TEXT NOT NULL DEFAULT (`+postgresNow+`)
	)`); err != nil {
		return fmt.Errorf("creating the gateway marker table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+markerTable+` (id) VALUES (1)`); err != nil {
		return fmt.Errorf("writing the gateway marker: %w", err)
	}
	return nil
}

func migratePostgres(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (version BIGINT NOT NULL)`); err != nil {
		return fmt.Errorf("creating schema_version: %w", err)
	}
	var currentVersion int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&currentVersion); err != nil {
		return fmt.Errorf("reading schema version: %w", err)
	}
	for index := currentVersion; index < len(postgresMigrations); index++ {
		if _, err := tx.ExecContext(ctx, postgresMigrations[index]); err != nil {
			return fmt.Errorf("applying migration %d: %w", index+1, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_version (version) VALUES ($1)`, index+1); err != nil {
			return fmt.Errorf("recording migration %d: %w", index+1, err)
		}
	}
	return nil
}
