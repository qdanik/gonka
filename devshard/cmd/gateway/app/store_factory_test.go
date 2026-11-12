package app

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"common/storage/mode"

	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/store/postgres"
	"devshard/cmd/gateway/store/sqlite"
	"devshard/cmd/gateway/store/storetest"
)

func selectPostgres(t *testing.T) {
	t.Helper()
	dsn, err := url.Parse(storetest.FreshPostgresSchema(t, storetest.PostgresDSN(t)))
	if err != nil {
		t.Fatalf("parsing the test DSN: %v", err)
	}
	password, _ := dsn.User.Password()
	t.Setenv(mode.EnvStorageMode, "postgres")
	t.Setenv(env.InstanceIDVariable, "gw-test")
	t.Setenv("PGHOST", dsn.Hostname())
	t.Setenv("PGPORT", dsn.Port())
	t.Setenv("PGUSER", dsn.User.Username())
	t.Setenv("PGPASSWORD", password)
	t.Setenv("PGDATABASE", dsn.Path[1:])
	t.Setenv("PGSSLMODE", "disable")
	t.Setenv("PGOPTIONS", "-c search_path="+dsn.Query().Get("search_path"))
	t.Setenv(env.AllowFreshPostgresVariable, "")
}

// Test flow:
//  1. Leave the storage mode unset and open the store on a directory that does not exist yet.
//  2. Assert the SQLite database is created under it.
func TestTheStoreIsSQLiteWhenTheModeIsUnset(t *testing.T) {
	t.Setenv(mode.EnvStorageMode, "")
	storageDir := filepath.Join(t.TempDir(), "state")
	opened, err := openStore(context.Background(), storageDir)
	if err != nil {
		t.Fatalf("openStore() = %v, want nil", err)
	}
	defer opened.Close()
	if !sqlite.DatabaseExists(storageDir) {
		t.Fatal("no gateway database under the storage directory, want the SQLite one")
	}
}

// Test flow:
//  1. Select hybrid mode and open the store.
//  2. Assert the open is refused and no database is created.
func TestAModeTheGatewayDoesNotSupportOpensNoStore(t *testing.T) {
	t.Setenv(mode.EnvStorageMode, "hybrid")
	storageDir := t.TempDir()
	if _, err := openStore(context.Background(), storageDir); err == nil {
		t.Fatal("openStore() = nil, want a refusal")
	}
	if sqlite.DatabaseExists(storageDir) {
		t.Fatal("a database was created for a refused mode")
	}
}

// Test flow:
//  1. Select Postgres on an empty database while the storage directory holds a SQLite gateway database.
//  2. Assert the open is refused, naming the file, and succeeds once the override variable is set.
func TestPostgresBesideASQLiteDatabaseNeedsTheFreshStartOverride(t *testing.T) {
	selectPostgres(t)
	storageDir := t.TempDir()
	if err := os.WriteFile(sqlite.DatabasePath(storageDir), nil, 0o600); err != nil {
		t.Fatalf("writing the SQLite database: %v", err)
	}
	_, err := openStore(context.Background(), storageDir)
	if !errors.Is(err, postgres.ErrEmptyDatabaseRefused) {
		t.Fatalf("openStore() = %v, want ErrEmptyDatabaseRefused", err)
	}
	t.Setenv(env.AllowFreshPostgresVariable, "true")
	opened, err := openStore(context.Background(), storageDir)
	if err != nil {
		t.Fatalf("openStore() with the override = %v, want nil", err)
	}
	opened.Close()
}
