package app

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"common/storage/mode"

	"devshard/cmd/gateway/env"
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
}

func directoryAt(directory string) func() (string, error) {
	return func() (string, error) { return directory, nil }
}

// Test flow:
//  1. Leave the storage mode unset and choose the store on a directory that does not exist yet.
//  2. Assert the choice is SQLite carrying that directory, and opening it creates the database there.
func TestTheStoreIsSQLiteWhenTheModeIsUnset(t *testing.T) {
	t.Setenv(mode.EnvStorageMode, "")
	storageDir := filepath.Join(t.TempDir(), "state")
	choice, err := chooseStore(directoryAt(storageDir))
	if err != nil || choice != (sqliteChoice{directory: storageDir}) {
		t.Fatalf("chooseStore() = %+v, %v, want SQLite on %s", choice, err, storageDir)
	}
	opened, err := choice.open(context.Background())
	if err != nil {
		t.Fatalf("open() = %v, want nil", err)
	}
	defer opened.Close()
	if _, err := os.Stat(sqlite.DatabasePath(storageDir)); err != nil {
		t.Fatalf("no gateway database under the storage directory: %v", err)
	}
}

// Test flow:
//  1. Select hybrid mode and choose the store.
//  2. Assert the choice is refused.
func TestAModeTheGatewayDoesNotSupportChoosesNoStore(t *testing.T) {
	t.Setenv(mode.EnvStorageMode, "hybrid")
	if _, err := chooseStore(directoryAt(t.TempDir())); err == nil {
		t.Fatal("chooseStore() = nil, want a refusal")
	}
}

// Test flow:
//  1. Select Postgres while a directory holding a SQLite gateway database is on offer.
//  2. Assert the choice is Postgres for the instance and the directory was never asked for.
//  3. Assert the store opens on the empty Postgres database and the directory still holds only the file it started with.
func TestPostgresChoiceNeverAsksForADirectory(t *testing.T) {
	selectPostgres(t)
	storageDir := t.TempDir()
	if err := os.WriteFile(sqlite.DatabasePath(storageDir), nil, 0o600); err != nil {
		t.Fatalf("writing the SQLite database: %v", err)
	}
	choice, err := chooseStore(func() (string, error) {
		t.Error("the directory was resolved for a Postgres store")
		return storageDir, nil
	})
	if err != nil || choice != (postgresChoice{instanceID: "gw-test"}) {
		t.Fatalf("chooseStore() = %+v, %v, want Postgres for gw-test", choice, err)
	}
	opened, err := choice.open(context.Background())
	if err != nil {
		t.Fatalf("open() = %v, want nil", err)
	}
	opened.Close()
	entries, err := os.ReadDir(storageDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("storage directory = %v, %v, want only the SQLite file it started with", entries, err)
	}
}
