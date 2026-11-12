package app

import (
	"context"
	"errors"
	"fmt"

	"common/storage/mode"

	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/store"
	"devshard/cmd/gateway/store/postgres"
	"devshard/cmd/gateway/store/sqlite"
	"devshard/logging"
)

func openStore(ctx context.Context, storageDir string) (store.Store, error) {
	backend, err := env.ResolveStorageBackend()
	if err != nil {
		return nil, err
	}
	if backend.Mode != mode.Postgres {
		if ignored := env.IgnoredPostgresVariables(); len(ignored) > 0 {
			logging.Warn("Postgres settings are ignored: the gateway store is SQLite unless DEVSHARD_STORAGE_MODE is postgres",
				logkey.Subsystem, "gateway", logkey.IgnoredVariables, ignored)
		}
		return sqlite.Open(storageDir)
	}
	logging.Info("the gateway store is Postgres",
		logkey.Subsystem, "gateway", logkey.StorageMode, string(backend.Mode), logkey.InstanceID, backend.InstanceID)
	opened, err := postgres.Open(ctx, postgres.Options{
		RefuseEmptyDatabase: sqlite.DatabaseExists(storageDir) && !backend.AllowFreshStart,
	})
	if errors.Is(err, postgres.ErrEmptyDatabaseRefused) {
		return nil, fmt.Errorf("%s holds a SQLite gateway database that is not copied into Postgres; set %s to start on the empty Postgres database anyway: %w",
			sqlite.DatabasePath(storageDir), env.AllowFreshPostgresVariable, err)
	}
	return opened, err
}
