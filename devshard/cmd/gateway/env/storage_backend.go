package env

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"common/storage/mode"
	"devshard/internal/boolvalue"
)

const (
	// InstanceIDVariable names this process in a shared Postgres database; it must be stable and unique per process.
	InstanceIDVariable = "GATEWAY_INSTANCE_ID"
	// AllowFreshPostgresVariable lets a gateway start on an empty Postgres database although a SQLite gateway database sits in the storage directory.
	AllowFreshPostgresVariable = "GATEWAY_POSTGRES_ALLOW_FRESH_START"
	postgresHostVariable       = "PGHOST"
)

var postgresVariables = []string{
	"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSSLMODE", "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE",
	"PG_CONNECT_TIMEOUT", "PG_OPERATION_TIMEOUT", "PG_POOL_MAX_CONNS",
}

// StorageBackend is where the gateway keeps its control-plane state. See ../docs/operations.md, "Storing state in Postgres".
type StorageBackend struct {
	Mode            mode.Mode
	InstanceID      string
	AllowFreshStart bool
}

// ResolveStorageBackend reads DEVSHARD_STORAGE_MODE: unset or sqlite is the local store, postgres is a shared one, anything else is refused, hybrid and auto included. The connection comes from the libpq PG* variables.
func ResolveStorageBackend() (StorageBackend, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(mode.EnvStorageMode)))
	switch mode.Mode(raw) {
	case "", mode.SQLite:
		return StorageBackend{Mode: mode.SQLite}, nil
	case mode.Postgres:
		return resolvePostgres()
	default:
		return StorageBackend{}, fmt.Errorf("%s %q is not supported by the gateway (want sqlite or postgres)", mode.EnvStorageMode, raw)
	}
}

func resolvePostgres() (StorageBackend, error) {
	var problems []error
	instanceID := strings.TrimSpace(os.Getenv(InstanceIDVariable))
	if instanceID == "" {
		problems = append(problems, fmt.Errorf("%s is required when %s=postgres", InstanceIDVariable, mode.EnvStorageMode))
	}
	if strings.TrimSpace(os.Getenv(postgresHostVariable)) == "" {
		problems = append(problems, fmt.Errorf("%s is required when %s=postgres", postgresHostVariable, mode.EnvStorageMode))
	}
	allowFresh, err := boolvalue.Parse(strings.TrimSpace(os.Getenv(AllowFreshPostgresVariable)))
	if err != nil {
		problems = append(problems, fmt.Errorf("%s: %w", AllowFreshPostgresVariable, err))
	}
	if err := errors.Join(problems...); err != nil {
		return StorageBackend{}, err
	}
	return StorageBackend{Mode: mode.Postgres, InstanceID: instanceID, AllowFreshStart: allowFresh}, nil
}

// IgnoredPostgresVariables names the PG* variables set while the store is SQLite, so an operator who set them hears they have no effect.
func IgnoredPostgresVariables() []string {
	var set []string
	for _, name := range postgresVariables {
		if _, present := os.LookupEnv(name); present {
			set = append(set, name)
		}
	}
	return set
}
