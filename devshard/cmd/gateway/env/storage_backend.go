package env

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"

	"common/storage/mode"
)

// InstanceIDVariable names this process in a shared Postgres database; it must be stable and unique per process.
const InstanceIDVariable = "GATEWAY_INSTANCE_ID"

var postgresVariables = []string{"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSSLMODE", "PG_CONNECT_TIMEOUT"}

// StorageBackend is where the gateway keeps its control-plane state. See ../docs/operations.md, "Storing state in Postgres".
type StorageBackend struct {
	Mode       mode.Mode
	InstanceID string
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
	hosts := strings.TrimSpace(os.Getenv("PGHOST"))
	if hosts == "" {
		problems = append(problems, fmt.Errorf("PGHOST is required when %s=postgres", mode.EnvStorageMode))
	}
	if err := requireTLSForRemoteHosts(hosts, strings.TrimSpace(os.Getenv("PGSSLMODE"))); err != nil {
		problems = append(problems, err)
	}
	if err := errors.Join(problems...); err != nil {
		return StorageBackend{}, err
	}
	return StorageBackend{Mode: mode.Postgres, InstanceID: instanceID}, nil
}

// requireTLSForRemoteHosts fails closed: libpq's default sslmode falls back to plaintext, which a remote database must not.
func requireTLSForRemoteHosts(hosts, sslMode string) error {
	if sslMode == "require" || sslMode == "verify-full" {
		return nil
	}
	for _, host := range strings.Split(hosts, ",") {
		if host = strings.TrimSpace(host); host != "" && !isLoopbackHost(host) {
			return fmt.Errorf("PGSSLMODE must be require or verify-full when PGHOST (%s) is not a loopback address, got %q", host, sslMode)
		}
	}
	return nil
}

func isLoopbackHost(host string) bool {
	if strings.HasPrefix(host, "/") || strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
