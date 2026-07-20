// Command gateway is the devshard gateway between the broker and race
// participants.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/store"
	"devshard/logging"
)

// Version is stamped by the build via -ldflags "-X main.Version=...".
var Version = "dev"

// shutdownGracePeriod bounds how long in-flight HTTP work may drain.
const shutdownGracePeriod = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		logging.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

// run wires the gateway and serves until ctx is cancelled. Shutdown order is
// a fixed contract: stop accepting HTTP first, close the store last.
func run(ctx context.Context) error {
	values, err := env.Load()
	if err != nil {
		return err
	}

	storageDir, err := resolveStorageDir(values.StorageDir)
	if err != nil {
		return err
	}
	values.StorageDir = &storageDir
	gatewayStore, err := store.Open(storageDir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := gatewayStore.Close(); closeErr != nil {
			logging.Error("closing store", "error", closeErr)
		}
	}()

	overrides, err := gatewayStore.LoadOverrides(ctx)
	if err != nil {
		return err
	}
	configuration, err := config.Build(values, overrides)
	if err != nil {
		return err
	}
	configHolder := config.NewHolder(configuration)

	gatewayMetrics := metrics.New()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", gatewayMetrics.InstrumentRoute("/metrics", gatewayMetrics.Handler()))

	port := configHolder.Load().Server.Port
	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.ListenAndServe() }()
	logging.Info("gateway started", "version", Version, "port", port, "storage_dir", storageDir)

	select {
	case err := <-serveResult:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancelDrain()
	if err := server.Shutdown(drainCtx); err != nil {
		return fmt.Errorf("shutting down http server: %w", err)
	}
	if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logging.Info("gateway stopped")
	return nil
}

// resolveStorageDir picks the storage directory: explicit value or the
// platform default ~/.cache/gonka-gateway (created if it doesn't exist).
func resolveStorageDir(explicit *string) (string, error) {
	if explicit != nil {
		return *explicit, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving home dir for storage: %w", err)
	}
	return filepath.Join(homeDir, ".cache", "gonka-gateway"), nil
}
