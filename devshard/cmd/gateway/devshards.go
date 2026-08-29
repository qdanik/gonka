package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	commonchain "common/chain"
	"golang.org/x/sync/errgroup"

	"devshard/bridge"
	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/store"
	"devshard/logging"
	"devshard/user"
)

// chainBackedSessions owns the chain connection because it is the only provider that needs one.
func chainBackedSessions(records devshardLookup, storageDir string) sessionSources {
	return func(endpoints config.Chain, routePrefix string) (chainSources, error) {
		// This client carries the CometBFT RPC query fallback. See README.md, "Escrow sessions and the chain connection".
		chainClient, err := commonchain.NewWithQueryFallback(endpoints.GRPCEndpoint, endpoints.RPCEndpoint)
		if err != nil {
			return chainSources{}, fmt.Errorf("dialing chain grpc %s: %w", endpoints.GRPCEndpoint, err)
		}
		grpcChain := chain.NewGRPCChain(chainClient, endpoints.ChainID)
		return chainSources{
			Serving:   servingSessions(records, storageDir, bridge.NewGRPCBridge(chainClient), routePrefix),
			ReadOnly:  readOnlySessions(records, storageDir),
			Reader:    grpcChain,
			Transport: grpcChain,
		}, nil
	}
}

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

// publishEscrows brings the registry to what the store calls active, builders at a time. See operations.md, "Boot".
func (g *gateway) publishEscrows(ctx context.Context) error {
	records, err := g.store.ListDevshards(ctx)
	if err != nil {
		return fmt.Errorf("listing devshards: %w", err)
	}
	return publishEscrows(ctx, records, g.builders, g.escrows.Add, g.escrows.Retire, func(escrowID string) error {
		return g.store.SetDevshardActive(ctx, escrowID, false)
	})
}

func publishEscrows(
	ctx context.Context,
	records []store.DevshardRecord,
	builders int,
	add func(ctx context.Context, escrowID, model string) error,
	retire func(escrowID string) error,
	deactivate func(escrowID string) error,
) error {
	var (
		active   []store.DevshardRecord
		problems []error
	)
	for _, record := range records {
		if record.Active {
			active = append(active, record)
			continue
		}
		if err := retire(record.EscrowID); err != nil {
			problems = append(problems, fmt.Errorf("retiring inactive escrow %s: %w", record.EscrowID, err))
		}
	}

	built := make([]error, len(active))
	var building errgroup.Group
	building.SetLimit(builders)
	for index, record := range active {
		building.Go(func() error {
			built[index] = add(ctx, record.EscrowID, record.Model)
			return nil
		})
	}
	_ = building.Wait()

	for index, err := range built {
		escrowID := active[index].EscrowID
		switch {
		case err == nil:
		case errors.Is(err, bridge.ErrEscrowNotFound), errors.Is(err, env.ErrPrivateKeyMissing):
			logging.Warn("devshard cannot be served, marking inactive", logkey.Escrow, escrowID, logkey.Error, err)
			if deactivateErr := deactivate(escrowID); deactivateErr != nil {
				problems = append(problems, fmt.Errorf("deactivating escrow %s: %w", escrowID, deactivateErr))
			}
		default:
			problems = append(problems, err)
		}
	}
	return errors.Join(problems...)
}

// republishOnDevshardWrites keeps routing in step with the rotation lifecycle, which knows no registry.
func (g *gateway) republishOnDevshardWrites(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-g.devshardWork:
				if err := g.publishEscrows(ctx); err != nil && ctx.Err() == nil {
					logging.Error("republishing escrows", logkey.Error, err)
				}
			}
		}
	}()
	return done
}

func notify(work chan struct{}) {
	select {
	case work <- struct{}{}:
	default:
	}
}

// devshardWrites reports changed rows, so a replacement escrow routes without waiting for a poll.
type devshardWrites struct {
	*store.Store
	changed func()
}

func (w devshardWrites) UpsertDevshard(ctx context.Context, record store.DevshardRecord) error {
	return w.report(w.Store.UpsertDevshard(ctx, record))
}

func (w devshardWrites) SetDevshardActive(ctx context.Context, escrowID string, active bool) error {
	return w.report(w.Store.SetDevshardActive(ctx, escrowID, active))
}

func (w devshardWrites) DeleteDevshard(ctx context.Context, escrowID string) error {
	return w.report(w.Store.DeleteDevshard(ctx, escrowID))
}

func (w devshardWrites) report(err error) error {
	if err == nil {
		w.changed()
	}
	return err
}

// depletionNotice breaks the registry/manager cycle: the manager settles through the registry.
type depletionNotice struct{ manager *escrow.Manager }

func (d *depletionNotice) OnBalanceExhausted(escrowID, reason string) {
	d.manager.OnBalanceExhausted(escrowID, reason)
}

// sessionSources is a parameter so the transport an escrow is served over is chosen once, at compose.
type sessionSources func(endpoints config.Chain, routePrefix string) (chainSources, error)

// chainSources is what one dial yields; Reader and Transport are interfaces so a test dials nothing.
type chainSources struct {
	Serving   registry.SessionFactory
	ReadOnly  registry.SessionFactory
	Reader    chain.Reader
	Transport chain.Transport
}

type seedDevshard struct {
	EscrowID      string `json:"escrow_id"`
	PrivateKeyEnv string `json:"private_key_env"`
	Model         string `json:"model"`
}
type devshardRegistry interface {
	ListDevshards(ctx context.Context) ([]store.DevshardRecord, error)
	UpsertDevshard(ctx context.Context, record store.DevshardRecord) error
}

// seedDevshards leaves a known devshard alone, so a restart cannot resurrect a deactivated one.
func seedDevshards(ctx context.Context, records devshardRegistry, raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var seeds []seedDevshard
	if err := json.Unmarshal([]byte(raw), &seeds); err != nil {
		return fmt.Errorf("parsing seed devshards: %w", err)
	}
	known, err := records.ListDevshards(ctx)
	if err != nil {
		return fmt.Errorf("listing devshards: %w", err)
	}
	registered := make(map[string]bool, len(known))
	for _, record := range known {
		registered[record.EscrowID] = true
	}
	for _, seed := range seeds {
		switch {
		case strings.TrimSpace(seed.EscrowID) == "":
			return fmt.Errorf("seed devshard: escrow_id is required")
		case strings.TrimSpace(seed.Model) == "":
			return fmt.Errorf("seed devshard %s: model is required", seed.EscrowID)
		case strings.TrimSpace(seed.PrivateKeyEnv) == "":
			return fmt.Errorf("seed devshard %s: private_key_env is required", seed.EscrowID)
		case registered[seed.EscrowID]:
			continue
		}
		record := store.DevshardRecord{
			EscrowID:      seed.EscrowID,
			PrivateKeyEnv: seed.PrivateKeyEnv,
			Model:         seed.Model,
			Active:        true,
		}
		if err := records.UpsertDevshard(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

type devshardLookup interface {
	ListDevshards(ctx context.Context) ([]store.DevshardRecord, error)
}

func findDevshard(ctx context.Context, records devshardLookup, escrowID string) (store.DevshardRecord, error) {
	known, err := records.ListDevshards(ctx)
	if err != nil {
		return store.DevshardRecord{}, fmt.Errorf("listing devshards: %w", err)
	}
	for _, record := range known {
		if record.EscrowID == escrowID {
			return record, nil
		}
	}
	return store.DevshardRecord{}, fmt.Errorf("devshard %s: %w", escrowID, escrow.ErrUnknownEscrow)
}

// sessionInputs resolves what both session factories need before they diverge on how they reach hosts.
func sessionInputs(ctx context.Context, records devshardLookup, storageDir, escrowID string) (store.DevshardRecord, string, string, error) {
	record, err := findDevshard(ctx, records, escrowID)
	if err != nil {
		return store.DevshardRecord{}, "", "", err
	}
	keyHex, err := env.PrivateKey(record.PrivateKeyEnv)
	if err != nil {
		return store.DevshardRecord{}, "", "", err
	}
	storagePath, err := escrowStorage(storageDir, escrowID)
	if err != nil {
		return store.DevshardRecord{}, "", "", err
	}
	return record, keyHex, storagePath, nil
}

// The bridge is one object for the process. See README.md, "Escrow sessions and the chain connection".
func servingSessions(records devshardLookup, storageDir string, escrowBridge bridge.MainnetBridge, routePrefix string) registry.SessionFactory {
	return func(ctx context.Context, escrowID string) (registry.EscrowSession, error) {
		record, keyHex, storagePath, err := sessionInputs(ctx, records, storageDir, escrowID)
		if err != nil {
			return nil, err
		}
		session, machine, err := user.NewHTTPSession(user.HTTPSessionConfig{
			PrivateKeyHex: keyHex,
			EscrowID:      escrowID,
			Bridge:        escrowBridge,
			StoragePath:   storagePath,
			RoutePrefix:   escrowRoutePrefix(record, routePrefix),
		})
		if err != nil {
			return nil, err
		}
		return registry.NewSessionHandle(session, machine), nil
	}
}

func escrowRoutePrefix(record store.DevshardRecord, gatewayPrefix string) string {
	if record.RoutePrefix != "" {
		return record.RoutePrefix
	}
	logging.Warn("escrow route prefix unpinned, following the gateway",
		logkey.Escrow, record.EscrowID, logkey.RoutePrefix, gatewayPrefix)
	return gatewayPrefix
}

func readOnlySessions(records devshardLookup, storageDir string) registry.SessionFactory {
	return func(ctx context.Context, escrowID string) (registry.EscrowSession, error) {
		_, keyHex, storagePath, err := sessionInputs(ctx, records, storageDir, escrowID)
		if err != nil {
			return nil, err
		}
		session, machine, err := user.NewLocalSession(user.LocalSessionConfig{
			PrivateKeyHex: keyHex,
			EscrowID:      escrowID,
			StoragePath:   storagePath,
		})
		if err != nil {
			return nil, err
		}
		return registry.NewSessionHandle(session, machine), nil
	}
}

func escrowStorage(storageDir, escrowID string) (string, error) {
	storagePath := api.DevshardStoragePath(storageDir, escrowID)
	if err := os.MkdirAll(storagePath, 0o755); err != nil {
		return "", fmt.Errorf("creating storage for escrow %s: %w", escrowID, err)
	}
	return storagePath, nil
}

// copySessionStorage copies regular files only: a directory below a flat SQLite set is not the escrow's.
func copySessionStorage(sourceDir, targetDir string) error {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return fmt.Errorf("reading session storage %s: %w", sourceDir, err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		contents, err := os.ReadFile(filepath.Join(sourceDir, entry.Name()))
		if err != nil {
			return fmt.Errorf("reading %s: %w", entry.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(targetDir, entry.Name()), contents, 0o600); err != nil {
			return fmt.Errorf("writing %s: %w", entry.Name(), err)
		}
	}
	return nil
}
