// Command gateway is the devshard gateway between the broker and race
// participants.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	commonchain "common/chain"
	devshardpkg "devshard"

	"devshard/bridge"
	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
	"devshard/logging"
	"devshard/signing"
	"devshard/transport"
	"devshard/user"

	json "github.com/goccy/go-json"
)

// Version is stamped by the build via -ldflags "-X main.Version=...".
var Version = "dev"

const (
	shutdownGracePeriod = 10 * time.Second
	chainRequestTimeout = 10 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		logging.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

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
	gateway, err := compose(ctx, values, storageDir, gatewayStore, chainBackedSessions(gatewayStore, storageDir))
	if err != nil {
		return errors.Join(err, gatewayStore.Close())
	}
	return gateway.serve(ctx)
}

// sessionSources opens the two kinds of escrow session, given the chain bridge and host route prefix
// compose resolves. It is a parameter so the transport an escrow is served over is chosen once, at the
// composition root.
type sessionSources func(endpoints config.Chain, routePrefix string) (serving, readOnly registry.SessionFactory, err error)

// chainBackedSessions owns the chain connection because it is the only provider that needs one: an
// in-process provider dials nothing, which is what keeps a test from carrying a live gRPC client.
func chainBackedSessions(records devshardLookup, storageDir string) sessionSources {
	return func(endpoints config.Chain, routePrefix string) (registry.SessionFactory, registry.SessionFactory, error) {
		// NewGRPCBridgeFromURL is upstream's test constructor; production builds the bridge over a client
		// carrying the CometBFT RPC query fallback, so an escrow read survives the gRPC query path failing.
		chainClient, err := commonchain.NewWithQueryFallback(endpoints.GRPCEndpoint, "")
		if err != nil {
			return nil, nil, fmt.Errorf("dialing chain grpc %s: %w", endpoints.GRPCEndpoint, err)
		}
		escrowBridge := bridge.NewGRPCBridge(chainClient)
		return servingSessions(records, storageDir, escrowBridge, routePrefix), readOnlySessions(records, storageDir), nil
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

type gateway struct {
	config       *config.Holder
	store        *store.Store
	observer     *chain.PhaseObserver
	escrows      *registry.Registry
	manager      *escrow.Manager
	router       *scheduler.Scheduler
	races        *engine.Engine
	limiter      *limits.GatewayLimiter
	participants *limits.ParticipantLimiter
	telemetry    *metrics.Metrics
	server       *http.Server
	chainClient  *http.Client

	builders     int
	devshardWork chan struct{}
}

func compose(ctx context.Context, values env.Values, storageDir string, gatewayStore *store.Store, openSessions sessionSources) (*gateway, error) {
	overrides, err := gatewayStore.LoadOverrides(ctx)
	if err != nil {
		return nil, err
	}
	configuration, err := config.Build(values, overrides)
	if err != nil {
		return nil, err
	}
	configHolder := config.NewHolder(configuration)
	clock := time.Now

	boot := newBootBudget(int(configuration.Server.MaxConcurrentRuntimeBuilds))
	routePrefix, _, err := devshardpkg.ResolveRoutePrefix(devshardpkg.VersionedRoutePrefix(Version))
	if err != nil {
		return nil, fmt.Errorf("resolving host route prefix from version %q: %w", Version, err)
	}

	observer, err := chain.NewPhaseObserver(chain.ObserverConfig{
		PublicAPIBaseURL: configuration.Chain.PublicAPIBaseURL,
		ChainRESTBaseURL: configuration.Chain.RESTBaseURL,
		HTTPClient:       boot.client,
		Now:              clock,
	})
	if err != nil {
		return nil, err
	}
	txClient, err := chain.NewTxClient(chain.Config{
		RESTBaseURL:         configuration.Chain.RESTBaseURL,
		TxQueryFallbackURLs: configuration.Chain.TxQueryFallbackURLs,
		FeeDenom:            configuration.Tx.FeeDenom,
		FeeAmount:           uint64(configuration.Tx.FeeAmount),
		GasLimit:            uint64(configuration.Tx.GasLimit),
		PollInterval:        time.Duration(configuration.Tx.PollIntervalMS) * time.Millisecond,
		PollTimeout:         time.Duration(configuration.Tx.PollTimeoutMS) * time.Millisecond,
		HTTPClient:          boot.client,
		Now:                 clock,
	})
	if err != nil {
		return nil, err
	}

	participants := limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(configuration.Limits), clock)
	capacity := limits.NewCapacity(participants.Available)
	observer.Subscribe(capacity.Update)
	observer.Subscribe((&phaseNarrator{}).observe)
	gatewayLimiter := limits.NewGatewayLimiter(limits.GatewayConfigFromLimits(configuration.Limits))
	configHolder.Subscribe(func(next *config.Config) {
		gatewayLimiter.Reconfigure(limits.GatewayConfigFromLimits(next.Limits))
	})
	hosts := perf.NewTracker(configHolder, clock)
	telemetry := metrics.New()

	devshardWork := make(chan struct{}, 1)
	depletion := &depletionNotice{}
	serving, readOnly, err := openSessions(configuration.Chain, routePrefix)
	if err != nil {
		return nil, err
	}
	escrows, router := newRouting(routingDeps{
		Sessions:     serving,
		ReadOnly:     readOnly,
		Capacity:     capacity,
		Participants: participants,
		Hosts:        hosts,
		Snapshots:    observer,
		Config:       configHolder,
		Depletion:    depletion,
		Dispatches:   metrics.NewDispatchRecorder(telemetry),
		Now:          clock,
	})
	manager := escrow.NewManager(escrow.Deps{
		Tx:         txClient,
		Store:      devshardWrites{Store: gatewayStore, changed: func() { notify(devshardWork) }},
		Snapshots:  observer,
		Settlement: escrows,
		Signer:     environmentSigner{},
		Config:     configHolder,
		Now:        clock,
	})
	depletion.manager = manager

	ledger, err := gatewayStore.NewLedger(store.Retention{
		MaxAge:  time.Duration(configuration.Accounting.RetentionHours) * time.Hour,
		MaxRows: configuration.Accounting.RetentionMaxRows,
	}, clock)
	if err != nil {
		return nil, err
	}

	suspicious, err := newSuspiciousHosts(ctx, gatewayStore)
	if err != nil {
		return nil, err
	}

	sessions := api.NewSessions(escrows)
	races := engine.NewEngine(engine.Deps{
		Picker:     router,
		Targets:    sessions,
		Windows:    participants,
		Perf:       hosts,
		Snapshots:  observer,
		Config:     configHolder,
		Metrics:    metrics.NewRaceRecorder(telemetry),
		Ledger:     api.NewRaceLedger(ledger),
		Lifecycle:  manager,
		Suspicious: suspicious.Suspicious,
		Timeouts:   sessions.Poster,
		Now:        clock,
	})
	// One wrapper for both readers: the gauge must report the scale admission actually applies, and
	// only this type knows the operator's relaxed-mode override of the chain's raw blocking state.
	modelCapacities := modelCapacity{capacity: capacity, snapshots: observer, config: configHolder}
	telemetry.Register(
		metrics.NewLimitsCollector(metrics.LimitsSources{
			Limiter:      gatewayLimiter,
			Capacity:     modelCapacities,
			Participants: participants,
			Models:       escrows.Models,
		}),
		metrics.NewPerfCollector(hosts),
		metrics.NewRegistryCollector(metrics.RegistrySources{
			Escrows:            escrows,
			EscrowWeight:       capacity.EscrowWeight,
			Available:          participants.Available,
			DrainCloseFailures: escrows.DrainCloseFailures,
		}),
		metrics.NewChainCollector(observer, clock),
		metrics.NewTransportCollector(transport.DefaultHostConnectionTracker()),
		metrics.NewAccountingCollector(ledger),
	)

	server, err := api.New(api.Deps{
		Config:     configHolder,
		Escrows:    escrows,
		Inference:  races,
		Limiter:    gatewayLimiter,
		Capacity:   modelCapacities,
		Snapshots:  observer,
		Control:    gatewayStore,
		Accounting: ledger,
		Operations: &operations{
			values:       values,
			config:       configHolder,
			store:        gatewayStore,
			escrows:      escrows,
			manager:      manager,
			participants: participants,
			storageDir:   storageDir,
		},
		Suspicious: suspicious,
		Telemetry:  telemetry,
		StorageDir: storageDir,
		Version:    Version,
		Now:        clock,
	})
	if err != nil {
		return nil, err
	}
	// Registered after the server, which owns the capture sink. Nothing evicts capture files, so the
	// refusal count is the only signal that capture has turned itself off at its byte cap.
	telemetry.Register(metrics.NewCaptureCollector(server))
	telemetry.Register(metrics.NewCacheCollector(server))

	return &gateway{
		config:       configHolder,
		store:        gatewayStore,
		observer:     observer,
		escrows:      escrows,
		manager:      manager,
		router:       router,
		races:        races,
		limiter:      gatewayLimiter,
		participants: participants,
		telemetry:    telemetry,
		server:       server.HTTPServer(fmt.Sprintf(":%d", configuration.Server.Port)),
		chainClient:  boot.client,
		builders:     boot.builders,
		devshardWork: devshardWork,
	}, nil
}

func (g *gateway) serve(ctx context.Context) error {
	backgroundCtx, stopBackground := context.WithCancel(ctx)
	defer stopBackground()

	g.observer.Start(backgroundCtx)
	configuration := g.config.Load()
	if err := seedDevshards(ctx, g.store, configuration.Server.DevshardsJSON); err != nil {
		return errors.Join(err, g.shutdown(shutdownGracePeriod))
	}
	if err := g.publishEscrows(ctx); err != nil {
		return errors.Join(err, g.shutdown(shutdownGracePeriod))
	}
	g.manager.Start(backgroundCtx)
	republished := g.republishOnDevshardWrites(backgroundCtx)

	serveResult := make(chan error, 1)
	go func() { serveResult <- g.server.ListenAndServe() }()
	logging.Info("gateway started",
		"version", Version, "port", configuration.Server.Port,
		"storage_dir", configuration.Server.StorageDir, "escrow_builders", g.builders)

	var listenErr error
	select {
	case listenErr = <-serveResult:
	case <-ctx.Done():
	}
	stopBackground()
	shutdownErr := g.shutdown(shutdownGracePeriod)
	<-republished
	if listenErr == nil {
		listenErr = <-serveResult
	}
	if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
		return errors.Join(fmt.Errorf("http server: %w", listenErr), shutdownErr)
	}
	logging.Info("gateway stopped")
	return shutdownErr
}

type routingDeps struct {
	Sessions     registry.SessionFactory
	ReadOnly     registry.SessionFactory
	Capacity     *limits.Capacity
	Participants *limits.ParticipantLimiter
	Hosts        *perf.Tracker
	Snapshots    *chain.PhaseObserver
	Config       *config.Holder
	Depletion    *depletionNotice
	Dispatches   *metrics.DispatchRecorder
	Now          func() time.Time
}

// newRouting joins the escrow set to the picker through the capacity model: an escrow whose
// membership never reaches it scores as weightless, is skipped by every pick, and serves nothing.
func newRouting(deps routingDeps) (*registry.Registry, *scheduler.Scheduler) {
	escrows := registry.New(registry.Deps{
		ServingSessions:  deps.Sessions,
		ReadOnlySessions: deps.ReadOnly,
		Membership:       deps.Capacity,
		Exhaustion:       deps.Depletion,
		Now:              deps.Now,
	})
	router := scheduler.NewScheduler(scheduler.Deps{
		Escrows:           escrows,
		Capacity:          deps.Capacity,
		Limiter:           deps.Participants,
		Perf:              deps.Hosts,
		Snapshots:         deps.Snapshots,
		Config:            deps.Config,
		Observer:          tracedDispatches{recorder: deps.Dispatches},
		Now:               deps.Now,
		OnEscrowExhausted: escrows.Exhausted,
	})
	return escrows, router
}

// needsQuiesced marks a step that destroys state the steps above it may still be using. See
// gateway-invariants.md, "6. Shutdown order is a contract".
type shutdownStep struct {
	name          string
	stop          func(context.Context) error
	needsQuiesced bool
}

type httpListener interface {
	Shutdown(ctx context.Context) error
}

type stopper interface{ Stop() }

// idleConnections is satisfied by *http.Client: the pooled chain client keeps sockets open between
// polls, and nothing above it in the order closes them.
type idleConnections interface{ CloseIdleConnections() }

// shutdownOrder is the eight-step contract every shutdown follows. See gateway-architecture.md,
// "Shutdown" and gateway-invariants.md, "6. Shutdown order is a contract".
func shutdownOrder(listener httpListener, races, dispatchers, escrowLifecycle, chainObserver stopper, sessions, storage io.Closer, chainClient idleConnections) []shutdownStep {
	return []shutdownStep{
		{name: "http server", stop: listener.Shutdown},
		{name: "races", stop: waitFor(races)},
		{name: "dispatchers", stop: waitFor(dispatchers)},
		{name: "escrow lifecycle", stop: waitFor(escrowLifecycle)},
		{name: "chain observer", stop: waitFor(chainObserver)},
		{name: "escrow sessions", stop: closeOf(sessions), needsQuiesced: true},
		{name: "store", stop: closeOf(storage)},
		// Last: every step above can still reach the chain, and an idle socket closed under one of them
		// is a socket the next poll has to re-dial.
		{name: "chain connections", stop: closeIdle(chainClient)},
	}
}

// waitFor bounds a drain by the shutdown budget without cancelling the work inside it. See
// gateway-architecture.md, "Shutdown".
func waitFor(component stopper) func(context.Context) error {
	return func(ctx context.Context) error {
		stopped := make(chan struct{})
		go func() { defer close(stopped); component.Stop() }()
		select {
		case <-stopped:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("abandoned with work still running: %w", ctx.Err())
		}
	}
}

func closeOf(component io.Closer) func(context.Context) error {
	return func(context.Context) error { return component.Close() }
}

func closeIdle(component idleConnections) func(context.Context) error {
	return func(context.Context) error {
		component.CloseIdleConnections()
		return nil
	}
}

// stopAll runs every step even after a failure, except one marked needsQuiesced, which is skipped and
// the skip reported. See gateway-architecture.md, "Shutdown".
func stopAll(ctx context.Context, steps []shutdownStep) error {
	var problems []error
	quiesced := true
	for _, step := range steps {
		if step.needsQuiesced && !quiesced {
			problems = append(problems, fmt.Errorf("skipping %s: work above it is still running", step.name))
			continue
		}
		if err := step.stop(ctx); err != nil {
			problems = append(problems, fmt.Errorf("stopping %s: %w", step.name, err))
			quiesced = false
		}
	}
	return errors.Join(problems...)
}

func (g *gateway) shutdown(grace time.Duration) error {
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), grace)
	defer cancelDrain()
	return stopAll(drainCtx, shutdownOrder(g.server, g.races, g.router, g.manager, g.observer, g.escrows, g.store, g.chainClient))
}

// bootBudget ties the two halves of a bounded boot: the concurrent-build limit and the idle pool those
// builds reuse. See gateway-architecture.md, "Boot".
type bootBudget struct {
	builders int
	client   *http.Client
}

func newBootBudget(builders int) bootBudget {
	if builders < 1 {
		builders = 1
	}
	pooled := http.DefaultTransport.(*http.Transport).Clone()
	pooled.MaxIdleConns = builders
	pooled.MaxIdleConnsPerHost = builders
	return bootBudget{
		builders: builders,
		client:   &http.Client{Timeout: chainRequestTimeout, Transport: pooled},
	}
}

// publishEscrows brings the registry to what the store calls active, builders at a time. See
// gateway-operations.md, "Start-up".
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
	semaphore := make(chan struct{}, builders)
	var building sync.WaitGroup
	for index, record := range active {
		building.Add(1)
		go func() {
			defer building.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			built[index] = add(ctx, record.EscrowID, record.Model)
		}()
	}
	building.Wait()

	for index, err := range built {
		escrowID := active[index].EscrowID
		switch {
		case err == nil:
		case errors.Is(err, bridge.ErrEscrowNotFound), errors.Is(err, env.ErrPrivateKeyMissing):
			logging.Warn("devshard cannot be served, marking inactive", "escrow_id", escrowID, "error", err)
			if deactivateErr := deactivate(escrowID); deactivateErr != nil {
				problems = append(problems, fmt.Errorf("deactivating escrow %s: %w", escrowID, deactivateErr))
			}
		default:
			problems = append(problems, err)
		}
	}
	return errors.Join(problems...)
}

// republishOnDevshardWrites keeps routing in step with the rotation lifecycle, which owns the rows
// and knows nothing about the registry. The returned channel closes once the watcher has exited.
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
					logging.Error("republishing escrows", "error", err)
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

// devshardWrites reports the rows the rotation lifecycle changes, so a replacement escrow routes
// without waiting for a poll and a retired one stops taking requests at once.
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

// depletionNotice breaks the registry/manager cycle: the manager settles through the registry, so it
// cannot also be constructed before it.
type depletionNotice struct{ manager *escrow.Manager }

func (d *depletionNotice) OnBalanceExhausted(escrowID string) { d.manager.OnBalanceExhausted(escrowID) }

type seedDevshard struct {
	EscrowID      string `json:"escrow_id"`
	PrivateKeyEnv string `json:"private_key_env"`
	Model         string `json:"model"`
}

type devshardRegistry interface {
	ListDevshards(ctx context.Context) ([]store.DevshardRecord, error)
	UpsertDevshard(ctx context.Context, record store.DevshardRecord) error
}

// seedDevshards leaves a devshard it already knows alone, so a restart cannot resurrect one an
// operator deactivated.
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

// The bridge is one object for the process: it holds the chain client every session reads escrow state
// through, so building one per session would open a connection per escrow and lose the client's cache.
func servingSessions(records devshardLookup, storageDir string, escrowBridge bridge.MainnetBridge, routePrefix string) registry.SessionFactory {
	return func(ctx context.Context, escrowID string) (registry.EscrowSession, error) {
		record, err := findDevshard(ctx, records, escrowID)
		if err != nil {
			return nil, err
		}
		keyHex, err := env.PrivateKey(record.PrivateKeyEnv)
		if err != nil {
			return nil, err
		}
		storagePath, err := escrowStorage(storageDir, escrowID)
		if err != nil {
			return nil, err
		}
		session, machine, err := user.NewHTTPSession(user.HTTPSessionConfig{
			PrivateKeyHex: keyHex,
			EscrowID:      escrowID,
			Bridge:        escrowBridge,
			StoragePath:   storagePath,
			RoutePrefix:   routePrefix,
		})
		if err != nil {
			return nil, err
		}
		return registry.NewSessionHandle(session, machine), nil
	}
}

func readOnlySessions(records devshardLookup, storageDir string) registry.SessionFactory {
	return func(ctx context.Context, escrowID string) (registry.EscrowSession, error) {
		record, err := findDevshard(ctx, records, escrowID)
		if err != nil {
			return nil, err
		}
		keyHex, err := env.PrivateKey(record.PrivateKeyEnv)
		if err != nil {
			return nil, err
		}
		storagePath, err := escrowStorage(storageDir, escrowID)
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

type environmentSigner struct{}

func (environmentSigner) SignerFor(privateKeyEnv string) (*signing.Secp256k1Signer, error) {
	keyHex, err := env.PrivateKey(privateKeyEnv)
	if err != nil {
		return nil, err
	}
	return signing.SignerFromHex(keyHex)
}

// modelCapacity's per-weight allowance follows the raw chain phase rather than the relaxed-mode
// override, because it bounds what the hosts can actually do.
type modelCapacity struct {
	capacity  *limits.Capacity
	snapshots *chain.PhaseObserver
	config    *config.Holder
}

func (m modelCapacity) ForModel(model string) limits.ModelCapacity {
	current, baseline := m.capacity.Weights(model)
	concurrency := m.config.Load().Limits.Concurrency
	perWeight := concurrency.RequestsPer10000Weight
	if m.snapshots.Snapshot().RequestsBlocked {
		perWeight = concurrency.PoCRequestsPer10000Weight
	}
	return limits.ModelCapacity{
		ScaleFactor:                 m.ScaleFactor(model),
		CurrentWeight:               current,
		BaselineWeight:              baseline,
		MaxConcurrentPer10000Weight: perWeight,
	}
}

// ScaleFactor applies the operator's relaxed-mode override before asking for the ratio. Reading the
// chain's raw blocking state instead would zero the scale during PoC, and a zero scale clamps every
// weight-derived cap to nothing -- so relaxed mode would go dead in exactly the deployments that set
// an input-token cap or a per-model override, which are the ones that need it.
func (m modelCapacity) ScaleFactor(model string) float64 {
	return m.capacity.ScaleFactor(model, blockedFor(m.snapshots.Snapshot(), m.config.Load().Modes))
}

func (m modelCapacity) Weights(model string) (current, baseline float64) {
	return m.capacity.Weights(model)
}

func (m modelCapacity) WeightsUnobserved(model string) bool {
	return m.capacity.WeightsUnobserved(model)
}

// blockedFor is the effective blocking state: the chain's raw fact, unless the operator's relaxed mode
// overrides it. api.admission folds the same override in for the pre-queue gate.
func blockedFor(snapshot chain.PhaseSnapshot, modes config.Modes) bool {
	return snapshot.RequestsBlocked && modes.PoCMode != config.PoCModeRelaxed
}

// suspiciousHosts is the operator's manual pin list, cached in memory because every race reads it and
// backed by the store because a pin outlives the incident that prompted it. The store is written
// first: a pin the gateway acts on but forgets on restart is the failure an operator cannot see.
type suspiciousHosts struct {
	pins suspiciousHostStore

	mu    sync.RWMutex
	hosts map[string]bool
}

type suspiciousHostStore interface {
	ListSuspiciousHosts(ctx context.Context) ([]string, error)
	AddSuspiciousHost(ctx context.Context, participantKey string) error
	RemoveSuspiciousHost(ctx context.Context, participantKey string) error
}

func newSuspiciousHosts(ctx context.Context, pins suspiciousHostStore) (*suspiciousHosts, error) {
	pinned, err := pins.ListSuspiciousHosts(ctx)
	if err != nil {
		return nil, err
	}
	hosts := make(map[string]bool, len(pinned))
	for _, participantKey := range pinned {
		hosts[participantKey] = true
	}
	return &suspiciousHosts{pins: pins, hosts: hosts}, nil
}

func (s *suspiciousHosts) Suspicious(participantKey string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.hosts[participantKey]
}

func (s *suspiciousHosts) List() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	listed := make([]string, 0, len(s.hosts))
	for host := range s.hosts {
		listed = append(listed, host)
	}
	sort.Strings(listed)
	return listed
}

func (s *suspiciousHosts) Add(ctx context.Context, participantKey string) error {
	if err := s.pins.AddSuspiciousHost(ctx, participantKey); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hosts[participantKey] = true
	return nil
}

func (s *suspiciousHosts) Remove(ctx context.Context, participantKey string) error {
	if err := s.pins.RemoveSuspiciousHost(ctx, participantKey); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.hosts, participantKey)
	return nil
}

// escrowLifecycle is the chain-facing half of an operator action; *escrow.Manager satisfies it.
type escrowLifecycle interface {
	CreateEscrow(ctx context.Context, model escrow.ModelConfig) (chain.CreateEscrowResult, error)
	Settle(ctx context.Context, escrowID string) (chain.SettleEscrowResult, error)
}

type operations struct {
	values       env.Values
	config       *config.Holder
	store        *store.Store
	escrows      *registry.Registry
	manager      escrowLifecycle
	participants *limits.ParticipantLimiter
	storageDir   string
}

// CreateEscrow takes the name of the variable holding the signing key, never the key. See
// gateway-operations.md, "Operator".
func (o *operations) CreateEscrow(ctx context.Context, request api.CreateEscrowRequest) (chain.CreateEscrowResult, error) {
	if strings.TrimSpace(request.PrivateKeyEnv) == "" {
		return chain.CreateEscrowResult{}, api.ErrPrivateKeyEnvRequired
	}
	result, err := o.manager.CreateEscrow(ctx, escrow.ModelConfig{
		ModelID:       request.Model,
		Amount:        request.Amount,
		PrivateKeyEnv: request.PrivateKeyEnv,
	})
	if err != nil {
		return chain.CreateEscrowResult{}, err
	}
	if !request.Activate {
		return result, o.Deactivate(ctx, escrowID(result.EscrowID))
	}
	return result, o.escrows.Add(ctx, escrowID(result.EscrowID), request.Model)
}

func (o *operations) AddDevshard(ctx context.Context, request api.AddDevshardRequest) error {
	return o.register(ctx, store.DevshardRecord{
		EscrowID:      request.EscrowID,
		PrivateKeyEnv: request.PrivateKeyEnv,
		Model:         request.Model,
		Active:        request.Activate,
	})
}

// ImportDevshard copies rather than references the storage, so the gateway owns the only handle to
// what it serves.
func (o *operations) ImportDevshard(ctx context.Context, request api.ImportDevshardRequest) error {
	storagePath, err := escrowStorage(o.storageDir, request.EscrowID)
	if err != nil {
		return err
	}
	if err := copySessionStorage(request.SourcePath, storagePath); err != nil {
		return err
	}
	return o.register(ctx, store.DevshardRecord{
		EscrowID:      request.EscrowID,
		PrivateKeyEnv: request.PrivateKeyEnv,
		Model:         request.Model,
		Active:        request.Activate,
	})
}

func (o *operations) register(ctx context.Context, record store.DevshardRecord) error {
	if _, err := env.PrivateKey(record.PrivateKeyEnv); err != nil {
		return err
	}
	if err := o.store.UpsertDevshard(ctx, record); err != nil {
		return err
	}
	if !record.Active {
		return nil
	}
	return o.escrows.Add(ctx, record.EscrowID, record.Model)
}

func (o *operations) Activate(ctx context.Context, id string) error {
	record, err := findDevshard(ctx, o.store, id)
	if err != nil {
		return err
	}
	if err := o.store.SetDevshardActive(ctx, id, true); err != nil {
		return err
	}
	return o.escrows.Add(ctx, id, record.Model)
}

// Deactivate and Settle stop routing before the row changes, so nothing new is admitted while the
// write runs. See gateway-operations.md, "Operator".
func (o *operations) Deactivate(ctx context.Context, id string) error {
	if err := o.escrows.Retire(id); err != nil {
		return err
	}
	return o.store.SetDevshardActive(ctx, id, false)
}

func (o *operations) Settle(ctx context.Context, id string) (chain.SettleEscrowResult, error) {
	if err := o.escrows.Retire(id); err != nil {
		return chain.SettleEscrowResult{}, err
	}
	return o.manager.Settle(ctx, id)
}

func (o *operations) Unquarantine(_ context.Context, participantKey string) error {
	if !o.participants.ClearQuarantine(participantKey) {
		return fmt.Errorf("%w: %s", api.ErrUnknownParticipant, participantKey)
	}
	return nil
}

func (o *operations) Reconfigure(ctx context.Context, overrides config.Overrides) error {
	next, err := config.Build(o.values, overrides)
	if err != nil {
		return err
	}
	if err := o.store.SaveOverrides(ctx, overrides); err != nil {
		return err
	}
	o.config.Swap(next)
	return nil
}

func escrowID(created uint64) string { return fmt.Sprintf("%d", created) }

// copySessionStorage copies regular files only: session storage is a flat set of SQLite files, so a
// directory below it is not part of the escrow.
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
