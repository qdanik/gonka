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
	"syscall"
	"time"

	devshardpkg "devshard"
	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/api"
	"devshard/cmd/gateway/burns"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/nonces"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
	"devshard/cmd/gateway/warmup"
	"devshard/logging"
	"devshard/signing"
	"devshard/transport"
)

// Version is stamped by the build via -ldflags "-X main.Version=...".
var Version = "dev"

const (
	shutdownGracePeriod = 10 * time.Second
	chainRequestTimeout = 10 * time.Second
)

func main() {
	// Before anything can log: a collector reads JSON fields as labels, where the default text line
	// carries log's own date prefix and has to be re-parsed.
	logging.ConfigureFormat(os.Getenv("GATEWAY_LOG_FORMAT"))
	if err := serve(); err != nil {
		logging.Error("gateway exited", logkey.Error, err)
		os.Exit(1)
	}
}

// serve owns the signal context so releasing it survives a panic; os.Exit skips a defer left in main.
func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx)
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
	publicAPI    *http.Client
	nonces       *nonces.Recorder
	warmup       *warmup.Prober

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

	sources, err := openSessions(configuration.Chain, routePrefix)
	if err != nil {
		return nil, err
	}
	observer, err := chain.NewPhaseObserver(chain.ObserverConfig{
		PublicAPIBaseURL: configuration.Chain.PublicAPIBaseURL,
		Chain:            sources.Reader,
		HTTPClient:       boot.client,
		Now:              clock,
	})
	if err != nil {
		return nil, err
	}
	txClient, err := chain.NewTxClient(chain.Config{
		Transport:    sources.Transport,
		FeeDenom:     configuration.Tx.FeeDenom,
		FeeAmount:    uint64(configuration.Tx.FeeAmount),
		GasLimit:     uint64(configuration.Tx.GasLimit),
		PollInterval: time.Duration(configuration.Tx.PollIntervalMS) * time.Millisecond,
		PollTimeout:  time.Duration(configuration.Tx.PollTimeoutMS) * time.Millisecond,
		Now:          clock,
	})
	if err != nil {
		return nil, err
	}

	recorder := nonces.Open(configuration.NonceAccounting, storageDir, observer, clock)

	participants := limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(configuration.Limits), clock)
	capacity := limits.NewCapacity(participants.Available)
	observer.Subscribe(capacity.Update)
	observer.Subscribe((&phaseNarrator{}).observe)
	gatewayLimiter := limits.NewGatewayLimiter(limits.GatewayConfigFromLimits(configuration.Limits))
	configHolder.Subscribe(func(next *config.Config) {
		gatewayLimiter.Reconfigure(limits.GatewayConfigFromLimits(next.Limits))
		participants.Reconfigure(limits.ParticipantConfigFromLimits(next.Limits))
	})
	hosts := perf.NewTracker(configHolder, clock)
	recorder.SetCapability(func(participant, model string) accounting.HostCapability {
		contextLimit, versionRefusals, toolRefusals, contextRefusals := hosts.Capability(participant, model)
		return accounting.HostCapability{
			ProtocolVersionUnsupported: versionRefusals > 0,
			ToolChoiceUnsupported:      toolRefusals > 0,
			ContextLimit:               contextLimit,
			VersionRefusals:            versionRefusals,
			ToolRefusals:               toolRefusals,
			ContextRefusals:            contextRefusals,
		}
	})
	telemetry := metrics.New()

	devshardWork := make(chan struct{}, 1)
	depletion := &depletionNotice{}
	escrows, router, prober, charges := newRouting(routingDeps{
		Sessions:     sources.Serving,
		ReadOnly:     sources.ReadOnly,
		Capacity:     capacity,
		Participants: participants,
		Hosts:        hosts,
		Snapshots:    observer,
		Config:       configHolder,
		Depletion:    depletion,
		Dispatches:   metrics.NewDispatchRecorder(telemetry),
		Ledger:       recorder,
		Now:          clock,
	})
	manager := escrow.NewManager(escrow.Deps{
		Tx:          txClient,
		Store:       devshardWrites{Store: gatewayStore, changed: func() { notify(devshardWork) }},
		Snapshots:   observer,
		Settlement:  escrows,
		Signer:      environmentSigner{},
		Config:      configHolder,
		Now:         clock,
		RoutePrefix: routePrefix,
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
	// The warmup and the burn charge both vote through the poster and observer the race already uses.
	raceObserver := nonceAccountedRaces{recorder: metrics.NewRaceRecorder(telemetry), ledger: recorder}
	prober.Settle(sessions.Poster, raceObserver)
	charges.Serve(escrows, sessions.Poster, raceObserver)
	races := engine.NewEngine(engine.Deps{
		Picker:     router,
		Targets:    sessions,
		Windows:    participants,
		Perf:       hosts,
		Snapshots:  observer,
		Config:     configHolder,
		Metrics:    raceObserver,
		Ledger:     api.NewRaceLedger(ledger),
		Lifecycle:  manager,
		Suspicious: suspicious.Suspicious,
		Timeouts:   sessions.Poster,
		Now:        clock,
	})
	// One wrapper for both readers: the gauge must report the scale admission actually applies, and
	// only this type knows the operator's relaxed-mode override of the chain's raw blocking state.
	modelCapacities := modelCapacity{capacity: capacity, snapshots: observer, config: configHolder}
	telemetry.Register(recorder.Collectors()...)
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
			nonces:       recorder,
		},
		Suspicious: suspicious,
		Telemetry:  telemetry,
		Rejections: metrics.NewLimitRecorder(telemetry),
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
		publicAPI:    boot.client,
		nonces:       recorder,
		warmup:       prober,
		builders:     boot.builders,
		devshardWork: devshardWork,
	}, nil
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
	Ledger       *nonces.Recorder
	Now          func() time.Time
}

// newRouting joins the escrow set to the picker through the capacity model: an escrow whose
// membership never reaches it scores as weightless, is skipped by every pick, and serves nothing.
func newRouting(deps routingDeps) (*registry.Registry, *scheduler.Scheduler, *warmup.Prober, *burns.Accountant) {
	// The warmup needs the registry it observes, so it is handed the registry once that exists.
	registryDeps := registry.Deps{
		ServingSessions:  deps.Sessions,
		ReadOnlySessions: deps.ReadOnly,
		Membership:       deps.Capacity,
		Exhaustion:       deps.Depletion,
		Now:              deps.Now,
	}
	// A nil warmup must not reach the interface field: a typed nil there is non-nil to a nil check.
	prober := warmup.New(deps.Config, deps.Ledger.Book(), deps.Now)
	if prober != nil {
		registryDeps.Publications = prober
	}
	charges := burns.New(deps.Now, func() bool { return deps.Config.Load().Scheduler.ChargeRefusedNonces })
	escrows := registry.New(registryDeps)
	prober.Serve(escrows)
	router := scheduler.NewScheduler(scheduler.Deps{
		Escrows:           escrows,
		Capacity:          deps.Capacity,
		Limiter:           deps.Participants,
		Perf:              deps.Hosts,
		Snapshots:         deps.Snapshots,
		Config:            deps.Config,
		Observer:          tracedDispatches{recorder: deps.Dispatches, ledger: deps.Ledger, charges: charges},
		Now:               deps.Now,
		OnEscrowExhausted: escrows.Exhausted,
	})
	return escrows, router, prober, charges
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
