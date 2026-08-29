package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
)

// EscrowRegistry is the live escrow set as the HTTP boundary reads it; *registry.Registry satisfies it.
type EscrowRegistry interface {
	Serves(model string) bool
	Models() []string
	Candidates(model string) []scheduler.Escrow
	RoutableSession(escrowID string) (registry.EscrowSession, bool)
	SettlementSession(escrowID string) (registry.EscrowSession, bool)
	Inspect(ctx context.Context, escrowID string) (registry.EscrowSession, func(), error)
	IsBusy(escrowID string) bool
}

type InferenceEngine interface {
	Run(ctx context.Context, request engine.Request, client io.Writer) (engine.RaceOutcome, error)
}

// GatewayLimiter is the admission slot and input-token budget a request holds while it races.
type GatewayLimiter interface {
	AcquireForModel(ctx context.Context, model string, inputTokens int64, capacity limits.ModelCapacity) error
	ReleaseForModel(model string, inputTokens int64)
}

// CapacityReader yields the already block- and availability-filtered capacity one Acquire needs.
type CapacityReader interface {
	ForModel(model string) limits.ModelCapacity
}

type SnapshotSource interface {
	Snapshot() chain.PhaseSnapshot
}

// RequestLedger is the per-request accounting ledger; *store.Ledger satisfies it.
type RequestLedger interface {
	Record(record store.RequestRecord)
	Find(ctx context.Context, requestID string) (store.RequestRecord, bool, error)
}

type ControlStore interface {
	ListDevshards(ctx context.Context) ([]store.DevshardRecord, error)
	DeleteDevshard(ctx context.Context, escrowID string) error
	LoadOverrides(ctx context.Context) (config.Overrides, error)
	LoadRotationStatuses(ctx context.Context) ([]store.RotationStatus, error)
}

// CreateEscrowRequest is the body of POST /v1/admin/escrows. It names the variable holding the signing
// key, never the key. See gateway-operations.md, "Operator".
type CreateEscrowRequest struct {
	Model         string `json:"model"`
	Amount        uint64 `json:"amount"`
	PrivateKeyEnv string `json:"private_key_env"`
	Activate      bool   `json:"activate"`
}

type AddDevshardRequest struct {
	EscrowID      string `json:"escrow_id"`
	Model         string `json:"model"`
	PrivateKeyEnv string `json:"private_key_env"`
	Activate      bool   `json:"activate"`
}

type ImportDevshardRequest struct {
	EscrowID      string `json:"escrow_id"`
	Model         string `json:"model"`
	PrivateKeyEnv string `json:"private_key_env"`
	SourcePath    string `json:"source_path"`
	Activate      bool   `json:"activate"`
}

// Operations are the lifecycle actions the operator routes trigger. They span the chain client, the
// escrow manager and the registry, so the composition root implements them and this package calls them.
type Operations interface {
	CreateEscrow(ctx context.Context, request CreateEscrowRequest) (chain.CreateEscrowResult, error)
	AddDevshard(ctx context.Context, request AddDevshardRequest) error
	ImportDevshard(ctx context.Context, request ImportDevshardRequest) error
	Activate(ctx context.Context, escrowID string) error
	Deactivate(ctx context.Context, escrowID string) error
	Settle(ctx context.Context, escrowID string) (chain.SettleEscrowResult, error)
	Unquarantine(ctx context.Context, participantKey string) error
	Reconfigure(ctx context.Context, overrides config.Overrides) error
	ResetAccountingEpoch(ctx context.Context, epoch uint64) (cleared int, err error)
}

// SuspiciousHosts is the operator's manual never-trust-this-host list.
type SuspiciousHosts interface {
	List() []string
	Add(ctx context.Context, participantKey string) error
	Remove(ctx context.Context, participantKey string) error
}

type Telemetry interface {
	InstrumentRoute(routeLabel string, next http.Handler) http.Handler
	Handler() http.Handler
}

// LimitRejections counts what the gateway's own limiter turned away; *metrics.LimitRecorder satisfies it.
type LimitRejections interface {
	Rejected(model, reason string)
}

// Deps is everything the HTTP boundary reads or calls. New requires every field except Version and
// RequestIDs, which default to empty and to a random id generator.
type Deps struct {
	Config     *config.Holder
	Escrows    EscrowRegistry
	Inference  InferenceEngine
	Limiter    GatewayLimiter
	Capacity   CapacityReader
	Snapshots  SnapshotSource
	Control    ControlStore
	Accounting RequestLedger
	Operations Operations
	Suspicious SuspiciousHosts
	Telemetry  Telemetry
	Buffers    *BufferBudget
	Rejections LimitRejections
	StorageDir string
	Version    string
	Now        func() time.Time
	RequestIDs func() string
}

type Server struct {
	config     *config.Holder
	escrows    EscrowRegistry
	inference  InferenceEngine
	limiter    GatewayLimiter
	capacity   CapacityReader
	snapshots  SnapshotSource
	control    ControlStore
	accounting RequestLedger
	operations Operations
	suspicious SuspiciousHosts
	telemetry  Telemetry
	rejections LimitRejections
	storageDir string
	version    string
	now        func() time.Time
	requestIDs func() string
	buffers    *BufferBudget
	cache      *responseCache
	capture    *requestCapture

	verify  func(authorization string) credentials
	handler http.Handler
}

func New(deps Deps) (*Server, error) {
	switch {
	case deps.Config == nil:
		return nil, errors.New("api: Config is required")
	case deps.Escrows == nil:
		return nil, errors.New("api: Escrows is required")
	case deps.Inference == nil:
		return nil, errors.New("api: Inference is required")
	case deps.Limiter == nil:
		return nil, errors.New("api: Limiter is required")
	case deps.Capacity == nil:
		return nil, errors.New("api: Capacity is required")
	case deps.Snapshots == nil:
		return nil, errors.New("api: Snapshots is required")
	case deps.Control == nil:
		return nil, errors.New("api: Control is required")
	case deps.Accounting == nil:
		return nil, errors.New("api: Accounting is required")
	case deps.Operations == nil:
		return nil, errors.New("api: Operations is required")
	case deps.Suspicious == nil:
		return nil, errors.New("api: Suspicious is required")
	case deps.Telemetry == nil:
		return nil, errors.New("api: Telemetry is required")
	case deps.StorageDir == "":
		return nil, errors.New("api: StorageDir is required")
	case deps.Now == nil:
		return nil, errors.New("api: Now is required")
	}
	server := &Server{
		buffers:    deps.Buffers,
		config:     deps.Config,
		escrows:    deps.Escrows,
		inference:  deps.Inference,
		limiter:    deps.Limiter,
		capacity:   deps.Capacity,
		snapshots:  deps.Snapshots,
		control:    deps.Control,
		accounting: deps.Accounting,
		operations: deps.Operations,
		suspicious: deps.Suspicious,
		telemetry:  deps.Telemetry,
		rejections: deps.Rejections,
		storageDir: deps.StorageDir,
		version:    deps.Version,
		now:        deps.Now,
		requestIDs: deps.RequestIDs,
	}
	if server.requestIDs == nil {
		server.requestIDs = randomRequestID
	}
	configuration := deps.Config.Load()
	server.cache = newResponseCache(configuration.Cache.ChatCacheMaxBytes)
	capture, err := newRequestCapture(configuration.Capture, deps.StorageDir, server.now)
	if err != nil {
		return nil, err
	}
	server.capture = capture
	server.verify = server.compareKeys
	server.handler = server.buildHandler()
	return server, nil
}

// Handler is the process handler: one mux, no wrapper above it.
func (s *Server) Handler() http.Handler { return s.handler }

// HTTPServer builds the listener's server. Neither ReadTimeout nor WriteTimeout is set: both are
// absolute deadlines over an exchange that outlives the request, and either would cut an SSE stream
// mid-answer. Reading the body carries its own deadline, armed and cleared per request in readBody.
func (s *Server) HTTPServer(address string) *http.Server {
	return &http.Server{
		Addr:              address,
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

func randomRequestID() string {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return ""
	}
	return hex.EncodeToString(raw)
}

// CaptureStats reports the request-capture sink's own account of itself, for the metrics collector.
func (s *Server) CaptureStats() (written, refused, failed, held int64) { return s.capture.stats() }

// CacheStats reports the response cache's own account of itself, for the metrics collector.
func (s *Server) CacheStats() (hits, misses, entries, bytes int64) { return s.cache.stats() }
