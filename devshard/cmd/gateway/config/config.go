// Package config owns the gateway's immutable configuration snapshot:
// defaults ← environment ← admin overrides. A snapshot is never mutated after
// Build — reconfiguration swaps the whole snapshot (see Holder) — and Build
// clones every map/slice it takes from its inputs.
package config

import (
	"devshard/cmd/gateway/env"
)

// Server is the listener's own configuration. StorageDir is resolved by main before Build, which applies
// the default and never unsets it there.
type Server struct {
	Port                       int64
	StorageDir                 string
	APIKeys                    []string
	AdminAPIKey                string
	DevshardsJSON              string
	MaxConcurrentRuntimeBuilds int64
}

// GRPCEndpoint is what the escrow bridge dials. common/chain derives the CometBFT RPC host from it
// at the standard port, and that derived endpoint is the query fallback every escrow read inherits.
type Chain struct {
	PublicAPIBaseURL string
	GRPCEndpoint     string
	RPCEndpoint      string
	ChainID          string
}

type Tx struct {
	FeeDenom       string
	FeeAmount      int64
	GasLimit       int64
	PollIntervalMS int64
	PollTimeoutMS  int64
}

// Concurrency caps requests in flight. A zero MaxRequests is no static cap, leaving admission to
// the capacity-scaled per-weight limit alone.
type Concurrency struct {
	MaxRequests               int64
	RequestsPer10000Weight    float64
	PoCRequestsPer10000Weight float64
}

type HostInflight struct {
	Initial int64
	Max     int64
}

type HostCutoff struct {
	AfterFailures int64
	BaseMS        int64
	MaxMS         int64
}

// ModelLimits is the per-model override set. Token fields are required as a
// pair; pointer fields are optional — nil inherits the global limit.
type ModelLimits struct {
	DefaultMaxTokens       int64  `json:"default_max_tokens"`
	MaxTokensCap           int64  `json:"max_tokens_cap"`
	MaxConcurrentRequests  *int64 `json:"max_concurrent_requests,omitempty"`
	MaxInputTokensInFlight *int64 `json:"max_input_tokens_in_flight,omitempty"`
}

// Limits groups admission tuning. A zero MaxInputTokensInFlight is unlimited, MaxTokensCap bounds
// what a client may ask for and deliberately does not clamp DefaultMaxTokens, and ModelAccess maps a
// model to one of the tiers below.
type Limits struct {
	DefaultMaxTokens       int64
	ForceUpstreamStreaming bool
	MaxTokensCap           int64
	Concurrency            Concurrency
	MaxInputTokensInFlight int64
	AdmissionQueueWaitMS   int64
	AdmissionQueuePerSlot  int64
	HostInflight           HostInflight
	HostCutoff             HostCutoff
	ModelLimits            map[string]ModelLimits
	ModelAccess            map[string]string
}

type Modes struct {
	PoCMode             string
	Disabled            bool
	DisabledMessage     string
	DisabledRedirectURL string
}

type Rotation struct {
	Enabled           bool
	SettlementEnabled bool
	PrePoCBlocks      int64
	ModelsJSON        string
}

type Cache struct {
	ChatCacheMaxBytes int64
}

// Accounting bounds the per-request ledger on both axes; neither may be zero.
type Accounting struct {
	RetentionHours   int64
	RetentionMaxRows int64
}

// NonceAccounting configures the per-nonce ledger, which answers a different question than Accounting:
// where every committed nonce went, rather than what became of one client request. It reaches an
// operator as devshard_gateway_nonces_* on the gateway's own metrics endpoint. RetentionEpochs of 0
// keeps every epoch.
type NonceAccounting struct {
	Enabled         bool
	ListenAddr      string
	RetentionEpochs int64
	SnapshotSeconds int64
}

// Capture bounds the request-capture sink. An empty Dir means <storageDir>/captured-requests, SampleRate
// runs from 1 (every matching request) to 0 (none), and MaxBytes ceilings what the directory may hold.
type Capture struct {
	Enabled    bool
	Dir        string
	SampleRate float64
	MaxBytes   int64
}

type Stream struct {
	DrainTimeoutSeconds         int64
	ClassifyMaxAttemptBytes     int64
	ClassifyMaxParticipantBytes int64
	ClassifyMaxGlobalBytes      int64
}

// Perf groups Envoy-style host ejection tuning: consecutive-fail and rate-with-min-volume triggers, timed
// backoff, and the pool-wide ejection cap. MinAvailableHosts is a floor kept routable regardless of that
// cap, and host-model state unseen for the staleness window is evicted.
type Perf struct {
	EWMAHalfLifeSeconds      int64
	ConsecutiveFailThreshold int64
	FailureRateThreshold     float64
	FailureRateMinVolume     float64
	EjectionBaseSeconds      int64
	EjectionMaxSeconds       int64
	MaxEjectionFraction      float64
	MinAvailableHosts        int64
	HostStalenessSeconds     int64
}

// Engine groups race-escalation tuning; a zero MaxSpeculativeAttempts is bounded only by the host
// group, and the backstops it does not carry are engine constants. See gateway-speculative-race.md,
// "Tunables and backstops".
type Engine struct {
	ReceiptTimeoutMS       int64
	FirstTokenFloorMS      int64
	FirstTokenCeilingMS    int64
	InterChunkStallMS      int64
	LoserGraceMS           int64
	MaxSpeculativeAttempts int64
}

// Scheduler groups nonce-holding tuning. MatchWaitMS is how long a bound nonce waits for a co-arriving
// compatible request before it is burned; 0 burns immediately.
type Scheduler struct {
	MatchWaitMS          int64
	WarmNewEscrows       bool
	ChargeRefusedNonces  bool
	ParticipantAllowlist []string
}

// Config is the complete immutable gateway configuration snapshot.
type Config struct {
	Server     Server
	Chain      Chain
	Tx         Tx
	Limits     Limits
	Modes      Modes
	Rotation   Rotation
	Cache      Cache
	Accounting Accounting
	Capture    Capture
	Stream     Stream
	Perf       Perf
	Engine     Engine
	Scheduler  Scheduler

	NonceAccounting NonceAccounting
}

const (
	// PoC mode values accepted in Modes.PoCMode.
	PoCModeOff     = env.PoCModeOff
	PoCModeRelaxed = env.PoCModeRelaxed

	// Model access values accepted in Limits.ModelAccess.
	ModelAccessOpen      = "open"
	ModelAccessAPIKey    = "api_key"
	ModelAccessAdminOnly = "admin_only"

	minAdminAPIKeyLength = 16
)

// AdminEnabled reports whether admin routes are configured at all. Callers must gate on this rather
// than comparing a presented credential against AdminAPIKey, which would authenticate an empty one.
func (s Server) AdminEnabled() bool { return s.AdminAPIKey != "" }

// AccessFor resolves a model's access tier; a model outside a populated ModelAccess is admin-only, not
// open. See gateway-request-lifecycle.md, "3. Authorisation and routability".
func (l Limits) AccessFor(model string) string {
	if len(l.ModelAccess) == 0 {
		return ModelAccessOpen
	}
	if access, ok := l.ModelAccess[model]; ok {
		return access
	}
	return ModelAccessAdminOnly
}
