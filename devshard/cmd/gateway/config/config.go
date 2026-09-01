// Package config owns the gateway's immutable configuration snapshot: defaults ← environment ← admin
// overrides, never mutated after Build. See README.md.
package config

import (
	"devshard/cmd/gateway/env"
)

// Server is the listener's own configuration; StorageDir is resolved by main before Build.
type Server struct {
	Port                       int64
	StorageDir                 string
	APIKeys                    []string
	AdminAPIKey                string
	DevshardsJSON              string
	MaxConcurrentRuntimeBuilds int64
}

// Chain addresses the network; a moved RPC port must be named, not derived. See README.md, "What each group holds".
type Chain struct {
	PublicAPIBaseURL      string
	GRPCEndpoint          string
	RPCEndpoint           string
	ChainID               string
	SnapshotMaxAgeSeconds int64
}

type Tx struct {
	FeeDenom       string
	FeeAmount      int64
	GasLimit       int64
	PollIntervalMS int64
	PollTimeoutMS  int64
}

// Concurrency caps requests in flight; a zero MaxRequests leaves only the per-weight limit.
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

// ModelLimits is the per-model override set; a nil pointer field inherits the global limit.
type ModelLimits struct {
	DefaultMaxTokens       int64  `json:"default_max_tokens"`
	MaxTokensCap           int64  `json:"max_tokens_cap"`
	MaxConcurrentRequests  *int64 `json:"max_concurrent_requests,omitempty"`
	MaxInputTokensInFlight *int64 `json:"max_input_tokens_in_flight,omitempty"`
}

// Limits groups admission tuning; several zeroes mean unlimited. See README.md, "What each group holds".
type Limits struct {
	DefaultMaxTokens         int64
	ForceUpstreamStreaming   bool
	MaxBufferedResponseBytes int64
	MaxTokensCap             int64
	Concurrency              Concurrency
	MaxInputTokensInFlight   int64
	AdmissionQueueWaitMS     int64
	AdmissionQueuePerSlot    int64
	HostInflight             HostInflight
	HostCutoff               HostCutoff
	ModelLimits              map[string]ModelLimits
	ModelAccess              map[string]string
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

// NonceAccounting configures the per-nonce ledger, not the per-request one; RetentionEpochs 0 keeps every epoch.
type NonceAccounting struct {
	Enabled         bool
	ListenAddr      string
	RetentionEpochs int64
	SnapshotSeconds int64
}

// Capture bounds the request-capture sink; an empty Dir means <storageDir>/captured-requests.
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

// Perf groups Envoy-style host ejection tuning. See README.md, "What each group holds".
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

// Engine groups race-escalation tuning. See race.md, "Tunables and backstops".
type Engine struct {
	ReceiptTimeoutMS       int64
	FirstTokenFloorMS      int64
	FirstTokenCeilingMS    int64
	InterChunkStallMS      int64
	LoserGraceMS           int64
	MaxSpeculativeAttempts int64
}

// Scheduler groups nonce-holding tuning; MatchWaitMS of 0 burns an unmatched nonce immediately.
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

// AdminEnabled is what callers must gate on: comparing against AdminAPIKey would authenticate an empty key.
func (s Server) AdminEnabled() bool { return s.AdminAPIKey != "" }

// AccessFor resolves a model's access tier. See operations.md, "Who may call what".
func (l Limits) AccessFor(model string) string {
	if len(l.ModelAccess) == 0 {
		return ModelAccessOpen
	}
	if access, ok := l.ModelAccess[model]; ok {
		return access
	}
	return ModelAccessAdminOnly
}
