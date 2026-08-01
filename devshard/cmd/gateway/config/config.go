// Package config owns the gateway's immutable configuration snapshot:
// defaults ← environment ← admin overrides. A snapshot is never mutated after
// Build — reconfiguration swaps the whole snapshot (see Holder) — and Build
// clones every map/slice it takes from its inputs.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/filters"
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

// GRPCEndpoint is what the escrow bridge dials. RPCEndpoint is optional: empty lets common/chain
// derive the CometBFT RPC host from the gRPC one, which is the query fallback every escrow read
// inherits.
type Chain struct {
	RESTBaseURL         string
	PublicAPIBaseURL    string
	GRPCEndpoint        string
	RPCEndpoint         string
	TxQueryFallbackURLs []string
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

// AIMD is the additive-increase/multiplicative-decrease per-host window tuning.
type AIMD struct {
	InitialWindow int64
	MaxWindow     int64
}

type Breaker struct {
	TripThreshold int64
	BaseOpenMS    int64
	MaxOpenMS     int64
}

// ModelLimits is the per-model override set. Token fields are required as a
// pair; pointer fields are optional — nil inherits the global limit.
type ModelLimits struct {
	DefaultMaxTokens       int64  `json:"default_max_tokens"`
	MaxTokensCap           int64  `json:"max_tokens_cap"`
	MaxConcurrentRequests  *int64 `json:"max_concurrent_requests,omitempty"`
	MaxInputTokensInFlight *int64 `json:"max_input_tokens_in_flight,omitempty"`
}

// Limits groups admission tuning. A zero MaxInputTokensInFlight is unlimited, MaxTokensCap is the
// ceiling DefaultMaxTokens is itself clamped to, and ModelAccess maps a model to one of the tiers below.
type Limits struct {
	DefaultMaxTokens       int64
	MaxTokensCap           int64
	Concurrency            Concurrency
	MaxInputTokensInFlight int64
	AcquireWaitMS          int64
	AIMD                   AIMD
	Breaker                Breaker
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

// Engine groups race-escalation tuning; a zero MaxSpeculativeAttempts is bounded only by the host group.
// The safety backstops it does not carry — streaming hard timeout, non-stream no-content timeout, max
// attempt wait, long-response exemption — are engine constants: they bound a request that every tunable
// already failed to bound.
type Engine struct {
	ReceiptTimeoutMS           int64
	FirstTokenFloorMS          int64
	InterChunkStallMS          int64
	LoserGraceMS               int64
	NonStreamResponseFloorMS   int64
	PerInputTokenResponseLagMS int64
	MaxSpeculativeAttempts     int64
}

// Scheduler groups nonce-holding tuning. HoldGraceMS is how long a bound nonce waits for a co-arriving
// compatible request before it is burned; 0 burns immediately.
type Scheduler struct {
	HoldGraceMS int64
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
}

// PoC mode values accepted in Modes.PoCMode.
const (
	PoCModeOff     = env.PoCModeOff
	PoCModeRelaxed = env.PoCModeRelaxed
)

// Model access values accepted in Limits.ModelAccess.
const (
	ModelAccessOpen      = "open"
	ModelAccessAPIKey    = "api_key"
	ModelAccessAdminOnly = "admin_only"
)

const minAdminAPIKeyLength = 16

// AdminEnabled reports whether admin routes are configured at all. Callers must gate on this rather
// than comparing a presented credential against AdminAPIKey, which would authenticate an empty one.
func (s Server) AdminEnabled() bool { return s.AdminAPIKey != "" }

// AccessFor resolves a model's access tier. An empty ModelAccess means no policy is configured and
// every model is open; once a policy exists, a model outside it is admin-only rather than open, so
// adding a model to the network cannot silently expose it.
func (l Limits) AccessFor(model string) string {
	if len(l.ModelAccess) == 0 {
		return ModelAccessOpen
	}
	if access, ok := l.ModelAccess[model]; ok {
		return access
	}
	return ModelAccessAdminOnly
}

func Defaults() Config {
	return Config{
		Server: Server{
			Port:                       8080,
			MaxConcurrentRuntimeBuilds: 16,
		},
		Chain: Chain{
			RESTBaseURL:         "http://localhost:1317",
			GRPCEndpoint:        "localhost:9090",
			PublicAPIBaseURL:    "http://localhost:9000",
			TxQueryFallbackURLs: []string{"http://node1.gonka.ai:8000/chain-api"},
		},
		Tx: Tx{
			FeeDenom:       chain.DefaultFeeDenom,
			FeeAmount:      int64(chain.DefaultFeeAmount),
			GasLimit:       int64(chain.DefaultGasLimit),
			PollIntervalMS: chain.DefaultPollInterval.Milliseconds(),
			PollTimeoutMS:  chain.DefaultPollTimeout.Milliseconds(),
		},
		Limits: Limits{
			DefaultMaxTokens: int64(filters.DefaultRequestMaxTokens),
			MaxTokensCap:     int64(filters.RequestMaxTokensCap),
			Concurrency: Concurrency{
				MaxRequests:               512,
				RequestsPer10000Weight:    5.0,
				PoCRequestsPer10000Weight: 10.0,
			},
			MaxInputTokensInFlight: 0,
			AcquireWaitMS:          500,
			AIMD: AIMD{
				InitialWindow: 4,
				MaxWindow:     64,
			},
			Breaker: Breaker{
				TripThreshold: 3,
				BaseOpenMS:    5_000,
				MaxOpenMS:     300_000,
			},
		},
		Modes: Modes{
			PoCMode: PoCModeOff,
		},
		Rotation: Rotation{
			PrePoCBlocks: 300,
		},
		Cache: Cache{
			ChatCacheMaxBytes: 256 << 20,
		},
		Accounting: Accounting{
			RetentionHours:   168,
			RetentionMaxRows: 1_000_000,
		},
		Capture: Capture{
			SampleRate: 1,
			MaxBytes:   1 << 30,
		},
		Stream: Stream{
			DrainTimeoutSeconds:         2_400,
			ClassifyMaxAttemptBytes:     1 << 20,
			ClassifyMaxParticipantBytes: 10 << 20,
			ClassifyMaxGlobalBytes:      100 << 20,
		},
		Perf: Perf{
			EWMAHalfLifeSeconds:      600,
			ConsecutiveFailThreshold: 5,
			FailureRateThreshold:     0.15,
			FailureRateMinVolume:     20,
			EjectionBaseSeconds:      30,
			EjectionMaxSeconds:       600,
			MaxEjectionFraction:      0.5,
			MinAvailableHosts:        1,
			HostStalenessSeconds:     3_600,
		},
		Engine: Engine{
			ReceiptTimeoutMS:           5_000,
			FirstTokenFloorMS:          1_000,
			InterChunkStallMS:          30_000,
			LoserGraceMS:               600_000,
			NonStreamResponseFloorMS:   20_000,
			PerInputTokenResponseLagMS: 20,
			MaxSpeculativeAttempts:     0,
		},
		Scheduler: Scheduler{
			HoldGraceMS: 200,
		},
	}
}

// Validate reports every problem in the snapshot at once. Field names in
// messages use snake_case to match the admin-API/JSON spelling.
func (c *Config) Validate() error {
	var problems []error
	complain := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf(format, args...))
	}

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		complain("port: %d out of range 1..65535", c.Server.Port)
	}
	if c.Server.MaxConcurrentRuntimeBuilds < 1 {
		complain("max_concurrent_runtime_builds: %d must be >= 1", c.Server.MaxConcurrentRuntimeBuilds)
	}
	checkBaseURL := func(name, value string) {
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			complain("%s: %q is not an absolute URL", name, value)
		}
	}
	checkBaseURL("chain_rest", c.Chain.RESTBaseURL)
	if strings.TrimSpace(c.Chain.GRPCEndpoint) == "" {
		complain("chain_grpc: required, the escrow bridge dials it")
	}
	checkBaseURL("public_api", c.Chain.PublicAPIBaseURL)
	for index, fallbackURL := range c.Chain.TxQueryFallbackURLs {
		checkBaseURL(fmt.Sprintf("tx_query_fallback_urls[%d]", index), fallbackURL)
	}

	if c.Tx.FeeAmount < 0 {
		complain("tx_fee_amount: %d must be >= 0", c.Tx.FeeAmount)
	}
	if c.Tx.GasLimit < 1 {
		complain("tx_gas_limit: %d must be >= 1", c.Tx.GasLimit)
	}
	if c.Tx.PollIntervalMS < 1 {
		complain("tx_poll_interval_ms: %d must be >= 1", c.Tx.PollIntervalMS)
	}
	if c.Tx.PollTimeoutMS < c.Tx.PollIntervalMS {
		complain("tx_poll_timeout_ms: %d must be >= tx_poll_interval_ms %d", c.Tx.PollTimeoutMS, c.Tx.PollIntervalMS)
	}

	if c.Limits.DefaultMaxTokens < 1 {
		complain("default_max_tokens: %d must be >= 1", c.Limits.DefaultMaxTokens)
	}
	if c.Limits.MaxTokensCap < 1 {
		complain("max_tokens_cap: %d must be >= 1", c.Limits.MaxTokensCap)
	}
	if c.Limits.Concurrency.MaxRequests < 0 {
		complain("max_concurrent_requests: %d must be >= 0", c.Limits.Concurrency.MaxRequests)
	}
	if c.Limits.Concurrency.RequestsPer10000Weight < 0 {
		complain("max_concurrent_requests_per_10000_weight: %v must be >= 0", c.Limits.Concurrency.RequestsPer10000Weight)
	}
	if c.Limits.Concurrency.PoCRequestsPer10000Weight < 0 {
		complain("poc_max_concurrent_requests_per_10000_weight: %v must be >= 0", c.Limits.Concurrency.PoCRequestsPer10000Weight)
	}
	if c.Limits.MaxInputTokensInFlight < 0 {
		complain("max_input_tokens_in_flight: %d must be >= 0", c.Limits.MaxInputTokensInFlight)
	}
	if c.Limits.AcquireWaitMS < 0 {
		complain("acquire_wait_ms: %d must be >= 0", c.Limits.AcquireWaitMS)
	}
	if c.Limits.AIMD.InitialWindow < 1 {
		complain("aimd_initial_window: %d must be >= 1", c.Limits.AIMD.InitialWindow)
	}
	if c.Limits.AIMD.MaxWindow < c.Limits.AIMD.InitialWindow {
		complain("aimd_max_window: %d must be >= aimd_initial_window %d", c.Limits.AIMD.MaxWindow, c.Limits.AIMD.InitialWindow)
	}
	if c.Limits.Breaker.TripThreshold < 1 {
		complain("breaker_trip_threshold: %d must be >= 1", c.Limits.Breaker.TripThreshold)
	}
	if c.Limits.Breaker.BaseOpenMS < 1 {
		complain("breaker_base_open_ms: %d must be >= 1", c.Limits.Breaker.BaseOpenMS)
	}
	if c.Limits.Breaker.MaxOpenMS < c.Limits.Breaker.BaseOpenMS {
		complain("breaker_max_open_ms: %d must be >= breaker_base_open_ms %d", c.Limits.Breaker.MaxOpenMS, c.Limits.Breaker.BaseOpenMS)
	}
	if perfEjectionMaxMS := c.Perf.EjectionMaxSeconds * 1000; c.Limits.Breaker.MaxOpenMS > perfEjectionMaxMS {
		complain("breaker_max_open_ms: %d must be <= perf_ejection_max_seconds %d (ms) so perf stays the dominant ejection authority", c.Limits.Breaker.MaxOpenMS, perfEjectionMaxMS)
	}
	// An admin key short enough to guess is worse than none, because "none" disables admin entirely
	// (AdminEnabled) while a weak one authenticates.
	if c.Server.AdminAPIKey != "" && len(c.Server.AdminAPIKey) < minAdminAPIKeyLength {
		complain("admin_api_key: must be at least %d characters when set (empty disables admin routes)", minAdminAPIKeyLength)
	}
	requiresAPIKey := false
	for model, access := range c.Limits.ModelAccess {
		if access != ModelAccessOpen && access != ModelAccessAPIKey && access != ModelAccessAdminOnly {
			complain("model_access[%s]: %q is not open, api_key or admin_only", model, access)
		}
		requiresAPIKey = requiresAPIKey || access == ModelAccessAPIKey
	}
	if requiresAPIKey && len(c.Server.APIKeys) == 0 {
		complain("api_keys: model_access marks a model api_key but no api keys are configured, so it is unreachable")
	}
	for model, limit := range c.Limits.ModelLimits {
		if limit.DefaultMaxTokens < 1 || limit.MaxTokensCap < 1 {
			complain("model_limits[%s]: default %d / cap %d invalid", model, limit.DefaultMaxTokens, limit.MaxTokensCap)
		}
		if limit.MaxConcurrentRequests != nil && *limit.MaxConcurrentRequests < 1 {
			complain("model_limits[%s]: max_concurrent_requests %d must be >= 1", model, *limit.MaxConcurrentRequests)
		}
		if limit.MaxInputTokensInFlight != nil && *limit.MaxInputTokensInFlight < 0 {
			complain("model_limits[%s]: max_input_tokens_in_flight %d must be >= 0", model, *limit.MaxInputTokensInFlight)
		}
	}

	if c.Modes.PoCMode != PoCModeOff && c.Modes.PoCMode != PoCModeRelaxed {
		complain("poc_mode: %q is not %q or %q", c.Modes.PoCMode, PoCModeOff, PoCModeRelaxed)
	}
	if c.Rotation.PrePoCBlocks < 0 {
		complain("rotation_pre_poc_blocks: %d must be >= 0", c.Rotation.PrePoCBlocks)
	}
	if c.Cache.ChatCacheMaxBytes < 0 {
		complain("chat_cache_max_bytes: %d must be >= 0", c.Cache.ChatCacheMaxBytes)
	}
	if c.Accounting.RetentionHours < 1 {
		complain("accounting_retention_hours: %d must be >= 1", c.Accounting.RetentionHours)
	}
	if c.Accounting.RetentionMaxRows < 1 {
		complain("accounting_retention_max_rows: %d must be >= 1", c.Accounting.RetentionMaxRows)
	}
	if c.Capture.SampleRate < 0 || c.Capture.SampleRate > 1 {
		complain("capture_sample_rate: %v must be in [0, 1]", c.Capture.SampleRate)
	}
	if c.Capture.MaxBytes < 1 {
		complain("capture_max_bytes: %d must be >= 1", c.Capture.MaxBytes)
	}
	if c.Stream.DrainTimeoutSeconds < 1 {
		complain("drain_timeout_seconds: %d must be >= 1", c.Stream.DrainTimeoutSeconds)
	}
	if c.Stream.ClassifyMaxAttemptBytes < 1 {
		complain("classify_max_attempt_bytes: %d must be >= 1", c.Stream.ClassifyMaxAttemptBytes)
	}
	if c.Stream.ClassifyMaxParticipantBytes < c.Stream.ClassifyMaxAttemptBytes {
		complain("classify_max_participant_bytes: %d must be >= classify_max_attempt_bytes %d", c.Stream.ClassifyMaxParticipantBytes, c.Stream.ClassifyMaxAttemptBytes)
	}
	if c.Stream.ClassifyMaxGlobalBytes < c.Stream.ClassifyMaxParticipantBytes {
		complain("classify_max_global_bytes: %d must be >= classify_max_participant_bytes %d", c.Stream.ClassifyMaxGlobalBytes, c.Stream.ClassifyMaxParticipantBytes)
	}

	if c.Perf.EWMAHalfLifeSeconds < 1 {
		complain("perf_ewma_half_life_seconds: %d must be >= 1", c.Perf.EWMAHalfLifeSeconds)
	}
	if c.Perf.ConsecutiveFailThreshold < 1 {
		complain("perf_consecutive_fail_threshold: %d must be >= 1", c.Perf.ConsecutiveFailThreshold)
	}
	if c.Perf.FailureRateThreshold <= 0 || c.Perf.FailureRateThreshold > 1 {
		complain("perf_failure_rate_threshold: %v must be in (0, 1]", c.Perf.FailureRateThreshold)
	}
	if c.Perf.FailureRateMinVolume < 1 {
		complain("perf_failure_rate_min_volume: %v must be >= 1", c.Perf.FailureRateMinVolume)
	}
	if c.Perf.EjectionBaseSeconds < 1 {
		complain("perf_ejection_base_seconds: %d must be >= 1", c.Perf.EjectionBaseSeconds)
	}
	if c.Perf.EjectionMaxSeconds < c.Perf.EjectionBaseSeconds {
		complain("perf_ejection_max_seconds: %d must be >= perf_ejection_base_seconds %d", c.Perf.EjectionMaxSeconds, c.Perf.EjectionBaseSeconds)
	}
	if c.Perf.MaxEjectionFraction <= 0 || c.Perf.MaxEjectionFraction > 1 {
		complain("perf_max_ejection_fraction: %v must be in (0, 1]", c.Perf.MaxEjectionFraction)
	}
	if c.Perf.MinAvailableHosts < 0 {
		complain("perf_min_available_hosts: %d must be >= 0", c.Perf.MinAvailableHosts)
	}
	if c.Perf.HostStalenessSeconds < 1 {
		complain("perf_host_staleness_seconds: %d must be >= 1", c.Perf.HostStalenessSeconds)
	}

	if c.Engine.ReceiptTimeoutMS < 1 {
		complain("engine_receipt_timeout_ms: %d must be >= 1", c.Engine.ReceiptTimeoutMS)
	}
	if c.Engine.FirstTokenFloorMS < 1 {
		complain("engine_first_token_floor_ms: %d must be >= 1", c.Engine.FirstTokenFloorMS)
	}
	if c.Engine.InterChunkStallMS < 1 {
		complain("engine_inter_chunk_stall_ms: %d must be >= 1", c.Engine.InterChunkStallMS)
	}
	// A loser is cancelled at the grace, so a grace under the stall window kills attempts that are
	// merely between chunks — before the gateway would even call such a stream stalled.
	if c.Engine.LoserGraceMS < c.Engine.InterChunkStallMS {
		complain("engine_loser_grace_ms: %d must be >= engine_inter_chunk_stall_ms %d", c.Engine.LoserGraceMS, c.Engine.InterChunkStallMS)
	}
	if c.Engine.NonStreamResponseFloorMS < 1 {
		complain("engine_non_stream_response_floor_ms: %d must be >= 1", c.Engine.NonStreamResponseFloorMS)
	}
	// A zero lag is the operator asking for the floor alone, which is a supported way to disable the
	// prompt-size term without disabling the fallback.
	if c.Engine.PerInputTokenResponseLagMS < 0 {
		complain("engine_per_input_token_response_lag_ms: %d must be >= 0", c.Engine.PerInputTokenResponseLagMS)
	}
	if c.Engine.MaxSpeculativeAttempts < 0 {
		complain("engine_max_speculative_attempts: %d must be >= 0 (0 = bounded only by the host group)", c.Engine.MaxSpeculativeAttempts)
	}

	// A long grace parks a committed-cost nonce on the chance of a co-arrival, so the ceiling is a
	// budget guard, not a taste judgement.
	if c.Scheduler.HoldGraceMS < 0 || c.Scheduler.HoldGraceMS > 5_000 {
		complain("scheduler_hold_grace_ms: %d must be in [0, 5000]", c.Scheduler.HoldGraceMS)
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %w", errors.Join(problems...))
	}
	return nil
}
