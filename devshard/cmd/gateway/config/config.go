// Package config owns the gateway's immutable configuration snapshot:
// defaults ← environment ← admin overrides. A *Config is never mutated after
// Build; hot reconfiguration swaps the whole snapshot (see Holder). Maps and
// slices inside Config are part of that contract: treat them as read-only,
// and Build must clone every map/slice it takes from its inputs so no caller
// can mutate a published snapshot through a retained reference.
package config

import (
	"errors"
	"fmt"
	"net/url"
)

// Server groups process-level settings.
type Server struct {
	Port                       int64
	StorageDir                 string // always the resolved absolute path; main resolves it before Build
	APIKeys                    []string
	AdminAPIKey                string
	DevshardsJSON              string
	MaxConcurrentRuntimeBuilds int64
}

// Chain groups chain-facing endpoints.
type Chain struct {
	RESTBaseURL         string
	PublicAPIBaseURL    string
	TxQueryFallbackURLs []string
}

// Tx groups transaction parameters.
type Tx struct {
	FeeDenom       string
	FeeAmount      int64
	GasLimit       int64
	PollIntervalMS int64
	PollTimeoutMS  int64
}

// Concurrency bounds in-flight requests: absolute cap + weight-scaled allowance.
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

// Breaker is the participant circuit-breaker ladder tuning.
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

// Limits groups admission and participant-protection tuning.
type Limits struct {
	DefaultMaxTokens       int64
	MaxTokensCap           int64
	Concurrency            Concurrency
	MaxInputTokensInFlight int64 // 0 = unlimited
	AcquireWaitMS          int64
	AIMD                   AIMD
	Breaker                Breaker
	ModelLimits            map[string]ModelLimits
	ModelAccess            map[string]string // model → open|api_key|admin_only
}

// Modes groups operational switches.
type Modes struct {
	PoCMode             string // off|relaxed
	CapacityAwareLimits bool
	Disabled            bool
	DisabledMessage     string
	DisabledRedirectURL string
}

// Rotation groups escrow-rotation settings.
type Rotation struct {
	Enabled           bool
	SettlementEnabled bool
	PrePoCBlocks      int64
	ModelsJSON        string
}

// Cache groups response-cache settings.
type Cache struct {
	ChatCacheMaxBytes int64
}

// Capture groups the debug request-capture settings.
type Capture struct {
	Enabled                      bool
	Dir                          string // empty = <storageDir>/captured-requests
	ShortContentAttempts         bool
	ShortContentResponses        bool
	ShortContentMinOutputChunks  int64
	ShortContentMaxContentRatio  float64
	ShortContentResponseMaxBytes int64
}

// Stream groups streaming/classification bounds.
type Stream struct {
	DrainTimeoutSeconds         int64
	ClassifyMaxAttemptBytes     int64
	ClassifyMaxParticipantBytes int64
	ClassifyMaxGlobalBytes      int64
}

// Config is the complete immutable gateway configuration snapshot.
type Config struct {
	Server   Server
	Chain    Chain
	Tx       Tx
	Limits   Limits
	Modes    Modes
	Rotation Rotation
	Cache    Cache
	Capture  Capture
	Stream   Stream
}

// PoC mode values accepted in Modes.PoCMode.
const (
	PoCModeOff     = "off"
	PoCModeRelaxed = "relaxed"
)

// Model access values accepted in Limits.ModelAccess.
const (
	ModelAccessOpen      = "open"
	ModelAccessAPIKey    = "api_key"
	ModelAccessAdminOnly = "admin_only"
)

// Defaults returns the default configuration.
func Defaults() Config {
	return Config{
		Server: Server{
			Port:                       8080,
			MaxConcurrentRuntimeBuilds: 16,
		},
		Chain: Chain{
			RESTBaseURL:         "http://localhost:1317",
			PublicAPIBaseURL:    "http://localhost:9000",
			TxQueryFallbackURLs: []string{"http://node1.gonka.ai:8000/chain-api"},
		},
		Tx: Tx{
			FeeDenom:       "ngonka",
			FeeAmount:      1_000_000,
			GasLimit:       500_000,
			PollIntervalMS: 2_000,
			PollTimeoutMS:  45_000,
		},
		Limits: Limits{
			DefaultMaxTokens: 3072,
			MaxTokensCap:     4096,
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
			ChatCacheMaxBytes: 268_435_456,
		},
		Capture: Capture{
			ShortContentMinOutputChunks:  1_000,
			ShortContentMaxContentRatio:  0.75,
			ShortContentResponseMaxBytes: 16_777_216,
		},
		Stream: Stream{
			DrainTimeoutSeconds:         2_400,
			ClassifyMaxAttemptBytes:     1_048_576,
			ClassifyMaxParticipantBytes: 10_485_760,
			ClassifyMaxGlobalBytes:      104_857_600,
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
	if c.Limits.MaxTokensCap < c.Limits.DefaultMaxTokens {
		complain("max_tokens_cap: %d must be >= default_max_tokens %d", c.Limits.MaxTokensCap, c.Limits.DefaultMaxTokens)
	}
	if c.Limits.Concurrency.MaxRequests < 1 {
		complain("max_concurrent_requests: %d must be >= 1", c.Limits.Concurrency.MaxRequests)
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
	for model, access := range c.Limits.ModelAccess {
		if access != ModelAccessOpen && access != ModelAccessAPIKey && access != ModelAccessAdminOnly {
			complain("model_access[%s]: %q is not open, api_key or admin_only", model, access)
		}
	}
	for model, limit := range c.Limits.ModelLimits {
		if limit.DefaultMaxTokens < 1 || limit.MaxTokensCap < limit.DefaultMaxTokens {
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
	if c.Capture.ShortContentMinOutputChunks < 0 {
		complain("capture_short_content_min_output_chunks: %d must be >= 0", c.Capture.ShortContentMinOutputChunks)
	}
	if c.Capture.ShortContentMaxContentRatio <= 0 || c.Capture.ShortContentMaxContentRatio > 1 {
		complain("capture_short_content_max_content_ratio: %v must be in (0, 1]", c.Capture.ShortContentMaxContentRatio)
	}
	if c.Capture.ShortContentResponseMaxBytes < 1 {
		complain("capture_short_content_response_max_bytes: %d must be >= 1", c.Capture.ShortContentResponseMaxBytes)
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

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %w", errors.Join(problems...))
	}
	return nil
}
