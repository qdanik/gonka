package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
)

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
	checkBaseURL("public_api", c.Chain.PublicAPIBaseURL)
	// A gRPC target is host:port with no scheme, so the URL check above would pass anything.
	if _, _, err := net.SplitHostPort(c.Chain.GRPCEndpoint); err != nil {
		complain("chain_grpc: %q is not host:port", c.Chain.GRPCEndpoint)
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
	if c.Engine.FirstTokenCeilingMS < c.Engine.FirstTokenFloorMS {
		complain("engine_first_token_ceiling_ms: %d must be >= engine_first_token_floor_ms %d",
			c.Engine.FirstTokenCeilingMS, c.Engine.FirstTokenFloorMS)
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
