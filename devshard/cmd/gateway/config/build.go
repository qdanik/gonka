package config

import (
	"maps"
	"strings"

	"devshard/cmd/gateway/env"
)

// Build produces the validated snapshot: Defaults() ← env ← admin overrides.
// Maps/slices from inputs are cloned (no-aliasing contract — see package doc).
func Build(values env.Values, overrides Overrides) (*Config, error) {
	configuration := Defaults()

	// Environment layer.
	overrideIfSet(&configuration.Server.Port, values.Port)
	overrideIfSet(&configuration.Server.StorageDir, values.StorageDir)
	if values.APIKeys != nil {
		configuration.Server.APIKeys = splitCommaSeparated(*values.APIKeys)
	}
	overrideIfSet(&configuration.Server.AdminAPIKey, values.AdminAPIKey)
	overrideIfSet(&configuration.Server.DevshardsJSON, values.DevshardsJSON)

	overrideIfSet(&configuration.Chain.RESTBaseURL, values.ChainREST)
	overrideIfSet(&configuration.Chain.GRPCEndpoint, values.ChainGRPC)
	overrideIfSet(&configuration.Chain.PublicAPIBaseURL, values.PublicAPI)
	if values.TxQueryFallbackURLs != nil {
		configuration.Chain.TxQueryFallbackURLs = splitCommaSeparated(*values.TxQueryFallbackURLs)
	}
	overrideIfSet(&configuration.Tx.FeeAmount, values.TxFeeAmount)
	overrideIfSet(&configuration.Tx.GasLimit, values.TxGasLimit)

	overrideIfSet(&configuration.Limits.DefaultMaxTokens, values.DefaultMaxTokens)
	overrideIfSet(&configuration.Limits.MaxTokensCap, values.MaxTokensCap)
	overrideIfSet(&configuration.Limits.Concurrency.MaxRequests, values.MaxConcurrentRequests)

	overrideIfSet(&configuration.Modes.PoCMode, values.PoCMode)
	overrideIfSet(&configuration.Modes.Disabled, values.Disabled)
	overrideIfSet(&configuration.Modes.DisabledMessage, values.DisabledMessage)
	overrideIfSet(&configuration.Modes.DisabledRedirectURL, values.DisabledRedirectURL)

	overrideIfSet(&configuration.Rotation.Enabled, values.RotationEnabled)
	overrideIfSet(&configuration.Rotation.SettlementEnabled, values.RotationSettlementEnabled)
	overrideIfSet(&configuration.Rotation.ModelsJSON, values.RotationModelsJSON)

	overrideIfSet(&configuration.Cache.ChatCacheMaxBytes, values.ChatCacheMaxBytes)

	overrideIfSet(&configuration.Accounting.RetentionHours, values.AccountingRetentionHours)
	overrideIfSet(&configuration.Accounting.RetentionMaxRows, values.AccountingRetentionMaxRows)

	overrideIfSet(&configuration.Capture.Enabled, values.CaptureEnabled)
	overrideIfSet(&configuration.Capture.Dir, values.CaptureDir)
	overrideIfSet(&configuration.Capture.SampleRate, values.CaptureSampleRate)
	overrideIfSet(&configuration.Capture.MaxBytes, values.CaptureMaxBytes)

	overrideIfSet(&configuration.Perf.EWMAHalfLifeSeconds, values.PerfEWMAHalfLifeSeconds)

	// Admin-override layer (wins over env).
	overrideIfSet(&configuration.Limits.DefaultMaxTokens, overrides.DefaultMaxTokens)
	overrideIfSet(&configuration.Limits.MaxTokensCap, overrides.MaxTokensCap)
	overrideIfSet(&configuration.Limits.Concurrency.MaxRequests, overrides.MaxConcurrentRequests)
	overrideIfSet(&configuration.Limits.Concurrency.RequestsPer10000Weight, overrides.MaxConcurrentRequestsPer10000Weight)
	overrideIfSet(&configuration.Limits.Concurrency.PoCRequestsPer10000Weight, overrides.PoCMaxConcurrentRequestsPer10000Weight)
	overrideIfSet(&configuration.Limits.MaxInputTokensInFlight, overrides.MaxInputTokensInFlight)
	overrideIfSet(&configuration.Limits.AcquireWaitMS, overrides.AcquireWaitMS)
	overrideIfSet(&configuration.Limits.AIMD.InitialWindow, overrides.AIMDInitialWindow)
	overrideIfSet(&configuration.Limits.AIMD.MaxWindow, overrides.AIMDMaxWindow)
	overrideIfSet(&configuration.Limits.Breaker.TripThreshold, overrides.BreakerTripThreshold)
	overrideIfSet(&configuration.Limits.Breaker.BaseOpenMS, overrides.BreakerBaseOpenMS)
	overrideIfSet(&configuration.Limits.Breaker.MaxOpenMS, overrides.BreakerMaxOpenMS)
	if overrides.ModelLimits != nil {
		configuration.Limits.ModelLimits = maps.Clone(overrides.ModelLimits)
	}
	if overrides.ModelAccess != nil {
		configuration.Limits.ModelAccess = maps.Clone(overrides.ModelAccess)
	}
	overrideIfSet(&configuration.Modes.Disabled, overrides.Disabled)
	overrideIfSet(&configuration.Modes.DisabledMessage, overrides.DisabledMessage)
	overrideIfSet(&configuration.Modes.DisabledRedirectURL, overrides.DisabledRedirectURL)
	overrideIfSet(&configuration.Rotation.Enabled, overrides.RotationEnabled)
	overrideIfSet(&configuration.Rotation.SettlementEnabled, overrides.RotationSettlementEnabled)
	overrideIfSet(&configuration.Rotation.PrePoCBlocks, overrides.RotationPrePoCBlocks)
	overrideIfSet(&configuration.Rotation.ModelsJSON, overrides.RotationModelsJSON)

	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	return &configuration, nil
}

// overrideIfSet: a nil source leaves the lower layer's value in place.
func overrideIfSet[T any](target, source *T) {
	if source != nil {
		*target = *source
	}
}

// splitCommaSeparated trims, drops empties, and always returns a fresh slice (no clone needed).
func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}
