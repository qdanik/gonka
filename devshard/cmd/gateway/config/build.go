package config

import (
	"maps"
	"strings"

	"devshard/cmd/gateway/env"
)

// Build produces the immutable snapshot: Defaults() ← env values ← admin
// overrides, validated as a whole. The returned *Config must never be
// mutated. Every map/slice taken from values/overrides is cloned into the
// snapshot (see the no-aliasing contract in config.go) so a caller mutating
// its own map/slice after Build cannot reach the published snapshot.
func Build(values env.Values, overrides Overrides) (*Config, error) {
	configuration := Defaults()

	applyInt := func(target *int64, source *int64) {
		if source != nil {
			*target = *source
		}
	}
	applyFloat := func(target *float64, source *float64) {
		if source != nil {
			*target = *source
		}
	}
	applyString := func(target *string, source *string) {
		if source != nil {
			*target = *source
		}
	}
	applyBool := func(target *bool, source *bool) {
		if source != nil {
			*target = *source
		}
	}

	// Environment layer.
	applyInt(&configuration.Server.Port, values.Port)
	applyString(&configuration.Server.StorageDir, values.StorageDir)
	if values.APIKeys != nil {
		configuration.Server.APIKeys = splitCommaSeparated(*values.APIKeys)
	}
	applyString(&configuration.Server.AdminAPIKey, values.AdminAPIKey)
	applyString(&configuration.Server.DevshardsJSON, values.DevshardsJSON)

	applyString(&configuration.Chain.RESTBaseURL, values.ChainREST)
	applyString(&configuration.Chain.PublicAPIBaseURL, values.PublicAPI)
	if values.TxQueryFallbackURLs != nil {
		configuration.Chain.TxQueryFallbackURLs = splitCommaSeparated(*values.TxQueryFallbackURLs)
	}
	applyInt(&configuration.Tx.FeeAmount, values.TxFeeAmount)
	applyInt(&configuration.Tx.GasLimit, values.TxGasLimit)

	applyInt(&configuration.Limits.DefaultMaxTokens, values.DefaultMaxTokens)
	applyInt(&configuration.Limits.MaxTokensCap, values.MaxTokensCap)
	applyInt(&configuration.Limits.Concurrency.MaxRequests, values.MaxConcurrentRequests)

	applyString(&configuration.Modes.PoCMode, values.PoCMode)
	applyBool(&configuration.Modes.CapacityAwareLimits, values.CapacityAwareLimits)
	applyBool(&configuration.Modes.Disabled, values.Disabled)
	applyString(&configuration.Modes.DisabledMessage, values.DisabledMessage)
	applyString(&configuration.Modes.DisabledRedirectURL, values.DisabledRedirectURL)

	applyBool(&configuration.Rotation.Enabled, values.RotationEnabled)
	applyBool(&configuration.Rotation.SettlementEnabled, values.RotationSettlementEnabled)
	applyString(&configuration.Rotation.ModelsJSON, values.RotationModelsJSON)

	applyInt(&configuration.Cache.ChatCacheMaxBytes, values.ChatCacheMaxBytes)

	applyBool(&configuration.Capture.Enabled, values.CaptureEnabled)
	applyString(&configuration.Capture.Dir, values.CaptureDir)

	// Admin-override layer (wins over env).
	applyInt(&configuration.Limits.DefaultMaxTokens, overrides.DefaultMaxTokens)
	applyInt(&configuration.Limits.MaxTokensCap, overrides.MaxTokensCap)
	applyInt(&configuration.Limits.Concurrency.MaxRequests, overrides.MaxConcurrentRequests)
	applyFloat(&configuration.Limits.Concurrency.RequestsPer10000Weight, overrides.MaxConcurrentRequestsPer10000Weight)
	applyFloat(&configuration.Limits.Concurrency.PoCRequestsPer10000Weight, overrides.PoCMaxConcurrentRequestsPer10000Weight)
	applyInt(&configuration.Limits.MaxInputTokensInFlight, overrides.MaxInputTokensInFlight)
	applyInt(&configuration.Limits.AcquireWaitMS, overrides.AcquireWaitMS)
	applyInt(&configuration.Limits.AIMD.InitialWindow, overrides.AIMDInitialWindow)
	applyInt(&configuration.Limits.AIMD.MaxWindow, overrides.AIMDMaxWindow)
	applyInt(&configuration.Limits.Breaker.TripThreshold, overrides.BreakerTripThreshold)
	applyInt(&configuration.Limits.Breaker.BaseOpenMS, overrides.BreakerBaseOpenMS)
	applyInt(&configuration.Limits.Breaker.MaxOpenMS, overrides.BreakerMaxOpenMS)
	if overrides.ModelLimits != nil {
		configuration.Limits.ModelLimits = maps.Clone(overrides.ModelLimits)
	}
	if overrides.ModelAccess != nil {
		configuration.Limits.ModelAccess = maps.Clone(overrides.ModelAccess)
	}
	applyBool(&configuration.Modes.Disabled, overrides.Disabled)
	applyString(&configuration.Modes.DisabledMessage, overrides.DisabledMessage)
	applyString(&configuration.Modes.DisabledRedirectURL, overrides.DisabledRedirectURL)
	applyBool(&configuration.Rotation.Enabled, overrides.RotationEnabled)
	applyBool(&configuration.Rotation.SettlementEnabled, overrides.RotationSettlementEnabled)
	applyInt(&configuration.Rotation.PrePoCBlocks, overrides.RotationPrePoCBlocks)
	applyString(&configuration.Rotation.ModelsJSON, overrides.RotationModelsJSON)

	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	return &configuration, nil
}

// splitCommaSeparated trims and drops empty elements, so it always returns a
// freshly allocated slice — callers do not need a further clone.
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
