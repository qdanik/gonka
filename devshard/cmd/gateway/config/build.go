package config

import (
	"maps"
	"slices"
	"strings"

	"devshard/cmd/gateway/env"
)

// Build produces the validated snapshot: Defaults() ← env ← admin overrides, cloning every input map and slice.
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

	overrideIfSet(&configuration.Chain.GRPCEndpoint, values.ChainGRPC)
	overrideIfSet(&configuration.Chain.PublicAPIBaseURL, values.PublicAPI)
	overrideIfSet(&configuration.Chain.ChainID, values.ChainID)
	overrideIfSet(&configuration.Chain.RPCEndpoint, values.ChainRPC)
	overrideIfSet(&configuration.Chain.SnapshotMaxAgeSeconds, values.ChainSnapshotMaxAgeSeconds)
	overrideIfSet(&configuration.Tx.FeeAmount, values.TxFeeAmount)
	overrideIfSet(&configuration.Tx.GasLimit, values.TxGasLimit)

	overrideIfSet(&configuration.Limits.DefaultMaxTokens, values.DefaultMaxTokens)
	overrideIfSet(&configuration.Limits.MaxTokensCap, values.MaxTokensCap)
	overrideIfSet(&configuration.Limits.ForceUpstreamStreaming, values.ForceUpstreamStreaming)
	overrideIfSet(&configuration.Limits.MaxBufferedResponseBytes, values.MaxBufferedResponseBytes)
	overrideIfSet(&configuration.Limits.Concurrency.MaxRequests, values.MaxConcurrentRequests)
	overrideIfSet(&configuration.Limits.AdmissionQueueWaitMS, values.AdmissionQueueWaitMS)
	overrideIfSet(&configuration.Limits.AdmissionQueuePerSlot, values.AdmissionQueuePerSlot)
	overrideIfSet(&configuration.Scheduler.MatchWaitMS, values.MatchWaitMS)
	overrideIfSet(&configuration.Scheduler.WarmNewEscrows, values.WarmNewEscrows)

	overrideIfSet(&configuration.Modes.PoCMode, values.PoCMode)
	overrideIfSet(&configuration.Modes.Disabled, values.Disabled)
	overrideIfSet(&configuration.Modes.DisabledMessage, values.DisabledMessage)
	overrideIfSet(&configuration.Modes.DisabledRedirectURL, values.DisabledRedirectURL)

	overrideIfSet(&configuration.Rotation.Enabled, values.RotationEnabled)
	overrideIfSet(&configuration.Rotation.PrePoCBlocks, values.RotationPrePoCBlocks)
	overrideIfSet(&configuration.Rotation.SettlementEnabled, values.RotationSettlementEnabled)
	overrideIfSet(&configuration.Rotation.ModelsJSON, values.RotationModelsJSON)

	overrideIfSet(&configuration.Cache.ChatCacheMaxBytes, values.ChatCacheMaxBytes)

	overrideIfSet(&configuration.Accounting.RetentionHours, values.AccountingRetentionHours)
	overrideIfSet(&configuration.Accounting.RetentionMaxRows, values.AccountingRetentionMaxRows)

	overrideIfSet(&configuration.NonceAccounting.Enabled, values.NonceAccountingEnabled)
	overrideIfSet(&configuration.NonceAccounting.ListenAddr, values.NonceAccountingListenAddr)
	overrideIfSet(&configuration.NonceAccounting.RetentionEpochs, values.NonceAccountingRetentionEpochs)
	overrideIfSet(&configuration.NonceAccounting.SnapshotSeconds, values.NonceAccountingSnapshotSeconds)

	overrideIfSet(&configuration.Capture.Enabled, values.CaptureEnabled)
	overrideIfSet(&configuration.Capture.Dir, values.CaptureDir)
	overrideIfSet(&configuration.Capture.SampleRate, values.CaptureSampleRate)
	overrideIfSet(&configuration.Capture.MaxBytes, values.CaptureMaxBytes)

	overrideIfSet(&configuration.Perf.EWMAHalfLifeSeconds, values.PerfEWMAHalfLifeSeconds)
	overrideIfSet(&configuration.Perf.ConsecutiveFailThreshold, values.PerfConsecutiveFailThreshold)
	overrideIfSet(&configuration.Perf.FailureRateThreshold, values.PerfFailureRateThreshold)
	overrideIfSet(&configuration.Perf.FailureRateMinVolume, values.PerfFailureRateMinVolume)
	overrideIfSet(&configuration.Perf.EjectionBaseSeconds, values.PerfEjectionBaseSeconds)
	overrideIfSet(&configuration.Perf.EjectionMaxSeconds, values.PerfEjectionMaxSeconds)
	overrideIfSet(&configuration.Perf.MaxEjectionFraction, values.PerfMaxEjectionFraction)
	overrideIfSet(&configuration.Perf.MinAvailableHosts, values.PerfMinAvailableHosts)
	overrideIfSet(&configuration.Perf.HostStalenessSeconds, values.PerfHostStalenessSeconds)

	overrideIfSet(&configuration.Engine.ReceiptTimeoutMS, values.EngineReceiptTimeoutMS)
	overrideIfSet(&configuration.Engine.FirstTokenFloorMS, values.EngineFirstTokenFloorMS)
	overrideIfSet(&configuration.Engine.FirstTokenCeilingMS, values.EngineFirstTokenCeilingMS)
	overrideIfSet(&configuration.Engine.InterChunkStallMS, values.EngineInterChunkStallMS)
	overrideIfSet(&configuration.Engine.LoserGraceMS, values.EngineLoserGraceMS)

	// Admin-override layer (wins over env).
	overrideIfSet(&configuration.Limits.DefaultMaxTokens, overrides.DefaultMaxTokens)
	overrideIfSet(&configuration.Limits.MaxTokensCap, overrides.MaxTokensCap)
	overrideIfSet(&configuration.Limits.Concurrency.MaxRequests, overrides.MaxConcurrentRequests)
	overrideIfSet(&configuration.Limits.Concurrency.RequestsPer10000Weight, overrides.MaxConcurrentRequestsPer10000Weight)
	overrideIfSet(&configuration.Limits.Concurrency.PoCRequestsPer10000Weight, overrides.PoCMaxConcurrentRequestsPer10000Weight)
	overrideIfSet(&configuration.Limits.MaxInputTokensInFlight, overrides.MaxInputTokensInFlight)
	overrideIfSet(&configuration.Limits.AdmissionQueueWaitMS, overrides.AdmissionQueueWaitMS)
	overrideIfSet(&configuration.Limits.AdmissionQueuePerSlot, overrides.AdmissionQueuePerSlot)
	overrideIfSet(&configuration.Scheduler.MatchWaitMS, overrides.MatchWaitMS)
	overrideIfSet(&configuration.Limits.ForceUpstreamStreaming, overrides.ForceUpstreamStreaming)
	overrideIfSet(&configuration.Limits.MaxBufferedResponseBytes, overrides.MaxBufferedResponseBytes)
	overrideIfSet(&configuration.Scheduler.WarmNewEscrows, overrides.WarmNewEscrows)
	overrideIfSet(&configuration.Chain.SnapshotMaxAgeSeconds, overrides.ChainSnapshotMaxAgeSeconds)
	overrideIfSet(&configuration.Engine.ReceiptTimeoutMS, overrides.EngineReceiptTimeoutMS)
	overrideIfSet(&configuration.Engine.FirstTokenFloorMS, overrides.EngineFirstTokenFloorMS)
	overrideIfSet(&configuration.Engine.FirstTokenCeilingMS, overrides.EngineFirstTokenCeilingMS)
	overrideIfSet(&configuration.Engine.InterChunkStallMS, overrides.EngineInterChunkStallMS)
	overrideIfSet(&configuration.Engine.LoserGraceMS, overrides.EngineLoserGraceMS)
	overrideIfSet(&configuration.Perf.EWMAHalfLifeSeconds, overrides.PerfEWMAHalfLifeSeconds)
	overrideIfSet(&configuration.Perf.ConsecutiveFailThreshold, overrides.PerfConsecutiveFailThreshold)
	overrideIfSet(&configuration.Perf.FailureRateThreshold, overrides.PerfFailureRateThreshold)
	overrideIfSet(&configuration.Perf.FailureRateMinVolume, overrides.PerfFailureRateMinVolume)
	overrideIfSet(&configuration.Perf.EjectionBaseSeconds, overrides.PerfEjectionBaseSeconds)
	overrideIfSet(&configuration.Perf.EjectionMaxSeconds, overrides.PerfEjectionMaxSeconds)
	overrideIfSet(&configuration.Perf.MaxEjectionFraction, overrides.PerfMaxEjectionFraction)
	overrideIfSet(&configuration.Perf.MinAvailableHosts, overrides.PerfMinAvailableHosts)
	overrideIfSet(&configuration.Perf.HostStalenessSeconds, overrides.PerfHostStalenessSeconds)
	if overrides.ParticipantAllowlist != nil {
		configuration.Scheduler.ParticipantAllowlist = slices.Clone(*overrides.ParticipantAllowlist)
	}
	overrideIfSet(&configuration.Limits.HostInflight.Initial, overrides.HostInitialInflight)
	overrideIfSet(&configuration.Limits.HostInflight.Max, overrides.HostMaxInflight)
	overrideIfSet(&configuration.Limits.HostCutoff.AfterFailures, overrides.HostCutoffAfterFailures)
	overrideIfSet(&configuration.Limits.HostCutoff.BaseMS, overrides.HostCutoffMS)
	overrideIfSet(&configuration.Limits.HostCutoff.MaxMS, overrides.HostCutoffMaxMS)
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
