package limits

import (
	"time"

	"devshard/cmd/gateway/config"
)

func GatewayConfigFromLimits(l config.Limits) GatewayConfig {
	modelLimits := make(map[string]ModelOverride, len(l.ModelLimits))
	for model, modelLimit := range l.ModelLimits {
		modelLimits[model] = ModelOverride{
			MaxConcurrent:  modelLimit.MaxConcurrentRequests,
			MaxInputTokens: modelLimit.MaxInputTokensInFlight,
		}
	}
	return GatewayConfig{
		MaxConcurrent:         l.Concurrency.MaxRequests,
		MaxInputTokens:        l.MaxInputTokensInFlight,
		AdmissionQueuePerSlot: l.AdmissionQueuePerSlot,
		AcquireWait:           time.Duration(l.AdmissionQueueWaitMS) * time.Millisecond,
		ModelLimits:           modelLimits,
	}
}

func ParticipantConfigFromLimits(l config.Limits) ParticipantConfig {
	return ParticipantConfig{
		Initial:       l.HostInflight.Initial,
		Max:           l.HostInflight.Max,
		AfterFailures: l.HostCutoff.AfterFailures,
		BaseOpen:      time.Duration(l.HostCutoff.BaseMS) * time.Millisecond,
		MaxOpen:       time.Duration(l.HostCutoff.MaxMS) * time.Millisecond,
	}
}
