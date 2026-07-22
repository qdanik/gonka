package limits

import (
	"time"

	"devshard/cmd/gateway/config"
)

func GatewayConfigFromLimits(l config.Limits) GatewayConfig {
	modelLimits := make(map[string]ModelOverride, len(l.ModelLimits))
	for model, modelLimit := range l.ModelLimits { // DefaultMaxTokens/MaxTokensCap are filter concerns, not limiter ones
		modelLimits[model] = ModelOverride{
			MaxConcurrent:  modelLimit.MaxConcurrentRequests,
			MaxInputTokens: modelLimit.MaxInputTokensInFlight,
		}
	}
	return GatewayConfig{
		MaxConcurrent:  l.Concurrency.MaxRequests,
		MaxInputTokens: l.MaxInputTokensInFlight,
		AcquireWait:    time.Duration(l.AcquireWaitMS) * time.Millisecond,
		ModelLimits:    modelLimits,
	}
}

func ParticipantConfigFromLimits(l config.Limits) ParticipantConfig {
	return ParticipantConfig{
		InitialWindow: l.AIMD.InitialWindow,
		MaxWindow:     l.AIMD.MaxWindow,
		TripThreshold: l.Breaker.TripThreshold,
		BaseOpen:      time.Duration(l.Breaker.BaseOpenMS) * time.Millisecond,
		MaxOpen:       time.Duration(l.Breaker.MaxOpenMS) * time.Millisecond,
	}
}
