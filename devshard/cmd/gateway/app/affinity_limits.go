package app

import (
	"time"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/config"
)

func affinityLimits(holder *config.Holder) func() affinity.Limits {
	return func() affinity.Limits {
		settings := holder.Load().Affinity
		return affinity.Limits{
			TTL:             time.Duration(settings.TTLMS) * time.Millisecond,
			MaxEntries:      int(settings.MaxEntries),
			MaxPinAge:       time.Duration(settings.MaxPinAgeMS) * time.Millisecond,
			ShareCap:        int(settings.ShareCap),
			MinCachedTokens: uint64(settings.MinCachedTokens),
		}
	}
}

func affinityMaxBurns(holder *config.Holder) func() int64 {
	return func() int64 { return holder.Load().Affinity.MaxBurns }
}
