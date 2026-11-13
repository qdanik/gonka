package app

import (
	"time"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/config"
)

func affinityLimits(holder *config.Holder) func() affinity.Limits {
	return func() affinity.Limits {
		scheduling := holder.Load().Scheduler
		return affinity.Limits{
			TTL:             time.Duration(scheduling.AffinityTTLMS) * time.Millisecond,
			MaxEntries:      int(scheduling.AffinityMaxEntries),
			MaxPinAge:       time.Duration(scheduling.AffinityMaxPinAgeMS) * time.Millisecond,
			ShareCap:        int(scheduling.AffinityShareCap),
			MinCachedTokens: uint64(scheduling.AffinityMinCachedTokens),
		}
	}
}
