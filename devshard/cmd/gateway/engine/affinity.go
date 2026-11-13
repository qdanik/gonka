package engine

import (
	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/scheduler"
)

type hostMemory interface {
	Lookup(key affinity.Key, promptTokens uint64) (participant string, status affinity.Status)
	Record(key affinity.Key, participant string, promptTokens uint64)
}

func (c *raceCoordinator) affinityIntent() (target string, miss affinity.Exception) {
	if c.deps.Affinity == nil || c.request.AffinityKey == affinity.NoKey {
		return scheduler.NoTarget, affinity.ExceptionNone
	}
	participant, status := c.deps.Affinity.Lookup(c.request.AffinityKey, c.request.InputTokens)
	switch status {
	case affinity.StatusHit:
		return participant, affinity.ExceptionNone
	case affinity.StatusExpired:
		return scheduler.NoTarget, affinity.ExceptionExpired
	case affinity.StatusPinAged:
		return scheduler.NoTarget, affinity.ExceptionPinAged
	case affinity.StatusShareCapped:
		return scheduler.NoTarget, affinity.ExceptionShareCapped
	case affinity.StatusBelowMinCached:
		return scheduler.NoTarget, affinity.ExceptionBelowMinCached
	}
	return scheduler.NoTarget, affinity.ExceptionNewKey
}

func (c *raceCoordinator) rememberWinner(participant string) {
	if c.deps.Affinity == nil || c.request.AffinityKey == affinity.NoKey {
		return
	}
	c.deps.Affinity.Record(c.request.AffinityKey, participant, c.request.InputTokens)
}
