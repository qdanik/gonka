package engine

import (
	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/scheduler"
)

type hostMemory interface {
	Lookup(key affinity.Key, promptTokens uint64) (participant string, status affinity.Status)
	Record(key affinity.Key, participant string, promptTokens uint64)
}

func (c *raceCoordinator) affinityIntent() (target string, miss scheduler.AffinityException) {
	if c.deps.Affinity == nil || c.request.AffinityKey == affinity.NoKey {
		return scheduler.NoTarget, scheduler.AffinityExceptionNone
	}
	participant, status := c.deps.Affinity.Lookup(c.request.AffinityKey, c.request.InputTokens)
	switch status {
	case affinity.StatusHit:
		return participant, scheduler.AffinityExceptionNone
	case affinity.StatusExpired:
		return scheduler.NoTarget, scheduler.AffinityExceptionExpired
	case affinity.StatusPinAged:
		return scheduler.NoTarget, scheduler.AffinityExceptionPinAged
	case affinity.StatusShareCapped:
		return scheduler.NoTarget, scheduler.AffinityExceptionShareCapped
	case affinity.StatusBelowMinCached:
		return scheduler.NoTarget, scheduler.AffinityExceptionBelowMinCached
	}
	return scheduler.NoTarget, scheduler.AffinityExceptionNewKey
}

func (c *raceCoordinator) rememberWinner(participant string) {
	if c.deps.Affinity == nil || c.request.AffinityKey == affinity.NoKey {
		return
	}
	c.deps.Affinity.Record(c.request.AffinityKey, participant, c.request.InputTokens)
}
