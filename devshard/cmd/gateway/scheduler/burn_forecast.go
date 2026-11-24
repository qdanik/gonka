package scheduler

import (
	"fmt"
	"sync"

	"devshard/cmd/gateway/chain"
)

type burnLimits struct {
	consecutive int64
	perRequest  int64
}

func (limits burnLimits) refuses(tally *BurnTally, forecast burnForecast) bool {
	return forecast.usable && tally.WouldExceed(limits.perRequest, forecast.charged)
}

func (s *Scheduler) burnLimits(model string) burnLimits {
	if s.settings == nil {
		return burnLimits{}
	}
	return burnLimits{consecutive: s.maxConsecutiveBurns(model), perRequest: s.maxBurnsPerRequest(model)}
}

func (s *Scheduler) checkPinnedBurnCap(profile RequestProfile, snapshot chain.PhaseSnapshot, pinned Escrow, queued *waiter) (Escrow, error) {
	limits := s.burnLimits(profile.Model)
	if profile.Burns == nil || limits.perRequest <= BurnCapOff {
		return pinned, nil
	}
	forecast := s.burnsToServe(pinned, s.fleetGates(profile.Model, snapshot), queued, s.queuedAheadOf(pinned), limits.consecutive)
	if limits.refuses(profile.Burns, forecast) {
		return Escrow{}, refuseCappedAtPick(fmt.Sprintf("escrow %s", pinned.ID), limits.perRequest)
	}
	return pinned, nil
}

func refuseCappedAtPick(subject string, limit int64) error {
	return fmt.Errorf("%s: the slot that can take the request is more burns away than it has left of its cap of %d burned nonces: %w", subject, limit, ErrRequestCapped)
}

type burnForecast struct {
	burns   int
	charged int
	usable  bool
}

func (s *Scheduler) burnsToServe(candidate Escrow, fleet availability, queued *waiter, queuedAhead uint64, forceAfter int64) burnForecast {
	return expectedBurns(candidate, s.escrowGates(candidate, fleet), queued, queuedAhead, forceAfter)
}

func (s *Scheduler) escrowGates(candidate Escrow, fleet availability) availability {
	return fleet.forEscrow(s.stateBlocked(candidate.ID), s.absentFromChain(candidate.ID), chatBudgetSpent(candidate))
}

func expectedBurns(candidate Escrow, gates availability, queued *waiter, queuedAhead uint64, forceAfter int64) burnForecast {
	if candidate.Session == nil {
		return burnForecast{}
	}
	slots := candidate.Session.SlotParticipants()
	if len(slots) == 0 {
		return burnForecast{}
	}
	cursor := int((candidate.Session.LatestNonce() + 1 + queuedAhead) % uint64(len(slots)))
	var forecast burnForecast
	reachable := sync.OnceValue(func() bool {
		canServe, _, _, _ := servable(queued, slots, gates)
		return canServe
	})
	for step := range slots {
		slot := slots[(cursor+step)%len(slots)]
		if gates.blocks(slot, queued) == blockNone {
			forecast.usable = true
			return forecast
		}
		if forceAfter > forcedSendOff && int64(forecast.burns) >= forceAfter && gates.servingOverFullWindows().blocks(slot, queued) == blockNone && reachable() {
			forecast.usable = true
			return forecast
		}
		forecast.burns++
		if !queued.exclude[slot] && !gates.shadowMember(slot) {
			forecast.charged++
		}
	}
	return forecast
}
