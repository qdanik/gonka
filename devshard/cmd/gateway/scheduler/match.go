package scheduler

import (
	"time"
)

// availability is a frozen per-drain snapshot of the host predicates. See routing.md, "The drain".
type availability struct {
	pocRequired  func(participant string) bool
	throttled    func(participant string) bool
	ejected      func(participant string) bool
	notAllowed   func(participant string) bool
	stateBlocked func(participant string) bool
}

type blockReason int

const (
	blockNone blockReason = iota
	blockPoCRequired
	blockThrottled
	blockEjected
	blockNotAllowed
	blockExcluded
	blockStateDiverged
)

// ghostFor names the burn each participant-only block earns, as one mapping rather than two switches.
var ghostFor = map[blockReason]GhostKind{
	blockPoCRequired:   ghostPoC,
	blockThrottled:     ghostThrottled,
	blockEjected:       ghostEjected,
	blockNotAllowed:    ghostNotAllowed,
	blockStateDiverged: ghostStateDiverged,
}

// A missing predicate reads as no block, so an availability built without one does not narrow dispatch.
func (a availability) divergedFromEscrowState(participant string) bool {
	return a.stateBlocked != nil && a.stateBlocked(participant)
}

func (a availability) outsideAllowlist(participant string) bool {
	return a.notAllowed != nil && a.notAllowed(participant)
}

// participantBlocked is the half of blocks that needs no waiter, so match and blocks share one ladder.
func (a availability) participantBlocked(participant string) blockReason {
	switch {
	case a.outsideAllowlist(participant):
		return blockNotAllowed
	case a.pocRequired(participant):
		return blockPoCRequired
	case a.throttled(participant):
		return blockThrottled
	case a.ejected(participant):
		return blockEjected
	case a.divergedFromEscrowState(participant):
		return blockStateDiverged
	}
	return blockNone
}

// blocks is the one definition of "this participant cannot serve this waiter", read by match and servable alike. See README, "The drain".
func (a availability) blocks(participant string, queued *waiter) blockReason {
	if reason := a.participantBlocked(participant); reason != blockNone {
		return reason
	}
	if queued.exclude[participant] {
		return blockExcluded
	}
	return blockNone
}

func (a availability) onlyThisHostIsLeft(participant string, participants []string, queued *waiter) bool {
	for _, other := range participants {
		if other != participant && a.blocks(other, queued) == blockNone {
			return false
		}
	}
	return true
}

// match is pure and total, and filters by participant rather than slot. See README, "The match decision".
func match(binding HostBinding, waiting []*waiter, participants []string, avail availability, now time.Time, matchWait time.Duration) Decision {
	participant := binding.Participant
	if reason := avail.participantBlocked(participant); reason != blockNone {
		return burn{kind: ghostFor[reason]}
	}

	var oldestLive, excludedOnly *waiter
	for _, queued := range waiting {
		if queued.abandoned.Load() {
			continue
		}
		if oldestLive == nil {
			oldestLive = queued
		}
		switch avail.blocks(participant, queued) {
		case blockNone:
			return serve{waiter: queued}
		case blockExcluded:
			if excludedOnly == nil && avail.onlyThisHostIsLeft(participant, participants, queued) {
				excludedOnly = queued
			}
		}
	}

	if oldestLive == nil {
		return decline{}
	}
	if until := oldestLive.enqueued.Add(matchWait); now.Before(until) {
		return hold{until: until}
	}
	// See routing.md, "Serving a host the request excluded".
	if excludedOnly != nil {
		return serve{waiter: excludedOnly, despiteExclusion: true}
	}
	return burn{kind: ghostExclude}
}
