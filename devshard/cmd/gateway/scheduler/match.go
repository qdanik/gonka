package scheduler

import (
	"time"

	"devshard/cmd/gateway/perf"
)

// availability is a frozen per-drain snapshot of the host predicates; capability reports why a host is
// blocked, not merely whether. See gateway-routing-and-nonces.md, "The drain".
type availability struct {
	pocRequired  func(participant string) bool
	throttled    func(participant string) bool
	ejected      func(participant string) bool
	capability   func(participant string, profile RequestProfile) (reason string, blocked bool)
	stateBlocked func(participant string) bool
}

type blockReason int

const (
	blockNone blockReason = iota
	blockPoCRequired
	blockThrottled
	blockEjected
	blockExcluded
	blockStateDiverged
	blockCapability
	blockToolsUnsupported
)

// ghostFor names the burn each participant-only block earns, as one mapping rather than two switches.
var ghostFor = map[blockReason]GhostKind{
	blockPoCRequired: ghostPoC,
	blockThrottled:   ghostThrottled,
	blockEjected:     ghostEjected,
}

// participantBlocked is the half of blocks that needs no waiter, so match and blocks share one ladder.
func (a availability) participantBlocked(participant string) blockReason {
	switch {
	case a.pocRequired(participant):
		return blockPoCRequired
	case a.throttled(participant):
		return blockThrottled
	case a.ejected(participant):
		return blockEjected
	}
	return blockNone
}

// blocks is the one definition of "this participant cannot serve this waiter". match and servable both
// read it because they must be exactly as strict as each other: a servable that is stricter fails a
// request match would have served, and one that is laxer keeps a waiter queued that every drain can
// only answer by burning a chain-costed nonce.
func (a availability) blocks(participant string, queued *waiter) blockReason {
	if reason := a.participantBlocked(participant); reason != blockNone {
		return reason
	}
	switch {
	case queued.exclude[participant]:
		return blockExcluded
	case a.stateBlocked(participant):
		return blockStateDiverged
	}
	switch reason, blocked := a.capability(participant, queued.profile); {
	case !blocked:
		return blockNone
	case reason == perf.CapabilityToolsUnsupported:
		return blockToolsUnsupported
	}
	return blockCapability
}

// match is pure and total: it reads nothing, mutates nothing, and every path yields a Decision.
// Filters are keyed by participant, never by slot, so a host a request excluded once can never be
// re-served to it through a sibling slot of the same validator.
func match(binding HostBinding, waiting []*waiter, avail availability, now time.Time, stale time.Duration) Decision {
	participant := binding.Participant
	if reason := avail.participantBlocked(participant); reason != blockNone {
		return burn{kind: ghostFor[reason]}
	}

	sawBlockedWaiter := false
	var oldestLive *waiter
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
		default:
			sawBlockedWaiter = true
		}
	}

	if oldestLive == nil {
		return decline{}
	}
	if until := oldestLive.enqueued.Add(stale); now.Before(until) {
		return hold{until: until}
	}
	if sawBlockedWaiter {
		return burn{kind: ghostCapability}
	}
	return burn{kind: ghostExclude}
}
