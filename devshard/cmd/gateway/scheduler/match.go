package scheduler

import "time"

// availability is a frozen per-drain snapshot of the host predicates: reading them live lets a host
// flip between decision and commit, burning a nonce every iteration. capability reports why, not just
// whether -- only the reason separates a refusal that waiting fixes from one it never will.
type availability struct {
	pocRequired  func(participant string) bool
	throttled    func(participant string) bool
	ejected      func(participant string) bool
	capability   func(participant string, profile RequestProfile) (reason string, blocked bool)
	stateBlocked func(participant string) bool
}

// CapabilityToolsUnsupported is the one permanent capability reason; perf writes it, routing reads it.
const CapabilityToolsUnsupported = "tool_choice_unsupported"

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

// blocks is the one definition of "this participant cannot serve this waiter". match and servable both
// read it because they must be exactly as strict as each other: a servable that is stricter fails a
// request match would have served, and one that is laxer keeps a waiter queued that every drain can
// only answer by burning a chain-costed nonce.
func (a availability) blocks(participant string, queued *waiter) blockReason {
	switch {
	case a.pocRequired(participant):
		return blockPoCRequired
	case a.throttled(participant):
		return blockThrottled
	case a.ejected(participant):
		return blockEjected
	case queued.exclude[participant]:
		return blockExcluded
	case a.stateBlocked(participant):
		return blockStateDiverged
	}
	switch reason, blocked := a.capability(participant, queued.profile); {
	case !blocked:
		return blockNone
	case reason == CapabilityToolsUnsupported:
		return blockToolsUnsupported
	}
	return blockCapability
}

// match is pure and total: it reads nothing, mutates nothing, and every path yields a Decision.
// Filters are keyed by participant, never by slot, so a host a request excluded once can never be
// re-served to it through a sibling slot of the same validator.
func match(binding HostBinding, waiting []*waiter, avail availability, now time.Time, stale time.Duration) Decision {
	participant := binding.Participant
	if avail.pocRequired(participant) {
		return burn{kind: ghostPoC}
	}
	if avail.throttled(participant) {
		return burn{kind: ghostThrottled}
	}
	if avail.ejected(participant) {
		return burn{kind: ghostEjected}
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

	// The hold is anchored on the oldest LIVE waiter; with none, the nonce goes back uncommitted.
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
