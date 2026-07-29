package scheduler

import "time"

// availability is a frozen per-drain snapshot of the host predicates. Reading them live would let a
// host flip between the decision and the commit, turning a bounded drain into an unbounded spin that
// burns a nonce every iteration.
type availability struct {
	pocRequired  func(participant string) bool
	throttled    func(participant string) bool
	capability   func(participant string, profile RequestProfile) bool
	stateBlocked func(participant string) bool
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

	blockedByHost := avail.stateBlocked(participant)
	sawBlockedWaiter := false
	var oldestLive *waiter
	for _, queued := range waiting {
		if queued.abandoned.Load() {
			continue
		}
		if oldestLive == nil {
			oldestLive = queued
		}
		if queued.exclude[participant] {
			continue
		}
		if blockedByHost || avail.capability(participant, queued.profile) {
			sawBlockedWaiter = true
			continue
		}
		return serve{waiter: queued}
	}

	// A hold parks the nonce on behalf of the queue's head, so an abandoned head must never set the
	// deadline: that would keep a nonce open for a caller who will never be served.
	if oldestLive != nil {
		if until := oldestLive.enqueued.Add(stale); now.Before(until) {
			return hold{until: until}
		}
	}
	if sawBlockedWaiter {
		return burn{kind: ghostCapability}
	}
	return burn{kind: ghostExclude}
}
