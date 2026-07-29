package scheduler

import (
	"sync/atomic"
	"time"
)

// Decision is match's total outcome for one nonce: exactly one of serve, burn, or hold. The
// exhaustiveness of this interface is the nonce-liveness invariant made compiler-checked.
type Decision interface{ isDecision() }

type serve struct{ waiter *waiter }
type burn struct{ kind GhostKind }

// hold declines the bound nonce without committing it until until, giving a co-arriving
// compatible waiter a chance before the nonce is burned.
type hold struct{ until time.Time }

func (serve) isDecision() {}
func (burn) isDecision()  {}
func (hold) isDecision()  {}

// waiter is one request queued on an escrow's dispatcher, waiting for a compatible nonce. abandoned
// is how a cancelled caller leaves the queue: the dispatcher owns the queue itself, so a departure
// must never be a message the actor has to receive.
type waiter struct {
	profile   RequestProfile
	exclude   map[string]bool // profile.Exclude as a set, participant-keyed
	enqueued  time.Time
	replyCh   chan pickResult
	abandoned atomic.Bool
}

// newWaiter buffers replyCh so the dispatcher's handoff never blocks on the caller.
func newWaiter(profile RequestProfile, enqueued time.Time) *waiter {
	queued := &waiter{
		profile:  profile,
		exclude:  make(map[string]bool, len(profile.Exclude)),
		enqueued: enqueued,
		replyCh:  make(chan pickResult, 1),
	}
	for _, participant := range profile.Exclude {
		queued.exclude[participant] = true
	}
	return queued
}

type pickResult struct {
	assignment Assignment
	err        error
}
