package registry

import (
	"context"
	"time"
)

// SweepExecutionTimeouts retries the execution timeouts no race is left to post. The budget is the
// ceiling for the whole tick, not per escrow, so the added vote traffic has one upper bound whatever
// the escrow count is; the walk starts one escrow further along each tick, so a long backlog on the
// first escrow cannot starve the rest. Each escrow is held for its own sweep, so a retirement waits
// instead of closing a session mid-vote.
func (r *Registry) SweepExecutionTimeouts(ctx context.Context, grace time.Duration, budget int) (due, applied, failed int) {
	states := r.Snapshot()
	if budget <= 0 || len(states) == 0 {
		return 0, 0, 0
	}
	start := int(r.sweepCursor.Add(1)-1) % len(states)
	remaining := budget
	for offset := range states {
		if remaining <= 0 || ctx.Err() != nil {
			break
		}
		state := states[(start+offset)%len(states)]
		session, release, held := r.Acquire(state.ID)
		if !held {
			continue // retired between the snapshot and the hold; its own settlement owns it now
		}
		report := func() sweepCounts {
			defer release()
			underlying := session.UserSession()
			if underlying == nil {
				return sweepCounts{}
			}
			swept := underlying.SweepExecutionTimeouts(ctx, grace, remaining)
			return sweepCounts{due: swept.Due, applied: swept.Applied, failed: swept.Failed}
		}()
		remaining -= report.due
		due += report.due
		applied += report.applied
		failed += report.failed
	}
	return due, applied, failed
}

type sweepCounts struct {
	due     int
	applied int
	failed  int
}
