package registry

import (
	"context"

	"devshard/types"
)

// DrainStalledChallenges carries the votes a challenged nonce is waiting on into a diff, for the escrows
// holding one. See routing.md, "A challenge waits for votes no request carries".
func (r *Registry) DrainStalledChallenges(ctx context.Context, budget int) (stalled, drained, failed int) {
	states := r.Snapshot()
	if budget <= 0 || len(states) == 0 {
		return 0, 0, 0
	}
	start := int(r.challengeCursor.Add(1)-1) % len(states)
	for offset := range states {
		if drained+failed >= budget || ctx.Err() != nil {
			break
		}
		state := states[(start+offset)%len(states)]
		session, release, held := r.Acquire(state.ID)
		if !held {
			continue
		}
		sent, failure := func() (bool, bool) {
			defer release()
			if challengedNonces(session) == 0 {
				return false, false
			}
			stalled++
			underlying := session.UserSession()
			if underlying == nil {
				return false, false
			}
			if err := underlying.SendPendingDiff(ctx); err != nil {
				return false, true
			}
			return true, false
		}()
		if sent {
			drained++
		}
		if failure {
			failed++
		}
	}
	return stalled, drained, failed
}

// challengedNonces counts the records a validator disputed and no further vote has resolved.
func challengedNonces(session EscrowSession) int {
	open := 0
	for _, record := range session.SnapshotState().Inferences {
		if record != nil && record.Status == types.StatusChallenged {
			open++
		}
	}
	return open
}
