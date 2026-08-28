package scheduler

import (
	"sync"
	"time"
)

// replayCredit holds the one catch-up replay a participant gets on an escrow before a state-root
// divergence blocks it. A host rolls its diff back when its root disagrees, so its state survives the
// refusal intact and replaying the retained chain costs one request to try.
type replayCredit struct {
	mu    sync.Mutex
	spent map[string]map[string]time.Time
}

// spend reports whether the participant still had its replay, and takes it if so.
func (c *replayCredit) spend(escrowID, participant string, at time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	byParticipant, known := c.spent[escrowID]
	if !known {
		byParticipant = map[string]time.Time{}
		if c.spent == nil {
			c.spent = map[string]map[string]time.Time{}
		}
		c.spent[escrowID] = byParticipant
	}
	if _, taken := byParticipant[participant]; taken {
		return false
	}
	byParticipant[participant] = at
	return true
}

// restore returns the replay to a participant that served since it was taken. Requests to one
// participant overlap, so a send that started before the replay proves nothing about the replayed
// state and must not return the credit.
func (c *replayCredit) restore(escrowID, participant string, sentAt time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	byParticipant := c.spent[escrowID]
	takenAt, taken := byParticipant[participant]
	if !taken || !sentAt.After(takenAt) {
		return
	}
	delete(byParticipant, participant)
	if len(byParticipant) == 0 {
		delete(c.spent, escrowID)
	}
}

func (c *replayCredit) forget(escrowID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.spent, escrowID)
}
