package engine

import (
	"context"
	"sync"
)

type handovers struct {
	parent context.Context

	mu       sync.Mutex
	byEscrow map[string]*handover
}

type handover struct {
	wake    context.Context
	release context.CancelFunc
}

func newHandovers(parent context.Context) *handovers {
	return &handovers{parent: parent, byEscrow: map[string]*handover{}}
}

func (h *handovers) wake(escrowID string) context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.entryLocked(escrowID).wake
}

func (h *handovers) release(escrowID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entryLocked(escrowID).release()
}

func (h *handovers) resume(escrowID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	entry, known := h.byEscrow[escrowID]
	if !known || entry.wake.Err() == nil {
		return false
	}
	delete(h.byEscrow, escrowID)
	return true
}

func (h *handovers) entryLocked(escrowID string) *handover {
	entry, known := h.byEscrow[escrowID]
	if !known {
		wake, release := context.WithCancel(h.parent)
		entry = &handover{wake: wake, release: release}
		h.byEscrow[escrowID] = entry
	}
	return entry
}

func executionVotesOnly(steps []TimeoutStep) bool {
	for _, step := range steps {
		if step.Post && (step.Kind != SettleByVote || step.Event.Kind != TimeoutKindExecution) {
			return false
		}
	}
	return true
}

// HandOver stops waiting for the execution votes of one escrow that are not yet due, now and for races that end later, so its session releases once its requests end. The votes stay in the session for the next owner's sweep. See race.md, "Handing an escrow over".
func (e *Engine) HandOver(escrowID string) { e.handovers.release(escrowID) }

// Resume makes an escrow wait for its execution votes again, as after a handover that was taken back, and reports whether it had been handed over.
func (e *Engine) Resume(escrowID string) bool { return e.handovers.resume(escrowID) }
