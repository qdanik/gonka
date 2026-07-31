package escrow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
)

// OnEscrowMissing marks an escrow a host reported as absent from chain; the confirming lookup happens
// in the next tick, so this hook does no I/O and never reaches the chain from the request path.
func (m *Manager) OnEscrowMissing(escrowID string) {
	m.missingMu.Lock()
	defer m.missingMu.Unlock()
	if m.missingMarks == nil {
		m.missingMarks = make(map[string]bool)
	}
	m.missingMarks[escrowID] = true
}

func (m *Manager) checkMissing(ctx context.Context) error {
	var errs []error
	for _, escrowID := range slices.Sorted(maps.Keys(m.drainMissingMarks())) {
		if err := m.TriggerEscrowCheck(ctx, escrowID); err != nil {
			m.OnEscrowMissing(escrowID) // a failed check must not un-schedule itself
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) drainMissingMarks() map[string]bool {
	m.missingMu.Lock()
	defer m.missingMu.Unlock()
	marks := m.missingMarks
	m.missingMarks = nil
	return marks
}

// Only a confirmed not-found deactivates the escrow; a lookup error or a found escrow both keep it
// active -- ambiguity is never a reason to deactivate. Concurrent callers for the same ID dedup to one check.
func (m *Manager) TriggerEscrowCheck(ctx context.Context, escrowID string) error {
	leave, busy := m.checks.enter(escrowID)
	if busy {
		return nil
	}
	defer leave()

	_, found, err := m.tx.GetEscrow(ctx, escrowID)
	if err != nil {
		return fmt.Errorf("checking escrow %s: %w", escrowID, err)
	}
	if found {
		return nil
	}
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardActive(ctx, escrowID, false)
	}); err != nil {
		return fmt.Errorf("deactivating escrow %s: %w", escrowID, err)
	}
	return nil
}
