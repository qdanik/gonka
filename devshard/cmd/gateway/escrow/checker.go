package escrow

import (
	"context"
	"fmt"
)

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
