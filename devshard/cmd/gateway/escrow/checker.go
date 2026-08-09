package escrow

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

// OnEscrowMissing marks an escrow a host reported as absent from chain; the confirming lookup happens
// in the next tick, so this hook does no I/O and never reaches the chain from the request path.
func (m *Manager) OnEscrowMissing(escrowID string) {
	m.missing.mark(escrowID)
}

func (m *Manager) checkMissing(ctx context.Context) error {
	var errs []error
	for _, escrowID := range slices.Sorted(maps.Keys(m.missing.drain())) {
		if err := m.TriggerEscrowCheck(ctx, escrowID); err != nil {
			m.OnEscrowMissing(escrowID) // a failed check must not un-schedule itself
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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
	// Routing stops before the row is written: an escrow the chain confirms absent must take no
	// further request even if that write fails.
	if err := m.settlementSource.Retire(escrowID); err != nil {
		return fmt.Errorf("retiring escrow %s from routing: %w", escrowID, err)
	}
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardActive(ctx, escrowID, false)
	}); err != nil {
		return fmt.Errorf("deactivating escrow %s: %w", escrowID, err)
	}
	logging.Warn("escrow gone from chain, taken out of service", logkey.Escrow, escrowID)
	return nil
}
