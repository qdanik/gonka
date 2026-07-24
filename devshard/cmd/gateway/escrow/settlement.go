package escrow

import (
	"context"
	"errors"
	"fmt"

	"devshard/cmd/gateway/store"
)

var errDevshardBusy = errors.New("devshard busy")

// settle deduplicates concurrent callers for the same escrow (a second caller
// is a no-op, not an error) and only clears SettlementPending once the
// broadcast is confirmed; any earlier failure leaves it set for recovery.
func (m *Manager) settle(ctx context.Context, record store.DevshardRecord) error {
	leave, busy := m.settlements.enter(record.EscrowID)
	if busy {
		return nil
	}
	defer leave()

	// deactivated before any chain call: stops routing traffic without claiming funds are settled.
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardActive(ctx, record.EscrowID, false)
	}); err != nil {
		return fmt.Errorf("deactivating escrow %s: %w", record.EscrowID, err)
	}
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, true)
	}); err != nil {
		return fmt.Errorf("marking settlement pending for escrow %s: %w", record.EscrowID, err)
	}

	// busy is a deferred-settle signal, not a failure: the now-deactivated escrow drains, then a retrigger settles it.
	if m.settlementSource.IsBusy(record.EscrowID) {
		return errDevshardBusy
	}

	signer, err := m.signer.SignerFor(record.PrivateKeyEnv)
	if err != nil {
		return fmt.Errorf("resolving signer for escrow %s: %w", record.EscrowID, err)
	}
	if err := m.settlementSource.Finalize(ctx, record.EscrowID); err != nil {
		return fmt.Errorf("finalizing escrow %s: %w", record.EscrowID, err)
	}
	input, err := m.settlementSource.BuildSettlement(ctx, record.EscrowID)
	if err != nil {
		return fmt.Errorf("building settlement for escrow %s: %w", record.EscrowID, err)
	}
	if _, err := m.tx.SettleEscrow(ctx, signer, input); err != nil {
		return fmt.Errorf("settling escrow %s: %w", record.EscrowID, err)
	}

	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, false)
	}); err != nil {
		return fmt.Errorf("clearing settlement pending for escrow %s: %w", record.EscrowID, err)
	}
	return nil
}

// retire honors the SettlementEnabled toggle: off skips the chain call
// entirely, on settles first and only drops the row once settle succeeds.
func (m *Manager) retire(ctx context.Context, record store.DevshardRecord) error {
	if !m.config.Load().Rotation.SettlementEnabled {
		if err := m.store.WithRetry(ctx, func() error {
			return m.store.SetDevshardActive(ctx, record.EscrowID, false)
		}); err != nil {
			return fmt.Errorf("deactivating escrow %s: %w", record.EscrowID, err)
		}
		if err := m.store.WithRetry(ctx, func() error {
			return m.store.DeleteDevshard(ctx, record.EscrowID)
		}); err != nil {
			return fmt.Errorf("deleting escrow %s: %w", record.EscrowID, err)
		}
		return nil
	}

	if err := m.settle(ctx, record); err != nil {
		return err // busy or broadcast failure: stays registered, inactive, pending -- not deleted.
	}
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.DeleteDevshard(ctx, record.EscrowID)
	}); err != nil {
		return fmt.Errorf("deleting settled escrow %s: %w", record.EscrowID, err)
	}
	return nil
}
