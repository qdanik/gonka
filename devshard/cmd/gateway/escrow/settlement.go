package escrow

import (
	"context"
	"errors"
	"fmt"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
)

var (
	// ErrDevshardBusy marks a settlement deferred because requests are still spending the escrow's nonces.
	ErrDevshardBusy = errors.New("devshard busy")

	// ErrUnknownEscrow marks a settlement asked for an escrow the registry has no row for.
	ErrUnknownEscrow = errors.New("unknown escrow")
)

// pendingSettleBudget bounds how many parked escrows one tick settles, so a large backlog cannot
// stretch a single tick across minutes of chain round-trips.
const pendingSettleBudget = 4

// settlePending drains escrows parked by retire: deactivated, still registered because their row
// carries the only key that can settle them. A busy or failing escrow simply stays parked.
func (m *Manager) settlePending(ctx context.Context, devshards []store.DevshardRecord) error {
	if !m.config.Load().Rotation.SettlementEnabled {
		return nil
	}
	var errs []error
	attempted := 0
	for _, record := range devshards {
		if record.Active || !record.SettlementPending || attempted >= pendingSettleBudget {
			continue
		}
		attempted++
		if _, err := m.settle(ctx, record); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := m.store.WithRetry(ctx, func() error { return m.store.DeleteDevshard(ctx, record.EscrowID) }); err != nil {
			errs = append(errs, fmt.Errorf("deleting settled escrow %s: %w", record.EscrowID, err))
		}
	}
	return errors.Join(errs...)
}

// Settle settles one escrow on demand, on the same path the rotation lifecycle uses, so an operator
// settlement carries the same dedup, deactivate-first ordering and busy check.
func (m *Manager) Settle(ctx context.Context, escrowID string) (chain.SettleEscrowResult, error) {
	devshards, err := m.store.ListDevshards(ctx)
	if err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("listing devshards: %w", err)
	}
	for _, record := range devshards {
		if record.EscrowID == escrowID {
			return m.settle(ctx, record)
		}
	}
	return chain.SettleEscrowResult{}, fmt.Errorf("settling escrow %s: %w", escrowID, ErrUnknownEscrow)
}

// settle deduplicates concurrent callers for the same escrow (a second caller
// is a no-op, not an error) and only clears SettlementPending once the
// broadcast is confirmed; any earlier failure leaves it set for recovery.
func (m *Manager) settle(ctx context.Context, record store.DevshardRecord) (chain.SettleEscrowResult, error) {
	leave, busy := m.settlements.enter(record.EscrowID)
	if busy {
		return chain.SettleEscrowResult{}, nil
	}
	defer leave()

	// deactivated before any chain call: stops routing traffic without claiming funds are settled.
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardActive(ctx, record.EscrowID, false)
	}); err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("deactivating escrow %s: %w", record.EscrowID, err)
	}
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, true)
	}); err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("marking settlement pending for escrow %s: %w", record.EscrowID, err)
	}

	// busy is a deferred-settle signal, not a failure: the now-deactivated escrow drains, then a retrigger settles it.
	if m.settlementSource.IsBusy(record.EscrowID) {
		return chain.SettleEscrowResult{}, ErrDevshardBusy
	}

	signer, err := m.signer.SignerFor(record.PrivateKeyEnv)
	if err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("resolving signer for escrow %s: %w", record.EscrowID, err)
	}
	if err := m.settlementSource.Finalize(ctx, record.EscrowID); err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("finalizing escrow %s: %w", record.EscrowID, err)
	}
	input, err := m.settlementSource.BuildSettlement(ctx, record.EscrowID)
	if err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("building settlement for escrow %s: %w", record.EscrowID, err)
	}
	result, err := m.tx.SettleEscrow(ctx, signer, input)
	if err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("settling escrow %s: %w", record.EscrowID, err)
	}

	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, false)
	}); err != nil {
		return result, fmt.Errorf("clearing settlement pending for escrow %s: %w", record.EscrowID, err)
	}
	return result, nil
}

// retire honors the SettlementEnabled toggle: off skips the chain call
// entirely, on settles first and only drops the row once settle succeeds.
func (m *Manager) retire(ctx context.Context, record store.DevshardRecord) error {
	// With settlement off the escrow is only parked: its row carries the private-key env name that
	// is the sole way to settle it later, so the row outlives retirement and is dropped only once
	// a settlement has actually been confirmed on chain.
	if !m.config.Load().Rotation.SettlementEnabled {
		if err := m.store.WithRetry(ctx, func() error {
			return m.store.SetDevshardActive(ctx, record.EscrowID, false)
		}); err != nil {
			return fmt.Errorf("deactivating escrow %s: %w", record.EscrowID, err)
		}
		return m.store.WithRetry(ctx, func() error {
			return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, true)
		})
	}

	if _, err := m.settle(ctx, record); err != nil {
		return err // busy or broadcast failure: stays registered, inactive, pending -- not deleted.
	}
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.DeleteDevshard(ctx, record.EscrowID)
	}); err != nil {
		return fmt.Errorf("deleting settled escrow %s: %w", record.EscrowID, err)
	}
	return nil
}
