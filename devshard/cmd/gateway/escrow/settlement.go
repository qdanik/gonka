package escrow

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
	"devshard/logging"
)

var (
	// ErrDevshardBusy marks a settlement deferred because requests are still spending the escrow's nonces.
	ErrDevshardBusy = errors.New("devshard busy")

	// ErrUnknownEscrow marks a settlement asked for an escrow the registry has no row for.
	ErrUnknownEscrow = errors.New("unknown escrow")

	// ErrSettlementInFlight marks a settlement another caller is already running: not success, because
	// the row carries the only key that can settle the escrow and belongs to that other caller.
	ErrSettlementInFlight = errors.New("settlement already in flight")
)

// pendingSettleBudget bounds how many parked escrows one tick settles. See
// gateway-escrow-lifecycle.md, "Settlement and retirement".
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
		if err := m.deleteSettled(ctx, record.EscrowID); err != nil {
			errs = append(errs, err)
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
		if record.EscrowID != escrowID {
			continue
		}
		result, err := m.settle(ctx, record)
		if err != nil {
			return result, err
		}
		// The row holds the only name of the key that could settle this escrow, and the settlement it
		// was kept for has happened. Retirement drops it on its own paths; this one has to as well.
		return result, m.deleteSettled(ctx, escrowID)
	}
	return chain.SettleEscrowResult{}, fmt.Errorf("settling escrow %s: %w", escrowID, ErrUnknownEscrow)
}

// park records the intent to settle -- inactive and pending in one write, so a crash leaves the escrow
// recoverable -- and then stops routing to it, which is what the busy check that follows relies on.
func (m *Manager) park(ctx context.Context, escrowID string) error {
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.ParkForSettlement(ctx, escrowID)
	}); err != nil {
		return fmt.Errorf("parking escrow %s for settlement: %w", escrowID, err)
	}
	if err := m.settlementSource.Retire(escrowID); err != nil {
		return fmt.Errorf("retiring escrow %s from routing: %w", escrowID, err)
	}
	logging.Info("escrow parked for settlement", "escrow", escrowID)
	return nil
}

// settle deduplicates concurrent callers for the same escrow and only clears SettlementPending once
// the broadcast is confirmed; any earlier failure leaves it set for recovery.
func (m *Manager) settle(ctx context.Context, record store.DevshardRecord) (chain.SettleEscrowResult, error) {
	leave, busy := m.settlements.enter(record.EscrowID)
	if busy {
		return chain.SettleEscrowResult{}, ErrSettlementInFlight
	}
	defer leave()

	if settled, err := m.alreadySettled(ctx, record); err != nil {
		return chain.SettleEscrowResult{}, err
	} else if settled {
		return chain.SettleEscrowResult{EscrowID: numericEscrowID(record.EscrowID), TxHash: record.SettleTxHash}, nil
	}

	if err := m.park(ctx, record.EscrowID); err != nil {
		return chain.SettleEscrowResult{}, err
	}

	// busy is a deferred-settle signal, not a failure: the now-retired escrow drains, then a retrigger settles it.
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
	recordHash := func(txHash string) error {
		return m.store.WithRetry(ctx, func() error { return m.store.SetDevshardSettleTxHash(ctx, record.EscrowID, txHash) })
	}
	result, err := m.tx.SettleEscrow(ctx, signer, input, recordHash)
	if err != nil {
		return chain.SettleEscrowResult{}, fmt.Errorf("settling escrow %s: %w", record.EscrowID, err)
	}
	logging.Info("escrow settled", "escrow", record.EscrowID, "model", record.Model, "tx", result.TxHash, "settler", result.Settler)

	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, false)
	}); err != nil {
		return result, fmt.Errorf("clearing settlement pending for escrow %s: %w", record.EscrowID, err)
	}
	return result, nil
}

// alreadySettled reports whether the transaction this escrow last broadcast reached the chain and
// succeeded. An unreachable endpoint is not an answer, so it fails the tick rather than concluding the
// settle never happened. See gateway-escrow-lifecycle.md, "Settling an escrow, and surviving a crash".
func (m *Manager) alreadySettled(ctx context.Context, record store.DevshardRecord) (bool, error) {
	if record.SettleTxHash == "" {
		return false, nil
	}
	succeeded, err := m.tx.TxCommitted(ctx, record.SettleTxHash)
	switch {
	case errors.Is(err, chain.ErrTxNotFound):
		return false, m.clearSettleTxHash(ctx, record.EscrowID) // never landed: a fresh transaction is right
	case err != nil:
		return false, fmt.Errorf("checking settle tx %s for escrow %s: %w", record.SettleTxHash, record.EscrowID, err)
	case !succeeded:
		return false, m.clearSettleTxHash(ctx, record.EscrowID) // committed and rejected: retry is right
	}
	logging.Info("settle already on chain, reconciled", "escrow", record.EscrowID, "tx", record.SettleTxHash)
	return true, nil
}

func (m *Manager) clearSettleTxHash(ctx context.Context, escrowID string) error {
	if err := m.store.WithRetry(ctx, func() error { return m.store.SetDevshardSettleTxHash(ctx, escrowID, "") }); err != nil {
		return fmt.Errorf("clearing settle tx hash for escrow %s: %w", escrowID, err)
	}
	return nil
}

func numericEscrowID(escrowID string) uint64 {
	parsed, err := strconv.ParseUint(escrowID, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

// retire honors the SettlementEnabled toggle: off skips the chain call
// entirely, on settles first and only drops the row once settle succeeds.
func (m *Manager) retire(ctx context.Context, record store.DevshardRecord) error {
	// With settlement off the escrow is only parked: its row carries the private-key env name that
	// is the sole way to settle it later, so the row outlives retirement and is dropped only once
	// a settlement has actually been confirmed on chain.
	if !m.config.Load().Rotation.SettlementEnabled {
		return m.park(ctx, record.EscrowID)
	}

	if _, err := m.settle(ctx, record); err != nil {
		return err // busy, deduped or broadcast failure: stays registered, inactive, pending -- not deleted.
	}
	return m.deleteSettled(ctx, record.EscrowID)
}

// deleteSettled drops the row that named the only key able to settle the escrow, so it runs on both
// settle paths only once the settlement itself succeeded.
func (m *Manager) deleteSettled(ctx context.Context, escrowID string) error {
	if err := m.store.WithRetry(ctx, func() error { return m.store.DeleteDevshard(ctx, escrowID) }); err != nil {
		return fmt.Errorf("deleting settled escrow %s: %w", escrowID, err)
	}
	logging.Info("settled escrow record dropped", "escrow", escrowID)
	return nil
}
