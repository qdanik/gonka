package escrow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/store"
	"devshard/logging"
)

var (
	// ErrDevshardBusy marks a settlement deferred because requests are still spending the escrow's nonces.
	ErrDevshardBusy = errors.New("devshard busy")

	// ErrUnknownEscrow marks a settlement asked for an escrow the registry has no row for.
	ErrUnknownEscrow = errors.New("unknown escrow")

	// ErrSettlementInFlight marks a settlement another caller is already running -- not success.
	ErrSettlementInFlight = errors.New("settlement already in flight")
)

// pendingSettleBudget bounds how many parked escrows one tick settles. See escrows.md, "Settlement and retirement".
const pendingSettleBudget = 4

// settlePending drains escrows parked by retire; a busy or failing escrow simply stays parked.
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

// Settle settles one escrow on demand, on the same path the rotation lifecycle uses.
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
		// The settlement the row was kept for has happened; retirement drops it on its own paths, this one has to as well.
		return result, m.deleteSettled(ctx, escrowID)
	}
	return chain.SettleEscrowResult{}, fmt.Errorf("settling escrow %s: %w", escrowID, ErrUnknownEscrow)
}

// park writes inactive and pending in one statement, then stops routing -- the order the busy check relies on.
func (m *Manager) park(ctx context.Context, escrowID string) error {
	if err := m.store.WithRetry(ctx, func() error {
		return m.store.ParkForSettlement(ctx, escrowID)
	}); err != nil {
		return fmt.Errorf("parking escrow %s for settlement: %w", escrowID, err)
	}
	if err := m.settlementSource.Retire(escrowID); err != nil {
		return fmt.Errorf("retiring escrow %s from routing: %w", escrowID, err)
	}
	logging.Info("escrow parked for settlement", logkey.Escrow, escrowID)
	return nil
}

// settle clears SettlementPending only once the broadcast is confirmed. See README.md, "Settlement and retirement".
func (m *Manager) settle(ctx context.Context, record store.DevshardRecord) (chain.SettleEscrowResult, error) {
	leave, busy := m.settlements.enter(record.EscrowID)
	if busy {
		return chain.SettleEscrowResult{}, ErrSettlementInFlight
	}
	defer leave()

	// Parked before the reconciliation: the caller deletes the row on success, and a row that is gone can no longer un-publish the escrow.
	if err := m.park(ctx, record.EscrowID); err != nil {
		return chain.SettleEscrowResult{}, err
	}

	if hash, settled, err := m.alreadySettled(ctx, record); err != nil {
		return chain.SettleEscrowResult{}, err
	} else if settled {
		return chain.SettleEscrowResult{EscrowID: numericEscrowID(record.EscrowID), TxHash: hash}, nil
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
	logging.Info("escrow settled", logkey.Escrow, record.EscrowID, logkey.Model, record.Model, logkey.Tx, result.TxHash, logkey.Settler, result.Settler)

	if err := m.store.WithRetry(ctx, func() error {
		return m.store.SetDevshardSettlementPending(ctx, record.EscrowID, false)
	}); err != nil {
		return result, fmt.Errorf("clearing settlement pending for escrow %s: %w", record.EscrowID, err)
	}
	return result, nil
}

// alreadySettled reports whether the transaction this escrow last broadcast landed. See escrows.md, "Settlement and retirement".
func (m *Manager) alreadySettled(ctx context.Context, record store.DevshardRecord) (string, bool, error) {
	hash, broadcastAt, err := m.store.DevshardSettleTxHash(ctx, record.EscrowID)
	if err != nil {
		return "", false, err
	}
	if hash == "" {
		return "", false, nil
	}
	succeeded, err := m.tx.TxCommitted(ctx, hash)
	switch {
	case errors.Is(err, chain.ErrTxNotFound) && m.settleTxMayStillLand(broadcastAt):
		// Not indexed yet is not never landed: an unordered tx stays landable for its whole TTL.
		return "", false, ErrSettlementInFlight
	case errors.Is(err, chain.ErrTxNotFound):
		return "", false, m.clearSettleTxHash(ctx, record.EscrowID) // past its TTL: a fresh transaction is right
	case err != nil:
		return "", false, fmt.Errorf("checking settle tx %s for escrow %s: %w", hash, record.EscrowID, err)
	case !succeeded:
		return "", false, m.clearSettleTxHash(ctx, record.EscrowID) // committed and rejected: retry is right
	}
	logging.Info("settle already on chain, reconciled", logkey.Escrow, record.EscrowID, logkey.Tx, hash)
	return hash, true, nil
}

// A missing broadcast stamp counts as past the window -- the create path defaults the other way. See README.md, "Reconciling a settle that may already have landed".
func (m *Manager) settleTxMayStillLand(broadcastAt time.Time) bool {
	if broadcastAt.IsZero() {
		return false
	}
	return m.now().Sub(broadcastAt) <= commitmentReconcileGrace
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

// retire honors the SettlementEnabled toggle. See README.md, "Settlement and retirement".
func (m *Manager) retire(ctx context.Context, record store.DevshardRecord) error {
	// Parked only: the row names the sole key that can settle this escrow later, so it outlives retirement.
	if !m.config.Load().Rotation.SettlementEnabled {
		return m.park(ctx, record.EscrowID)
	}

	if _, err := m.settle(ctx, record); err != nil {
		return err // busy, deduped or broadcast failure: stays registered, inactive, pending -- not deleted.
	}
	return m.deleteSettled(ctx, record.EscrowID)
}

// deleteSettled drops the row that named the only key able to settle the escrow, so it runs only after success.
func (m *Manager) deleteSettled(ctx context.Context, escrowID string) error {
	if err := m.store.WithRetry(ctx, func() error { return m.store.DeleteDevshard(ctx, escrowID) }); err != nil {
		return fmt.Errorf("deleting settled escrow %s: %w", escrowID, err)
	}
	logging.Info("settled escrow record dropped", logkey.Escrow, escrowID)
	return nil
}
