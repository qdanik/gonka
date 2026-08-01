package escrow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"
	"devshard/logging"
)

// commitmentIndexLagMargin is how long a landed tx may stay unqueryable past the chain's
// unordered-tx TTL before the commitment record can be cleared.
const commitmentIndexLagMargin = 2 * time.Minute

const commitmentReconcileGrace = chain.UnorderedTxTTL + commitmentIndexLagMargin

type Manager struct {
	tx        escrowTxClient
	store     escrowStore
	snapshots snapshotSource
	signer    SignerSource
	breaker   *createBreaker
	now       func() time.Time

	config           *config.Holder
	settlementSource SettlementSource
	settlements      inFlightSet
	checks           inFlightSet

	depleted markSet
	missing  markSet

	lifecycleMu sync.Mutex
	stop        chan struct{}
	done        chan struct{}
	cancel      context.CancelFunc
}

// CreateEscrow creates one escrow on demand and registers it, on the same durable-intent path the
// rotation lifecycle uses, so an operator-created escrow is recovered by reconcile like any other.
func (m *Manager) CreateEscrow(ctx context.Context, model ModelConfig) (chain.CreateEscrowResult, error) {
	snapshot := m.snapshots.Snapshot()
	return m.createEscrow(ctx, model, roleRegular, snapshot.EpochIndex, snapshot.BlockHeight)
}

// A failed intent-commitment write (in onPrepared) aborts before any chain broadcast: no broadcast without durable intent.
func (m *Manager) createEscrow(ctx context.Context, model ModelConfig, role string, epoch uint64, blockHeight int64) (chain.CreateEscrowResult, error) {
	signer, err := m.signer.SignerFor(model.PrivateKeyEnv)
	if err != nil {
		return chain.CreateEscrowResult{}, fmt.Errorf("resolving signer for %s: %w", model.PrivateKeyEnv, err)
	}

	c := store.Commitment{
		Model:         model.ModelID,
		Role:          role,
		Epoch:         epoch,
		PrivateKeyEnv: model.PrivateKeyEnv,
		BlockHeight:   blockHeight,
	}
	onPrepared := func(txHash string) error {
		c.TxHash = txHash
		c.CreatedAt = m.now()
		return m.store.WithRetry(ctx, func() error { return m.store.SaveCommitment(ctx, c) })
	}

	result, err := m.tx.CreateEscrow(ctx, signer, model.Amount, model.ModelID, onPrepared)
	if err != nil {
		return chain.CreateEscrowResult{}, fmt.Errorf("creating escrow for %s/%s: %w", model.ModelID, role, err)
	}
	logging.Info("escrow created", "escrow", result.EscrowID, "model", model.ModelID, "role", role, "epoch", epoch, "tx", result.TxHash)
	return result, m.persistEscrow(ctx, strconv.FormatUint(result.EscrowID, 10), c)
}

// UpsertDevshard is an idempotent upsert (duplicate registration is a no-op success); a failed
// registry write leaves the commitment in place so the next reconcile retries from durable intent.
func (m *Manager) persistEscrow(ctx context.Context, escrowID string, c store.Commitment) error {
	record := store.DevshardRecord{
		EscrowID:      escrowID,
		PrivateKeyEnv: c.PrivateKeyEnv,
		Model:         c.Model,
		Active:        true,
		RotationRole:  c.Role,
		RotationEpoch: int64(c.Epoch),
	}
	if err := m.store.WithRetry(ctx, func() error { return m.store.UpsertDevshard(ctx, record) }); err != nil {
		return fmt.Errorf("registering escrow %s: %w", escrowID, err)
	}
	if err := m.store.WithRetry(ctx, func() error { return m.store.DeleteCommitment(ctx, c.TxHash) }); err != nil {
		return fmt.Errorf("clearing commitment for escrow %s: %w", escrowID, err)
	}
	m.breaker.reset(c.Model, c.Role)
	return nil
}

func (m *Manager) reconcile(ctx context.Context) error {
	commitments, err := m.store.LoadCommitments(ctx)
	if err != nil {
		return fmt.Errorf("loading commitments: %w", err)
	}

	var errs []error
	for _, c := range commitments {
		if err := m.reconcileOne(ctx, c); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) reconcileOne(ctx context.Context, c store.Commitment) error {
	escrowID, found, err := m.tx.GetTxEscrowID(ctx, c.TxHash)
	switch {
	case err == nil && found:
		return m.persistEscrow(ctx, strconv.FormatUint(escrowID, 10), c)
	case err == nil && !found:
		return m.clearCommitment(ctx, c.TxHash) // committed but produced no escrow event: terminal
	case errors.Is(err, chain.ErrTxNotFound):
		if m.txMayStillLand(c) {
			return nil // unordered tx may still land: keep, retry next tick
		}
		return m.clearCommitment(ctx, c.TxHash) // past its TTL: can never land
	default:
		return fmt.Errorf("querying tx %s: %w", c.TxHash, err) // endpoint unreachable: keep, retry next tick
	}
}

func (m *Manager) clearCommitment(ctx context.Context, txHash string) error {
	if err := m.store.WithRetry(ctx, func() error { return m.store.DeleteCommitment(ctx, txHash) }); err != nil {
		return fmt.Errorf("clearing commitment %s: %w", txHash, err)
	}
	return nil
}

// A zero CreatedAt (malformed/legacy row) defensively counts as still-pending.
func (m *Manager) txMayStillLand(c store.Commitment) bool {
	if c.CreatedAt.IsZero() {
		return true
	}
	return m.now().Sub(c.CreatedAt) <= commitmentReconcileGrace
}
