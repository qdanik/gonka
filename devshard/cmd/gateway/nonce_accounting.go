package main

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/logging"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	nonceAccountingDatabase = "accounting.db"

	nonceAccountingShutdownGrace     = 5 * time.Second
	nonceAccountingReadHeaderTimeout = 10 * time.Second

	nonceAccountingSweepInterval = 10 * time.Second
)

type epochSource interface {
	Snapshot() chain.PhaseSnapshot
}

type escrowSource interface {
	Snapshot() []registry.EscrowState
	RoutableSession(escrowID string) (registry.EscrowSession, bool)
}

type nonceAccounting struct {
	service  *accounting.Service
	listener *http.Server
	epochs   epochSource
}

func openNonceAccounting(settings config.NonceAccounting, storageDir string, epochs epochSource, now func() time.Time) *nonceAccounting {
	if !settings.Enabled {
		return nil
	}
	ledger := &nonceAccounting{epochs: epochs}
	store, openErr := accounting.OpenStore(filepath.Join(storageDir, nonceAccountingDatabase))
	if openErr != nil {
		logging.Error("nonce accounting could not open its store", "error", openErr)
		return nil
	}
	service, restoreErr := accounting.NewService(accounting.Settings{
		Store:            store,
		SnapshotInterval: time.Duration(settings.SnapshotSeconds) * time.Second,
		RetentionEpochs:  uint64(settings.RetentionEpochs),
		CurrentEpoch:     ledger.currentEpoch,
		Now:              now,
	})
	if restoreErr != nil {
		logging.Warn("nonce accounting started without its stored counters", "error", restoreErr)
	}
	ledger.service = service
	if settings.ListenAddr != "" {
		ledger.listener = &http.Server{
			Addr:              settings.ListenAddr,
			Handler:           accounting.NewHandler(service.Book, ledger.currentEpoch),
			ReadHeaderTimeout: nonceAccountingReadHeaderTimeout,
		}
	}
	return ledger
}

func (n *nonceAccounting) currentEpoch(context.Context) (uint64, error) {
	if n == nil || n.epochs == nil {
		return 0, errors.New("chain snapshot is unavailable")
	}
	snapshot := n.epochs.Snapshot()
	if snapshot.EpochIndex == 0 {
		return 0, errors.New("current epoch is not known yet")
	}
	return snapshot.EpochIndex, nil
}

func (n *nonceAccounting) reset() error {
	if n == nil {
		return nil
	}
	return n.service.Reset()
}

func (n *nonceAccounting) collectors() []prometheus.Collector {
	if n == nil {
		return nil
	}
	return []prometheus.Collector{accounting.NewCollector(n.service.Book)}
}

func (n *nonceAccounting) start(ctx context.Context, escrows escrowSource) {
	if n == nil {
		return
	}
	if n.listener != nil {
		logging.Info("nonce accounting listening", logkey.Addr, n.listener.Addr)
		go func() {
			if err := n.listener.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logging.Error("nonce accounting listener stopped", "error", err)
			}
		}()
	}
	go n.sweepUntil(ctx, escrows)
}

func (n *nonceAccounting) sweepUntil(ctx context.Context, escrows escrowSource) {
	ticker := time.NewTicker(nonceAccountingSweepInterval)
	defer ticker.Stop()
	for {
		n.sweep(ctx, escrows)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (n *nonceAccounting) sweep(ctx context.Context, escrows escrowSource) {
	// The epoch stamped here is the one the escrow was first seen in, not the one it was created in,
	// which the gateway never reads. With counters that start empty on every boot the two say the same
	// thing: the epoch this ledger's numbers cover.
	epoch, epochErr := n.currentEpoch(ctx)
	published := make(map[string]struct{})
	for _, state := range escrows.Snapshot() {
		session, routable := escrows.RoutableSession(state.ID)
		if !routable {
			continue
		}
		published[state.ID] = struct{}{}
		escrowState := session.SnapshotState()
		if epochErr == nil {
			n.report(n.service.Book.OpenEscrow(accounting.EscrowMetadata{
				EscrowID:      state.ID,
				Model:         state.Model,
				CreationEpoch: epoch,
				Slots:         escrowState.Group,
			}))
		}
		n.report(n.service.Book.ObserveLatestNonce(state.ID, escrowState.LatestNonce))
		n.reconcileFinished(state.ID, session)
		for slotID, stats := range escrowState.HostStats {
			if stats != nil {
				n.report(n.service.Book.ObserveHostStats(state.ID, slotID, *stats))
			}
		}
	}
	for _, escrowID := range n.service.Book.EscrowIDs() {
		if _, still := published[escrowID]; !still {
			n.service.Book.RetireEscrow(escrowID)
		}
	}
}

func (n *nonceAccounting) reconcileFinished(escrowID string, session registry.EscrowSession) {
	unfinished := n.service.Book.UnfinishedNonces(escrowID)
	finished := make([]uint64, 0, len(unfinished))
	for _, nonce := range unfinished {
		if session.UserSession().IsNonceFinished(nonce) {
			finished = append(finished, nonce)
		}
	}
	n.report(n.service.Book.MarkFinished(escrowID, finished))
}

func (n *nonceAccounting) recordGhost(escrowID string, nonce uint64, reason string) {
	if n != nil {
		n.report(n.service.Book.RecordGhost(escrowID, nonce, reason))
	}
}

func (n *nonceAccounting) recordRace(outcome engine.RaceOutcome) {
	if n == nil {
		return
	}
	phase := accounting.PhaseNormal
	if outcome.PoCBypassActive {
		phase = accounting.PhasePoC
	}
	attempts := make([]accounting.Attempt, 0, len(outcome.Attempts))
	for _, attempt := range outcome.Attempts {
		attempts = append(attempts, accounting.Attempt{
			Nonce:        attempt.Nonce,
			Sent:         !attempt.SendTime.IsZero(),
			Acknowledged: !attempt.ReceiptTime.IsZero(),
			Finished:     attempt.NonceFinished,
			Usage:        usageOf(outcome, attempt),
			Terminal:     attempt.Terminal.String(),
			Phase:        phase,
			SlowReceipt:  slowReceipt(attempt),
			SlowChunk:    attempt.MaxChunkGap > accounting.SlowChunkGap,
			ClockDrifted: clockDrifted(attempt),
			SlowDecode:   engine.TimePerOutputToken(attempt) > accounting.SlowDecode,
		})
	}
	n.report(n.service.Book.RecordRace(outcome.EscrowID, attempts))
}

// slowReceipt measures from the dispatch, and only where both stamps exist: an attempt that never got
// a receipt is a refusal, which the ledger already counts, and calling it slow as well would report
// one failure twice.
func slowReceipt(attempt engine.AttemptOutcome) bool {
	if attempt.SendTime.IsZero() || attempt.ReceiptTime.IsZero() {
		return false
	}
	return attempt.ReceiptTime.Sub(attempt.SendTime) > accounting.SlowReceipt
}

// clockDrifted reads the offset in either direction: a host running ahead makes the gateway wait past
// a deadline that already passed, one running behind makes it vote on a nonce still being served.
func clockDrifted(attempt engine.AttemptOutcome) bool {
	offset, measured := engine.ClockOffset(attempt)
	return measured && (offset > accounting.ClockDrift || offset < -accounting.ClockDrift)
}

func (n *nonceAccounting) recordTimeout(event engine.TimeoutEvent) {
	if n != nil {
		n.report(n.service.Book.RecordTimeout(event.EscrowID, event.Nonce, event.Kind, event.Action, event.Reason))
	}
}

func usageOf(outcome engine.RaceOutcome, attempt engine.AttemptOutcome) accounting.Usage {
	switch {
	case outcome.IsWinner(attempt):
		return accounting.UsageWinner
	case outcome.Succeeded:
		return accounting.UsageLoser
	default:
		return accounting.UsageUnknown
	}
}

func (n *nonceAccounting) report(err error) {
	if err != nil && !errors.Is(err, accounting.ErrUnknownEscrow) {
		logging.Warn("nonce accounting refused a fact", "error", err)
	}
}

func (n *nonceAccounting) Close() error {
	if n == nil {
		return nil
	}
	if n.listener == nil {
		return n.service.Close()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), nonceAccountingShutdownGrace)
	defer cancelShutdown()
	return errors.Join(n.listener.Shutdown(shutdownCtx), n.service.Close())
}
