// Package nonces turns what the gateway did to a nonce -- the race it ran, the burn it took, the
// timeout it voted -- and what the chain later recorded about it into one ledger, and serves that ledger.
package nonces

import (
	"context"
	"errors"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/logging"
	"devshard/types"
)

const (
	nonceAccountingDatabase = "accounting.db"

	nonceAccountingShutdownGrace     = 5 * time.Second
	nonceAccountingReadHeaderTimeout = 10 * time.Second

	nonceAccountingSweepInterval = 10 * time.Second
)

type EpochSource interface {
	Snapshot() chain.PhaseSnapshot
}

type EscrowSource interface {
	Snapshot() []registry.EscrowState
	RoutableSession(escrowID string) (registry.EscrowSession, bool)
}

type Recorder struct {
	service  *accounting.Service
	listener *http.Server
	epochs   EpochSource

	capability atomic.Pointer[accounting.CapabilityFunc]

	observing sync.Map
}

func (n *Recorder) watchDiffs(escrowID string, session registry.EscrowSession) {
	if _, already := n.observing.LoadOrStore(escrowID, struct{}{}); already {
		return
	}
	underlying := session.UserSession()
	if underlying == nil {
		n.observing.Delete(escrowID)
		return
	}
	underlying.SetDiffObserver(func(diff types.Diff) {
		for _, tx := range diff.Txs {
			switch validation, timeout := tx.GetValidation(), tx.GetTimeoutInference(); {
			case validation != nil:
				n.report(n.service.Book.RecordValidation(escrowID, validation.ValidatorSlot))
				if !validation.Valid {
					n.report(n.service.Book.RecordInvalidVerdict(escrowID, validation.InferenceId))
				}
			case timeout != nil:
				n.report(n.service.Book.RecordAppliedTimeout(escrowID, timeout.InferenceId))
			}
		}
	})
}

func (n *Recorder) SetCapability(lookup accounting.CapabilityFunc) {
	if n != nil && lookup != nil {
		n.capability.Store(&lookup)
	}
}

func (n *Recorder) hostCapability(participant, model string) accounting.HostCapability {
	if n == nil {
		return accounting.HostCapability{}
	}
	if lookup := n.capability.Load(); lookup != nil {
		return (*lookup)(participant, model)
	}
	return accounting.HostCapability{}
}

func Open(settings config.NonceAccounting, storageDir string, epochs EpochSource, now func() time.Time) *Recorder {
	if !settings.Enabled {
		return nil
	}
	ledger := &Recorder{epochs: epochs}
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
			Handler:           accounting.NewHandler(service.Book, ledger.currentEpoch, ledger.hostCapability),
			ReadHeaderTimeout: nonceAccountingReadHeaderTimeout,
		}
	}
	return ledger
}

func (n *Recorder) currentEpoch(context.Context) (uint64, error) {
	if n == nil || n.epochs == nil {
		return 0, errors.New("chain snapshot is unavailable")
	}
	snapshot := n.epochs.Snapshot()
	if snapshot.EpochIndex == 0 {
		return 0, errors.New("current epoch is not known yet")
	}
	return snapshot.EpochIndex, nil
}

func (n *Recorder) Book() *accounting.Book {
	if n == nil || n.service == nil {
		return nil
	}
	return n.service.Book
}

func (n *Recorder) ResetEpoch(epoch uint64) (int, error) {
	if n == nil {
		return 0, nil
	}
	return n.service.ResetEpoch(epoch)
}

func (n *Recorder) Collectors() []prometheus.Collector {
	if n == nil {
		return nil
	}
	return []prometheus.Collector{accounting.NewCollector(n.service.Book)}
}

func (n *Recorder) Start(ctx context.Context, escrows EscrowSource) {
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

func (n *Recorder) sweepUntil(ctx context.Context, escrows EscrowSource) {
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

func (n *Recorder) sweep(ctx context.Context, escrows EscrowSource) {
	// The epoch stamped here is the one the escrow was first seen in. See README.md, "The judgements it does make".
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
		n.report(n.service.Book.ObserveChallenges(state.ID, openChallenges(escrowState)))
		n.watchDiffs(state.ID, session)
	}
	for _, escrowID := range n.service.Book.EscrowIDs() {
		if _, still := published[escrowID]; !still {
			n.service.Book.RetireEscrow(escrowID)
			n.observing.Delete(escrowID)
		}
	}
}

func (n *Recorder) reconcileFinished(escrowID string, session registry.EscrowSession) {
	unfinished := n.service.Book.UnfinishedNonces(escrowID)
	finished := make([]uint64, 0, len(unfinished))
	for _, nonce := range unfinished {
		if session.UserSession().IsNonceFinished(nonce) {
			finished = append(finished, nonce)
		}
	}
	n.report(n.service.Book.MarkFinished(escrowID, finished))
}

func (n *Recorder) RecordGhost(escrowID string, nonce uint64, reason string) {
	if n == nil || nonce == 0 {
		return
	}
	n.report(n.service.Book.RecordGhost(escrowID, nonce, reason))
}

func (n *Recorder) RecordRace(outcome engine.RaceOutcome) {
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
			Nonce:           attempt.Nonce,
			RequestID:       outcome.RequestID,
			Sent:            !attempt.SendTime.IsZero(),
			Acknowledged:    !attempt.ReceiptTime.IsZero(),
			Finished:        attempt.NonceFinished,
			Usage:           usageOf(outcome, attempt),
			Terminal:        terminalFor(outcome, attempt),
			Phase:           phase,
			SlowReceipt:     slowReceipt(attempt),
			SlowChunk:       attempt.MaxChunkGap > accounting.SlowChunkGap,
			ClockDrifted:    clockDrifted(attempt),
			SlowDecode:      engine.TimePerOutputToken(attempt) > accounting.SlowDecode,
			LogprobsDecoded: attempt.LogprobsDecoded,
		})
	}
	n.report(n.service.Book.RecordRace(outcome.EscrowID, attempts))
}

// slowReceipt needs both stamps: a missing receipt is a refusal already counted, not a second failure.
func slowReceipt(attempt engine.AttemptOutcome) bool {
	if attempt.SendTime.IsZero() || attempt.ReceiptTime.IsZero() {
		return false
	}
	return attempt.ReceiptTime.Sub(attempt.SendTime) > accounting.SlowReceipt
}

// clockDrifted reads the offset in either direction; both directions break a deadline. See README.md.
func clockDrifted(attempt engine.AttemptOutcome) bool {
	offset, measured := engine.ClockOffset(attempt)
	return measured && (offset > accounting.ClockDrift || offset < -accounting.ClockDrift)
}

func (n *Recorder) RecordTimeout(event engine.TimeoutEvent) {
	if n != nil {
		n.report(n.service.Book.RecordTimeout(event.EscrowID, event.Nonce, event.Kind, event.Action, event.Reason))
	}
}

// A winner crowned after its client left needs its own terminal, or that population is unfindable.
func terminalFor(outcome engine.RaceOutcome, attempt engine.AttemptOutcome) string {
	if outcome.Lifecycle.ClientGone && outcome.IsWinner(attempt) {
		return accounting.TerminalClientGone
	}
	return attempt.Terminal.String()
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

func (n *Recorder) report(err error) {
	if err != nil && !errors.Is(err, accounting.ErrUnknownEscrow) {
		logging.Warn("nonce accounting refused a fact", "error", err)
	}
}

func (n *Recorder) Close() error {
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

func openChallenges(escrowState types.EscrowState) map[uint32]uint64 {
	open := make(map[uint32]uint64)
	for _, record := range escrowState.Inferences {
		if record != nil && record.Status == types.StatusChallenged {
			open[record.ExecutorSlot]++
		}
	}
	return open
}
