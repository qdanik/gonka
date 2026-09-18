// Package nonces turns what the gateway did to a nonce -- the race it ran, the burn it took, the
// timeout it voted -- and what the chain later recorded about it into one ledger, and serves that ledger.
package nonces

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

// DiffJournal is satisfied by *journal.Journal; DiffComposed runs under the session lock, so it reads and appends only.
type DiffJournal interface {
	DiffComposed(escrowID string, diff *types.Diff)
}

// CreationEpochFunc resolves the chain epoch an escrow was created in. See README.md, "The judgements it does make".
type CreationEpochFunc func(ctx context.Context, escrowID string) (uint64, bool)

type Recorder struct {
	service  *accounting.Service
	listener *http.Server
	epochs   EpochSource

	capability    atomic.Pointer[accounting.CapabilityFunc]
	creationEpoch atomic.Pointer[CreationEpochFunc]

	pinnedEpochs sync.Map

	observing sync.Map
}

// watchDiffs hands every composed diff to the journal, so the book's lock is never taken under the session's. See README.md, "Boundaries".
func (n *Recorder) watchDiffs(escrowID string, session registry.EscrowSession, diffs DiffJournal) {
	if diffs == nil {
		return
	}
	if _, already := n.observing.LoadOrStore(escrowID, struct{}{}); already {
		return
	}
	underlying := session.UserSession()
	if underlying == nil {
		n.observing.Delete(escrowID)
		return
	}
	underlying.SetDiffObserver(func(diff types.Diff) { diffs.DiffComposed(escrowID, &diff) })
}

func (n *Recorder) SetCapability(lookup accounting.CapabilityFunc) {
	if n != nil && lookup != nil {
		n.capability.Store(&lookup)
	}
}

// SetCreationEpoch wires the resolver the sweep stamps escrows with. See README.md, "The judgements it does make".
func (n *Recorder) SetCreationEpoch(resolve CreationEpochFunc) {
	if n != nil && resolve != nil {
		n.creationEpoch.Store(&resolve)
	}
}

// epochOf answers the epoch an escrow was created in. See README.md, "The judgements it does make".
func (n *Recorder) epochOf(ctx context.Context, escrowID string) (uint64, bool) {
	if pinned, memoised := n.pinnedEpochs.Load(escrowID); memoised {
		return pinned.(uint64), true
	}
	resolve := n.creationEpoch.Load()
	if resolve == nil {
		return 0, false
	}
	epoch, resolved := (*resolve)(ctx, escrowID)
	if !resolved || epoch == 0 {
		return 0, false
	}
	n.pinnedEpochs.Store(escrowID, epoch)
	return epoch, true
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
	ledger.listener = &http.Server{
		Addr:              fmt.Sprintf(":%d", settings.Port),
		Handler:           accounting.NewHandler(service.Book, ledger.currentEpoch, ledger.hostCapability),
		ReadHeaderTimeout: nonceAccountingReadHeaderTimeout,
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

// Start sweeps until ctx ends; every escrow the sweep watches hands its composed diffs to diffs.
func (n *Recorder) Start(ctx context.Context, escrows EscrowSource, diffs DiffJournal) {
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
	go n.sweepUntil(ctx, escrows, diffs)
}

func (n *Recorder) sweepUntil(ctx context.Context, escrows EscrowSource, diffs DiffJournal) {
	ticker := time.NewTicker(nonceAccountingSweepInterval)
	defer ticker.Stop()
	for {
		n.sweep(ctx, escrows, diffs)
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

func (n *Recorder) sweep(ctx context.Context, escrows EscrowSource, diffs DiffJournal) {
	states := escrows.Snapshot()
	published := make(map[string]struct{}, len(states))
	for _, state := range states {
		session, routable := escrows.RoutableSession(state.ID)
		if !routable {
			continue
		}
		published[state.ID] = struct{}{}
		escrowState := session.SnapshotState()
		if epoch, known := n.epochOf(ctx, state.ID); known {
			n.report(n.service.Book.OpenEscrow(accounting.EscrowMetadata{
				EscrowID:      state.ID,
				Model:         state.Model,
				CreationEpoch: epoch,
				Slots:         escrowState.Group,
			}))
		}
		n.observeEscrowState(state.ID, escrowState)
		n.reconcileFinished(state.ID, session)
		n.watchDiffs(state.ID, session, diffs)
	}
	for _, escrowID := range n.service.Book.EscrowIDs() {
		if _, still := published[escrowID]; !still {
			n.service.Book.RetireEscrow(escrowID)
			n.observing.Delete(escrowID)
		}
	}
}

// What one sweep reads off an escrow: the watermark, the chain's per-slot stats, and every nonce's money.
func (n *Recorder) observeEscrowState(escrowID string, escrowState types.EscrowState) {
	n.report(n.service.Book.ObserveLatestNonce(escrowID, escrowState.LatestNonce))
	for slotID, stats := range escrowState.HostStats {
		if stats != nil {
			n.report(n.service.Book.ObserveHostStats(escrowID, slotID, *stats))
		}
	}
	n.report(n.service.Book.ObserveInferences(escrowID, escrowState.Inferences))
}

// The session handle is taken once: it is fixed for the sweep, and the ask behind it takes the lock a nonce commit holds.
func (n *Recorder) reconcileFinished(escrowID string, session registry.EscrowSession) {
	underlying := session.UserSession()
	if underlying == nil {
		return
	}
	unfinished := n.service.Book.UnfinishedNonces(escrowID)
	var finished []uint64
	for _, nonce := range unfinished {
		if underlying.IsNonceFinished(nonce) {
			finished = append(finished, nonce)
		}
	}
	n.report(n.service.Book.MarkFinished(escrowID, finished))
}

// EscrowRetiring takes the reading the sweep will never take again, and writes the ledger out.
// See docs/accounting.md, "Money and tokens".
func (n *Recorder) EscrowRetiring(escrowID string, session registry.EscrowSession) {
	if n == nil || n.service == nil {
		return
	}
	if session != nil {
		n.observeEscrowState(escrowID, session.SnapshotState())
		n.reconcileFinished(escrowID, session)
	}
	n.service.Book.RetireEscrow(escrowID)
	n.observing.Delete(escrowID)
	if err := n.service.Flush(); err != nil {
		logging.Error("nonce accounting snapshot failed", "error", err)
	}
}

func (n *Recorder) NonceAssigned(escrowID string, nonce uint64, requestID string) {
	if n == nil || nonce == 0 {
		return
	}
	n.report(n.service.Book.RecordAssigned(escrowID, nonce, requestID))
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
			OutputTokens:    attempt.OutputTokens(),
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

// RecordDiffFacts applies what a composed diff said, on the journal's goroutine rather than under the session lock.
func (n *Recorder) RecordDiffFacts(escrowID string, facts []accounting.DiffFact) {
	if n == nil {
		return
	}
	for _, fact := range facts {
		switch fact.Kind {
		case accounting.DiffFactValidation:
			n.report(n.service.Book.RecordValidation(escrowID, fact.ValidatorSlot))
		case accounting.DiffFactInvalidVerdict:
			n.report(n.service.Book.RecordInvalidVerdict(escrowID, fact.Nonce))
		case accounting.DiffFactAppliedTimeout:
			n.report(n.service.Book.RecordAppliedTimeout(escrowID, fact.Nonce))
		}
	}
}

// RecordProbe settles the warmup's own nonce and returns the book's refusal for the journal to name.
func (n *Recorder) RecordProbe(escrowID string, attempt accounting.Attempt) error {
	if n == nil {
		return nil
	}
	return n.service.Book.RecordRace(escrowID, []accounting.Attempt{attempt})
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
