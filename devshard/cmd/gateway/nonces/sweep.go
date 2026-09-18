package nonces

import (
	"context"
	"errors"
	"net/http"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/logging"
	"devshard/types"
)

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
