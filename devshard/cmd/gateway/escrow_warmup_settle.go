package main

import (
	"context"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
	"devshard/user"
)

const (
	warmupTimeoutKind     = "refused"
	warmupTimeoutNoPoster = "no_poster"
)

type warmupTimeouts interface {
	RecordTimeout(event engine.TimeoutEvent)
}

// settleRefusedProbe resolves the nonce the warmup spent: it is committed outside the scheduler, so
// nothing else ever will, and the escrow pays its reserve while the host escapes the miss.
func (w *escrowWarmup) settleRefusedProbe(ctx context.Context, escrowID, model string, params user.InferenceParams, nonce uint64) {
	if w.posters == nil {
		return
	}
	event := engine.TimeoutEvent{
		EscrowID: escrowID,
		Model:    model,
		Nonce:    nonce,
		Kind:     warmupTimeoutKind,
		Action:   engine.TimeoutActionStarted,
	}

	poster, resolved := w.posters(escrowID, params)
	if !resolved {
		w.recordTimeout(engine.TimeoutEvent{
			EscrowID: escrowID, Model: model, Nonce: nonce, Kind: warmupTimeoutKind,
			Action: engine.TimeoutActionSkipped, Reason: warmupTimeoutNoPoster,
		})
		return
	}
	w.recordTimeout(event)

	posted := event
	vote, err := poster.SettleTimeout(ctx, nonce, time.Unix(params.StartedAt, 0))
	posted.Action, posted.Reason = engine.TimeoutOutcome(vote, err, false)
	w.recordTimeout(posted)

	logging.Info("escrow warmup voted on its refused nonce", logkey.Escrow, escrowID,
		logkey.Nonce, nonce, logkey.Action, posted.Action, logkey.Reason, posted.Reason)
}

func (w *escrowWarmup) recordTimeout(event engine.TimeoutEvent) {
	if w.timeouts != nil {
		w.timeouts.RecordTimeout(event)
	}
}
