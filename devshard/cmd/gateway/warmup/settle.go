package warmup

import (
	"context"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
	"devshard/user"
)

type Timeouts interface {
	RecordTimeout(event engine.TimeoutEvent)
}

// settleRefusedProbe resolves a nonce committed outside the scheduler, which nothing else ever would.
func (w *Prober) settleRefusedProbe(ctx context.Context, escrowID, model string, params user.InferenceParams, nonce uint64) {
	if w.posters == nil {
		return
	}
	event := engine.TimeoutEvent{
		EscrowID: escrowID,
		Model:    model,
		Nonce:    nonce,
		Kind:     engine.TimeoutKindRefused,
		Action:   engine.TimeoutActionStarted,
	}

	poster, resolved := w.posters(escrowID, params)
	if !resolved {
		w.recordTimeout(engine.TimeoutEvent{
			EscrowID: escrowID, Model: model, Nonce: nonce, Kind: engine.TimeoutKindRefused,
			Action: engine.TimeoutActionSkipped, Reason: engine.TimeoutReasonNoPoster,
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

func (w *Prober) recordTimeout(event engine.TimeoutEvent) {
	if w.timeouts != nil {
		w.timeouts.RecordTimeout(event)
	}
}
