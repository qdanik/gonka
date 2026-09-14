package journal

import (
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/scheduler"
)

// renderBurn names the nonce only when the session committed one, and the request only when the burn had one. See README.md, "Absent subjects are omitted".
func renderBurn(lines logSink, escrowID string, burned scheduler.Burn) {
	fields := make([]any, 0, 10)
	fields = append(fields, logkey.Escrow, escrowID)
	if burned.Nonce != 0 {
		fields = append(fields, logkey.Nonce, burned.Nonce)
	}
	fields = append(fields, logkey.Host, logkey.ShortHost(burned.Participant), logkey.Reason, burned.Reason)
	if burned.RequestID != "" {
		fields = append(fields, logkey.BurnedDuringRequest, burned.RequestID)
	}
	lines.Warn("nonce burned for nobody", fields...)
}

func renderBurnBudgetExhausted(lines logSink, escrowID string) {
	lines.Warn("escrow stopped burning nonces at its budget", logkey.Escrow, escrowID)
}

// renderTimeoutVote reports a race vote that never reached the chain. See docs/race.md, "Timeout votes".
func renderTimeoutVote(lines logSink, vote *engine.TimeoutEvent) {
	if vote.Action != engine.TimeoutActionFailed || vote.Reason == engine.TimeoutReasonEscrowGone {
		return
	}
	lines.Warn("timeout vote failed",
		logkey.Escrow, vote.EscrowID, logkey.Nonce, vote.Nonce,
		logkey.Host, logkey.ShortHost(vote.Participant), logkey.Model, vote.Model,
		logkey.Kind, vote.Kind, logkey.Reason, vote.Reason)
}

// renderProbeRefused names the warmup nonce the ledger refused; nothing else records it.
func renderProbeRefused(lines logSink, escrowID string, nonce uint64, err error) {
	lines.Warn("escrow warmup could not settle its nonce", logkey.Escrow, escrowID, logkey.Nonce, nonce, logkey.Error, err)
}
