package journal

import (
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/scheduler"
)

// renderBurn names the nonce in the line because no metric may carry it.
func renderBurn(lines logSink, escrowID string, burned scheduler.Burn) {
	lines.Warn("nonce burned for nobody", logkey.Escrow, escrowID, logkey.Nonce, burned.Nonce,
		logkey.Host, logkey.ShortHost(burned.Participant), logkey.Reason, burned.Reason)
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
