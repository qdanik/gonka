package journal

import "devshard/cmd/gateway/internal/logkey"

// EscrowServing renders an escrow published for routing. See README.md, "Lifecycle lines outside a race".
func (j *Journal) EscrowServing(escrowID, model string) {
	j.emitLine(KindEscrowTransition, func(lines logSink) {
		lines.Info("escrow serving", logkey.Escrow, escrowID, logkey.Model, model)
	})
}

// EscrowRetired renders an idle escrow taken out of routing.
func (j *Journal) EscrowRetired(escrowID string) {
	j.emitLine(KindEscrowTransition, func(lines logSink) {
		lines.Info("escrow retired", logkey.Escrow, escrowID)
	})
}

// EscrowRetiredDraining renders an escrow taken out of routing with requests still running on it.
func (j *Journal) EscrowRetiredDraining(escrowID string, inFlight int64) {
	j.emitLine(KindEscrowTransition, func(lines logSink) {
		lines.Info("escrow retired, draining", logkey.Escrow, escrowID, logkey.InFlight, inFlight)
	})
}

// DrainingEscrowClosed renders a drained escrow's close, or the failure that keeps its storage held.
func (j *Journal) DrainingEscrowClosed(escrowID string, closeErr error) {
	j.emitLine(KindEscrowTransition, func(lines logSink) {
		if closeErr != nil {
			lines.Error("draining escrow failed to close, its storage stays held", logkey.Escrow, escrowID, logkey.Error, closeErr)
			return
		}
		lines.Info("draining escrow closed", logkey.Escrow, escrowID)
	})
}

// SettlementUnverifiable renders a settlement payload the chain would refuse, built anyway.
func (j *Journal) SettlementUnverifiable(escrowID string, nonce uint64, unverifiable error) {
	j.emitLine(KindEscrowTransition, func(lines logSink) {
		lines.Warn("settlement signatures did not verify",
			logkey.Escrow, escrowID, logkey.Nonce, nonce, logkey.Error, unverifiable)
	})
}

// EscrowUnservable renders a stored devshard boot marks inactive because the chain or the environment cannot serve it.
func (j *Journal) EscrowUnservable(escrowID string, err error) {
	j.emitLine(KindEscrowTransition, func(lines logSink) {
		lines.Warn("devshard cannot be served, marking inactive", logkey.Escrow, escrowID, logkey.Error, err)
	})
}
