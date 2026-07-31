package api

import (
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

// admission is the pre-queue chain check: it rejects before a request takes a cache lookup, a
// limiter slot or a token budget. Relaxed mode is folded in here because the observer publishes the
// chain's raw blocking state and relaxed mode is the operator's override of it, not a chain fact.
func admission(snapshot chain.PhaseSnapshot, modes config.Modes) error {
	if !snapshot.RequestsBlocked || modes.PoCMode == config.PoCModeRelaxed {
		return nil
	}
	return &BlockedError{
		Reason:     snapshot.BlockReason,
		Phase:      snapshot.EpochPhase,
		Confirming: snapshot.ConfirmationPoCPhase,
	}
}
