package api

import (
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

// admission is the pre-queue chain check, and the one place relaxed proof-of-compute mode is applied.
// See gateway-request-lifecycle.md, "4. Admission".
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
