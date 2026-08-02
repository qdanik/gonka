package api

import (
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

// admission is the pre-queue chain check. Relaxed proof-of-compute mode is read at three further
// sites: the shutdown gate in main, and the race's own bypass and generating checks in engine.
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
