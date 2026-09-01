package api

import (
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

// admission is the pre-queue chain check. See README.md, "What the boundary hands the engine".
func admission(snapshot chain.PhaseSnapshot, modes config.Modes, now time.Time, maxAgeSeconds int64) error {
	if !snapshot.RequestsBlocked || modes.PoCMode == config.PoCModeRelaxed {
		if stale := staleness(snapshot, now, maxAgeSeconds); stale != nil {
			return stale
		}
		return nil
	}
	return &BlockedError{
		Reason:     snapshot.BlockReason,
		Phase:      snapshot.EpochPhase,
		Confirming: snapshot.ConfirmationPoCPhase,
	}
}

func staleness(snapshot chain.PhaseSnapshot, now time.Time, maxAgeSeconds int64) error {
	if maxAgeSeconds <= 0 {
		return nil
	}
	limit := time.Duration(maxAgeSeconds) * time.Second
	if snapshot.LastHealthyAt.IsZero() {
		return &ChainStaleError{Age: limit}
	}
	if age := now.Sub(snapshot.LastHealthyAt); age >= limit {
		return &ChainStaleError{Age: age}
	}
	return nil
}
