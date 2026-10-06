package escrow

import (
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
)

func effectiveEpoch(snapshot chain.PhaseSnapshot) uint64 {
	if snapshot.EffectiveEpochIndex > 0 {
		return snapshot.EffectiveEpochIndex
	}
	return snapshot.EpochIndex
}

// ownEpochOver compares the effective epoch's lower bound with the chain epoch's upper bound, the label while unresolved.
func ownEpochOver(record store.DevshardRecord, snapshot chain.PhaseSnapshot) bool {
	chainEpoch := record.ChainEpoch
	if chainEpoch == 0 && record.RotationEpoch > 0 {
		chainEpoch = uint64(record.RotationEpoch)
	}
	effective := snapshot.EffectiveEpochIndex
	if effective == 0 && snapshot.EpochIndex > 0 {
		effective = snapshot.EpochIndex - 1
	}
	return chainEpoch > 0 && effective > chainEpoch
}

func bridgeLabel(snapshot chain.PhaseSnapshot) uint64 {
	return effectiveEpoch(snapshot) + 1
}
