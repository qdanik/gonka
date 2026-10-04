package escrow

import "devshard/cmd/gateway/chain"

func effectiveEpoch(snapshot chain.PhaseSnapshot) uint64 {
	if snapshot.EffectiveEpochIndex > 0 {
		return snapshot.EffectiveEpochIndex
	}
	return snapshot.EpochIndex
}

func bridgeLabel(snapshot chain.PhaseSnapshot) uint64 {
	return effectiveEpoch(snapshot) + 1
}
