package registry

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"devshard/cmd/gateway/chain"
	"devshard/state"
	"devshard/types"
)

// Finalize satisfies escrow.SettlementSource. Finalizing collects host signatures, so a non-resident
// escrow is rehydrated with a serving session rather than a read-only one.
func (r *Registry) Finalize(ctx context.Context, escrowID string) error {
	if session, held := r.SettlementSession(escrowID); held {
		return finalize(ctx, session)
	}
	if r.servingSessions == nil {
		return fmt.Errorf("escrow %s: no serving session factory", escrowID)
	}
	session, err := r.servingSessions(ctx, escrowID)
	if err != nil {
		return fmt.Errorf("rehydrating serving session for escrow %s: %w", escrowID, err)
	}
	return errors.Join(finalize(ctx, session), session.Close())
}

// BuildSettlement satisfies escrow.SettlementSource. A non-resident escrow is rehydrated read-only:
// the payload comes entirely from local storage, so it needs neither chain access nor host clients.
func (r *Registry) BuildSettlement(ctx context.Context, escrowID string) (chain.SettlementInput, error) {
	if session, held := r.SettlementSession(escrowID); held {
		return buildSettlement(escrowID, session)
	}
	if r.readOnlySessions == nil {
		return chain.SettlementInput{}, fmt.Errorf("escrow %s: no read-only session factory", escrowID)
	}
	session, err := r.readOnlySessions(ctx, escrowID)
	if err != nil {
		return chain.SettlementInput{}, fmt.Errorf("rehydrating read-only session for escrow %s: %w", escrowID, err)
	}
	input, buildErr := buildSettlement(escrowID, session)
	return input, errors.Join(buildErr, session.Close())
}

func finalize(ctx context.Context, session EscrowSession) error {
	if session.Phase() == types.PhaseSettlement {
		return nil
	}
	return session.Finalize(ctx)
}

func buildSettlement(escrowID string, session EscrowSession) (chain.SettlementInput, error) {
	numericID, err := strconv.ParseUint(escrowID, 10, 64)
	if err != nil {
		return chain.SettlementInput{}, fmt.Errorf("escrow id %q is not numeric: %w", escrowID, err)
	}
	nonce := session.Nonce()
	payload, err := state.BuildSettlement(escrowID, session.SnapshotState(), session.Signatures()[nonce], nonce)
	if err != nil {
		return chain.SettlementInput{}, fmt.Errorf("building settlement for escrow %s: %w", escrowID, err)
	}
	hostStats := statsPerPresentSlot(payload.HostStats)
	hostStatsHash, err := state.ComputeHostStatsHash(hostStats)
	if err != nil {
		return chain.SettlementInput{}, fmt.Errorf("hashing host stats for escrow %s: %w", escrowID, err)
	}
	return chain.SettlementInput{
		EscrowID: numericID,
		StateRoot: state.ComputeStateRootFromRestHash(
			hostStatsHash, payload.RestHash, payload.Fees, types.PhaseSettlement, payload.StateRootAndProtocolVersion),
		Nonce:     payload.Nonce,
		RestHash:  payload.RestHash,
		HostStats: settlementHostStats(hostStats),
		SlotSigs:  settlementSlotSigs(payload.Signatures),
		Fees:      payload.Fees,
		Version:   payload.StateRootAndProtocolVersion,
	}, nil
}

func statsPerPresentSlot(perSlot map[uint32]*types.HostStats) map[uint32]*types.HostStats {
	present := make(map[uint32]*types.HostStats, len(perSlot))
	for slotID, hostStats := range perSlot {
		if hostStats == nil {
			continue
		}
		present[slotID] = hostStats
	}
	return present
}

func settlementHostStats(perSlot map[uint32]*types.HostStats) []chain.SettlementHostStat {
	stats := make([]chain.SettlementHostStat, 0, len(perSlot))
	for _, slotID := range sortedKeys(perSlot) {
		hostStats := perSlot[slotID]
		stats = append(stats, chain.SettlementHostStat{
			SlotID:               uint64(slotID),
			Missed:               uint64(hostStats.Missed),
			Invalid:              uint64(hostStats.Invalid),
			Cost:                 hostStats.Cost,
			RequiredValidations:  uint64(hostStats.RequiredValidations),
			CompletedValidations: uint64(hostStats.CompletedValidations),
		})
	}
	return stats
}

func settlementSlotSigs(perSlot map[uint32][]byte) []chain.SettlementSlotSig {
	sigs := make([]chain.SettlementSlotSig, 0, len(perSlot))
	for _, slotID := range sortedKeys(perSlot) {
		sigs = append(sigs, chain.SettlementSlotSig{SlotID: uint64(slotID), Signature: perSlot[slotID]})
	}
	return sigs
}
