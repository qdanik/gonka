package scheduler

import (
	"fmt"
	"math"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

// fallbackNonceCeiling applies until governance max_nonce has been fetched: the fixed ceiling the gateway
// ran on before the param existed. See gateway-routing-and-nonces.md, "Picking an escrow".
const fallbackNonceCeiling uint64 = 19_800

func (s *Scheduler) pickEscrow(profile RequestProfile, snapshot chain.PhaseSnapshot) (Escrow, error) {
	candidates := s.escrows.Candidates(profile.Model)

	if profile.Escrow != "" {
		for _, candidate := range candidates {
			if candidate.ID == profile.Escrow {
				return candidate, nil
			}
		}
		return Escrow{}, fmt.Errorf("escrow %q for model %q: %w", profile.Escrow, profile.Model, ErrEscrowGone)
	}

	// Ties hold indices rather than candidates: an index does not escape the way a returned Escrow does,
	// so the common case picks without touching the heap at all.
	bestScore := math.Inf(1)
	var tied []int
	for index, candidate := range candidates {
		if atNonceCap(candidate, snapshot.MaxNonce) {
			// Routing only declines it; replacing it belongs to the rotation lifecycle, which
			// otherwise never learns and lets the escrow drain silently into ErrNoEscrowCapacity.
			if s.onEscrowExhausted != nil {
				s.onEscrowExhausted(candidate.ID)
			}
			continue
		}
		score := loadScore(candidate.ActiveUsers, s.capacity.EscrowWeight(candidate.ID, profile.Model))
		switch {
		case math.IsInf(score, 1):
			continue
		case score < bestScore:
			bestScore, tied = score, append(tied[:0], index)
		case score == bestScore:
			tied = append(tied, index)
		}
	}

	switch len(tied) {
	case 0:
		return Escrow{}, ErrNoEscrowCapacity
	case 1:
		return candidates[tied[0]], nil
	default:
		return candidates[tied[int(uint64(s.tieBreak.Add(1)-1)%uint64(len(tied)))]], nil
	}
}

// loadScore is the ascending utilisation ratio; a non-positive or corrupt weight scores unusable. See
// gateway-routing-and-nonces.md, "Picking an escrow".
func loadScore(activeUsers int, weight float64) float64 {
	if weight <= 0 || math.IsNaN(weight) {
		return math.Inf(1)
	}
	return float64(activeUsers) / weight
}

func atNonceCap(candidate Escrow, maxNonce uint64) bool {
	if candidate.Session == nil {
		return false
	}
	cutoff := fallbackNonceCeiling
	if maxNonce > 0 {
		// Clamp, never wrap: a cap wrapping to 0 makes MaxActiveNonce return ^uint64(0), disabling the gate.
		if maxNonce > math.MaxUint32 {
			maxNonce = math.MaxUint32
		}
		cutoff = types.MaxActiveNonce(uint32(maxNonce), candidate.Session.GroupSize())
	}
	return candidate.Session.LatestNonce() >= cutoff
}
