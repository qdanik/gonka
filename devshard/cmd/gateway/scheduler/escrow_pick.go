package scheduler

import (
	"fmt"
	"math"
	"slices"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

// fallbackNonceCeiling applies until governance max_nonce has been fetched: the fixed ceiling the gateway
// ran on before the param existed. See gateway-routing-and-nonces.md, "Picking an escrow".
const fallbackNonceCeiling uint64 = 19_800

func (s *Scheduler) pickEscrow(profile RequestProfile, snapshot chain.PhaseSnapshot) (Escrow, error) {
	candidates := s.escrows.Candidates(profile.Model)
	reserveTokens := s.reserveTokens(profile)

	if profile.Escrow != "" {
		for _, candidate := range candidates {
			if candidate.ID != profile.Escrow {
				continue
			}
			// The cap is checked here too: a client picks this escrow by id, and the nonce ceiling is
			// what reserves room for the finalize and settlement that follow. See
			// gateway-routing-and-nonces.md, "Picking an escrow".
			if reason := exhaustionReason(candidate, snapshot.MaxNonce, reserveTokens); reason != "" {
				if s.onEscrowExhausted != nil {
					s.onEscrowExhausted(candidate.ID, reason)
				}
				return Escrow{}, noCapacity(reason)
			}
			return candidate, nil
		}
		return Escrow{}, fmt.Errorf("escrow %q for model %q: %w", profile.Escrow, profile.Model, ErrEscrowGone)
	}
	// The allowlist is read here as well as at dispatch: an escrow whose whole group it refuses can
	// never serve, and picking it by load alone spends the request on a group holding nobody.
	reachable := reachableByAllowlist(s.participantAllowlist())

	// Ties hold indices rather than candidates: an index does not escape the way a returned Escrow does,
	// so the common case picks without touching the heap at all.
	bestScore := math.Inf(1)
	var tied []int
	admitted := 0
	declined := ""
	for index, candidate := range candidates {
		if !reachable(candidate) {
			continue
		}
		admitted++
		if reason := exhaustionReason(candidate, snapshot.MaxNonce, reserveTokens); reason != "" {
			declined = reason
			// Routing only declines it; replacing it belongs to the rotation lifecycle, which
			// otherwise never learns and lets the escrow drain silently into ErrNoEscrowCapacity.
			if s.onEscrowExhausted != nil {
				s.onEscrowExhausted(candidate.ID, reason)
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

	if admitted == 0 && len(candidates) > 0 {
		return Escrow{}, ErrAllowlistUnreachable
	}

	switch len(tied) {
	case 0:
		return Escrow{}, noCapacity(declined)
	case 1:
		return candidates[tied[0]], nil
	default:
		return candidates[tied[int(uint64(s.tieBreak.Add(1)-1)%uint64(len(tied)))]], nil
	}
}

// noCapacity carries why the last candidate was declined, so an escrow the floor caught while it could
// still refuse cleanly is accounted as the running dry it is, not as a model nobody serves.
func noCapacity(reason string) error {
	if reason == "balance_floor" {
		return fmt.Errorf("%w: %w", ErrNoEscrowCapacity, types.ErrInsufficientBalance)
	}
	return ErrNoEscrowCapacity
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

// exhaustionReason is empty while the escrow may still be picked.
func exhaustionReason(candidate Escrow, maxNonce uint64, reserveTokens uint64) string {
	switch {
	case atNonceCap(candidate, maxNonce):
		return "nonce_cap"
	case belowBalanceFloor(candidate, reserveTokens):
		return "balance_floor"
	}
	return ""
}

// belowBalanceFloor prices the reserve the way the chain does, (input+max_tokens)*token_price, and asks
// whether the escrow still covers everything in flight plus this arrival.
func belowBalanceFloor(candidate Escrow, reserveTokens uint64) bool {
	if candidate.Session == nil || reserveTokens == 0 {
		return false
	}
	reserve, ok := safeMul(reserveTokens, candidate.Session.TokenPrice())
	if !ok {
		return true
	}
	floor, ok := safeMul(reserve, uint64(candidate.ActiveUsers+1))
	if !ok {
		return true
	}
	return candidate.Session.Balance() < floor
}

// safeMul reports the product only when it did not wrap: a price this escrow cannot afford must not read
// as an affordable small one.
func safeMul(left, right uint64) (uint64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	product := left * right
	return product, product/left == right
}

// reachableByAllowlist is built once per pick rather than per candidate, and an empty allowlist skips
// the walk entirely: narrowing exists only where an operator asked for it.
func reachableByAllowlist(allowlist []string) func(Escrow) bool {
	if len(allowlist) == 0 {
		return func(Escrow) bool { return true }
	}
	allowed := allowedParticipants(allowlist)
	return func(candidate Escrow) bool {
		return candidate.Session != nil &&
			slices.ContainsFunc(candidate.Session.ParticipantKeys(), allowed)
	}
}

func (s *Scheduler) participantAllowlist() []string {
	if s.settings == nil {
		return nil
	}
	return s.settings.Load().Scheduler.ParticipantAllowlist
}
