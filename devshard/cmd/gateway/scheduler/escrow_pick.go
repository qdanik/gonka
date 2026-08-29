package scheduler

import (
	"fmt"
	"math"
	"slices"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

// fallbackNonceCeiling applies until governance max_nonce has been fetched. See routing.md, "Picking an escrow".
const fallbackNonceCeiling uint64 = 19_800

func (s *Scheduler) pickEscrow(profile RequestProfile, snapshot chain.PhaseSnapshot) (Escrow, error) {
	candidates := s.escrows.Candidates(profile.Model)
	reserveTokens := s.reserveTokens(profile)

	if profile.Escrow != "" {
		for _, candidate := range candidates {
			if candidate.ID != profile.Escrow {
				continue
			}
			// A pinned escrow is capped too: the ceiling reserves room for the finalize and settlement. See routing.md, "Picking an escrow".
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
	// Read here as well as at dispatch: an escrow whose whole group it refuses can never serve.
	reachable := reachableByAllowlist(s.participantAllowlist())

	// Indices, not candidates: an index does not escape, so the common case never touches the heap.
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
			// Routing only declines; the rotation lifecycle is what replaces an exhausted escrow.
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

// noCapacity carries why the last candidate was declined, so running dry is not read as a model nobody serves.
func noCapacity(reason string) error {
	if reason == "balance_floor" {
		return fmt.Errorf("%w: %w", ErrNoEscrowCapacity, types.ErrInsufficientBalance)
	}
	return ErrNoEscrowCapacity
}

// loadScore is the ascending utilisation ratio; a non-positive or corrupt weight scores unusable. See routing.md, "Picking an escrow".
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

// belowBalanceFloor prices the reserve the way the chain does, (input+max_tokens)*token_price.
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

// safeMul reports the product only when it did not wrap: an unaffordable price must not read as a small one.
func safeMul(left, right uint64) (uint64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	product := left * right
	return product, product/left == right
}

// reachableByAllowlist is built once per pick, and an empty allowlist skips the walk entirely.
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
