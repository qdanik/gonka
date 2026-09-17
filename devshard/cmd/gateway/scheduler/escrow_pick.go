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

// exhaustionFallbackNonceCeiling is the one decline reason never reported, so it never leaves this package. See routing.md, "Picking an escrow".
const exhaustionFallbackNonceCeiling = "fallback_nonce_ceiling"

// nonceInFlightMargin is room left under the hosts' nonce cap for work already routed. See routing.md, "Picking an escrow".
const nonceInFlightMargin uint64 = 200

func (s *Scheduler) pickEscrow(profile RequestProfile, snapshot chain.PhaseSnapshot, queued *waiter, avoid string) (Escrow, error) {
	candidates := s.escrows.Candidates(profile.Model)
	reserveTokens := s.reserveTokens(profile)

	if profile.Escrow != "" {
		for _, candidate := range candidates {
			if candidate.ID != profile.Escrow {
				continue
			}
			// A pinned escrow is capped too: the ceiling reserves room for the finalize and settlement. See routing.md, "Picking an escrow".
			if reason := exhaustionReason(candidate, snapshot.MaxNonce, reserveTokens); reason != "" {
				s.reportExhausted(candidate.ID, reason)
				return Escrow{}, noCapacity(reason)
			}
			return candidate, nil
		}
		return Escrow{}, fmt.Errorf("escrow %q for model %q: %w", profile.Escrow, profile.Model, ErrEscrowGone)
	}
	// Read here as well as at dispatch: an escrow whose whole group it refuses can never serve.
	reachable := reachableByAllowlist(s.participantAllowlist())
	fleet := s.fleetGates(profile.Model, snapshot)
	ahead := s.queuedAhead(candidates)

	// Indices, not candidates: a returned Escrow escapes where an index does not.
	bestScore := math.Inf(1)
	var tied []int
	admitted := 0
	declined := ""
	for index, candidate := range candidates {
		if !reachable(candidate) {
			continue
		}
		admitted++
		if candidate.ID == avoid {
			continue
		}
		if reason := exhaustionReason(candidate, snapshot.MaxNonce, reserveTokens); reason != "" {
			declined = reason
			// Routing only declines; the rotation lifecycle is what replaces an exhausted escrow.
			s.reportExhausted(candidate.ID, reason)
			continue
		}
		weight := s.capacity.EscrowWeight(candidate.ID, profile.Model)
		if unusableWeight(weight) {
			continue
		}
		forecast := expectedBurns(candidate, fleet.forEscrow(s.stateBlocked(candidate.ID)), queued, ahead[index])
		score := float64(candidate.ActiveUsers+forecast) / weight
		switch {
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

// reportExhausted passes over the fallback ceiling: it is not the hosts' cap, and a reported escrow is parked for good. See routing.md, "Picking an escrow".
func (s *Scheduler) reportExhausted(escrowID, reason string) {
	if s.onEscrowExhausted == nil || reason == exhaustionFallbackNonceCeiling {
		return
	}
	s.onEscrowExhausted(escrowID, reason)
}

// noCapacity carries why the last candidate was declined, so running dry is not read as a model nobody serves.
func noCapacity(reason string) error {
	if reason == exhaustionBalanceFloor {
		return fmt.Errorf("%w: %w", ErrNoEscrowCapacity, types.ErrInsufficientBalance)
	}
	return ErrNoEscrowCapacity
}

// expectedBurns is how many nonces this escrow spends on nobody before one binds to a host that can take this request. See routing.md, "Pricing an escrow by the burns it will cost".
func expectedBurns(candidate Escrow, gates availability, queued *waiter, queuedAhead uint64) int {
	if candidate.Session == nil {
		return 0
	}
	slots := candidate.Session.SlotParticipants()
	if len(slots) == 0 {
		return 0
	}
	cursor := int((candidate.Session.LatestNonce() + 1 + queuedAhead) % uint64(len(slots)))
	for step := range slots {
		if gates.blocks(slots[(cursor+step)%len(slots)], queued) == blockNone {
			return step
		}
	}
	return len(slots)
}

// unusableWeight holds for a weight no ratio can be taken against. See routing.md, "Picking an escrow".
func unusableWeight(weight float64) bool {
	return weight <= 0 || math.IsNaN(weight)
}

// nonceCeilingReason names the ceiling an escrow has reached, and is empty below it.
func nonceCeilingReason(candidate Escrow, maxNonce uint64) string {
	if candidate.Session == nil {
		return ""
	}
	cutoff, reason := fallbackNonceCeiling, exhaustionFallbackNonceCeiling
	if maxNonce > 0 {
		// Clamp, never wrap: a cap wrapping to 0 makes MaxActiveNonce return ^uint64(0), disabling the gate.
		if maxNonce > math.MaxUint32 {
			maxNonce = math.MaxUint32
		}
		cutoff = types.MaxActiveNonce(uint32(maxNonce), candidate.Session.GroupSize())
		cutoff -= min(nonceInFlightMargin, cutoff/2)
		reason = exhaustionNonceCap
	}
	if candidate.Session.LatestNonce() < cutoff {
		return ""
	}
	return reason
}

// exhaustionReason is empty while the escrow may still be picked; the fallback ceiling ranks last, so an escrow past it is still reported when its balance floor catches it. See routing.md, "Picking an escrow".
func exhaustionReason(candidate Escrow, maxNonce uint64, reserveTokens uint64) string {
	ceilingReason := nonceCeilingReason(candidate, maxNonce)
	switch {
	case ceilingReason == exhaustionNonceCap:
		return exhaustionNonceCap
	case belowBalanceFloor(candidate, reserveTokens):
		return exhaustionBalanceFloor
	}
	return ceilingReason
}

// belowBalanceFloor prices the reserve the way the chain does, (input_length_bytes + max_tokens_cap) * token_price.
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
