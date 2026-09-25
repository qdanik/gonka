package scheduler

import (
	"fmt"
	"math"

	"devshard/cmd/gateway/chain"
	"devshard/types"
)

// fallbackNonceCeiling applies until governance max_nonce has been fetched. See routing.md, "Picking an escrow".
const fallbackNonceCeiling uint64 = 19_800

// nonceInFlightMargin is room left under the hosts' nonce cap for work already routed. See routing.md, "Picking an escrow".
const nonceInFlightMargin uint64 = 200

// avoidReason is why one round of a pick steps over an escrow; only a busy one keeps the reserves shut.
type avoidReason int

const (
	avoidedOutOfFunds avoidReason = iota + 1
	avoidedHostsBusy
)

type avoidedEscrows map[string]avoidReason

func (s *Scheduler) pickEscrow(profile RequestProfile, snapshot chain.PhaseSnapshot, queued *waiter, avoided avoidedEscrows) (Escrow, error) {
	candidates := s.escrows.Candidates(profile.Model)
	retirement, request := s.retirementReserve(), requestReserve(profile)

	if profile.Escrow != "" {
		for _, candidate := range candidates {
			if candidate.ID != profile.Escrow {
				continue
			}
			// A pinned escrow is capped too: the ceiling reserves room for the finalize and settlement. See routing.md, "Picking an escrow".
			if reason := exhaustionReason(candidate, snapshot.MaxNonce, retirement); reason != "" {
				s.reportExhausted(candidate.ID, reason)
				return Escrow{}, noCapacity(reason)
			}
			if belowBalanceFloor(candidate, request) {
				return Escrow{}, noCapacity(ExhaustionBalanceFloor)
			}
			return candidate, nil
		}
		return Escrow{}, fmt.Errorf("escrow %q for model %q: %w", profile.Escrow, profile.Model, ErrEscrowGone)
	}
	// Read here as well as at dispatch: an escrow whose whole group it refuses can never serve.
	ranking := escrowRanking{
		profile: profile, snapshot: snapshot, queued: queued, avoided: avoided,
		retirement: retirement, request: request,
		reachable: reachableByAllowlist(s.participantAllowlist(), s.unthrottledParticipants()),
		fleet:     s.fleetGates(profile.Model, snapshot),
		ahead:     s.queuedAhead(candidates),
	}
	regular := s.rankCandidates(candidates, ranking, false)
	picked, admitted, declined := regular.picked, regular.admitted, regular.declined
	if picked < 0 && regular.passedOverForHosts == 0 {
		reserve := s.rankCandidates(candidates, ranking, true)
		picked, admitted = reserve.picked, admitted+reserve.admitted
		if reserve.declined != "" {
			declined = reserve.declined
		}
	}

	if admitted == 0 && len(candidates) > 0 {
		return Escrow{}, ErrAllowlistUnreachable
	}
	if picked < 0 {
		return Escrow{}, noCapacity(declined)
	}
	return candidates[picked], nil
}

// escrowRanking is what one pick reads for every candidate it ranks.
type escrowRanking struct {
	profile    RequestProfile
	snapshot   chain.PhaseSnapshot
	queued     *waiter
	avoided    avoidedEscrows
	retirement uint64
	request    uint64
	reachable  func(Escrow) bool
	fleet      availability
	ahead      []uint64
}

type rankedPick struct {
	picked             int
	admitted           int
	declined           ExhaustionReason
	passedOverForHosts int
}

// rankCandidates scores one tier, regulars or reserves, and counts the ones it passed over for their hosts. See routing.md, "A reserve escrow".
func (s *Scheduler) rankCandidates(candidates []Escrow, ranking escrowRanking, reserveTier bool) rankedPick {
	bestScore := math.Inf(1)
	var tied []int
	result := rankedPick{picked: -1}
	for index, candidate := range candidates {
		if candidate.IsReserve != reserveTier {
			continue
		}
		if !ranking.reachable(candidate) {
			result.passedOverForHosts++
			continue
		}
		result.admitted++
		if reason, avoided := ranking.avoided[candidate.ID]; avoided {
			if reason == avoidedHostsBusy {
				result.passedOverForHosts++
			}
			continue
		}
		if reason := exhaustionReason(candidate, ranking.snapshot.MaxNonce, ranking.retirement); reason != "" {
			result.declined = reason
			// Routing only declines; the rotation lifecycle is what replaces an exhausted escrow.
			s.reportExhausted(candidate.ID, reason)
			continue
		}
		if belowBalanceFloor(candidate, ranking.request) {
			result.declined = ExhaustionBalanceFloor
			continue
		}
		weight := s.capacity.EscrowWeight(candidate.ID, ranking.profile.Model)
		if unusableWeight(weight) {
			result.passedOverForHosts++
			continue
		}
		forecast := expectedBurns(candidate, ranking.fleet.forEscrow(s.stateBlocked(candidate.ID)), ranking.queued, ranking.ahead[index])
		score := float64(candidate.ActiveUsers+forecast) / weight
		switch {
		case score < bestScore:
			bestScore, tied = score, append(tied[:0], index)
		case score == bestScore:
			tied = append(tied, index)
		}
	}

	switch len(tied) {
	case 0:
	case 1:
		result.picked = tied[0]
	default:
		result.picked = tied[int(uint64(s.tieBreak.Add(1)-1)%uint64(len(tied)))]
	}
	return result
}

func (s *Scheduler) reportReserveTaken(escrowID string) {
	if s.onReserveTaken != nil {
		s.onReserveTaken(escrowID)
	}
}

// reportExhausted passes over the fallback ceiling: it is not the hosts' cap, and a reported escrow is parked or put on hold. See routing.md, "Picking an escrow".
func (s *Scheduler) reportExhausted(escrowID string, reason ExhaustionReason) {
	if s.onEscrowExhausted == nil || reason == exhaustionFallbackNonceCeiling {
		return
	}
	s.onEscrowExhausted(escrowID, reason)
}

// noCapacity carries why the last candidate was declined, so running dry is not read as a model nobody serves.
func noCapacity(reason ExhaustionReason) error {
	if reason == ExhaustionBalanceFloor {
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
func nonceCeilingReason(candidate Escrow, maxNonce uint64) ExhaustionReason {
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
		reason = ExhaustionNonceCap
	}
	if candidate.Session.LatestNonce() < cutoff {
		return ""
	}
	return reason
}

// exhaustionReason is empty while the escrow may still be picked; the fallback ceiling ranks last, so an escrow past it is still reported when its balance floor catches it. See routing.md, "Picking an escrow".
func exhaustionReason(candidate Escrow, maxNonce uint64, reserveTokens uint64) ExhaustionReason {
	ceilingReason := nonceCeilingReason(candidate, maxNonce)
	switch {
	case ceilingReason == ExhaustionNonceCap:
		return ExhaustionNonceCap
	case belowBalanceFloor(candidate, reserveTokens):
		return ExhaustionBalanceFloor
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

// ResumeReadiness prices an escrow on hold the way a pick would, with headroom so it does not flap at the floor; nonceSpent means it can never serve again. See routing.md, "An escrow on hold".
func (s *Scheduler) ResumeReadiness(candidate Escrow, answers uint64) (ready, nonceSpent bool) {
	reserve := s.retirementReserve()
	maxNonce := s.snapshots.Snapshot().MaxNonce
	if nonceCeilingReason(candidate, maxNonce) == ExhaustionNonceCap {
		return false, true
	}
	if candidate.Session == nil || reserve == 0 || exhaustionReason(candidate, maxNonce, reserve) != "" {
		return false, false
	}
	price, priced := safeMul(reserve, candidate.Session.TokenPrice())
	if !priced {
		return false, false
	}
	floor, priced := safeMul(price, answers)
	if !priced {
		return false, false
	}
	return candidate.Session.Balance() >= floor, false
}

// safeMul reports the product only when it did not wrap: an unaffordable price must not read as a small one.
func safeMul(left, right uint64) (uint64, bool) {
	if left == 0 || right == 0 {
		return 0, true
	}
	product := left * right
	return product, product/left == right
}
