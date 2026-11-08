package scheduler

import (
	"math"
	"sync"
)

type targetFound int

const (
	targetAbsent targetFound = iota
	targetBlocked
	targetReached
)

type targetLoad struct {
	sameHost uint64
	anyHost  uint64
}

type targetLedger struct {
	mu     sync.Mutex
	byHost map[string]uint64
	total  uint64
}

func (l *targetLedger) add(host string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byHost == nil {
		l.byHost = map[string]uint64{}
	}
	l.byHost[host]++
	l.total++
}

func (l *targetLedger) remove(host string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.byHost[host] == 0 {
		return
	}
	l.byHost[host]--
	l.total--
	if l.byHost[host] == 0 {
		delete(l.byHost, host)
	}
}

func (l *targetLedger) load(host string) targetLoad {
	l.mu.Lock()
	defer l.mu.Unlock()
	return targetLoad{sameHost: l.byHost[host], anyHost: l.total}
}

type reachTier struct {
	steps int
	score float64
	tied  []int
}

func newReachTier() reachTier { return reachTier{steps: math.MaxInt, score: math.Inf(1)} }

func (tier *reachTier) offer(index, steps int, score float64) {
	switch {
	case steps < tier.steps || (steps == tier.steps && score < tier.score):
		tier.steps, tier.score, tier.tied = steps, score, append(tier.tied[:0], index)
	case steps == tier.steps && score == tier.score:
		tier.tied = append(tier.tied, index)
	}
}

func stepsToTarget(candidate Escrow, gates availability, ranking escrowRanking, index int) (int, targetFound) {
	if candidate.Session == nil {
		return 0, targetAbsent
	}
	slots := candidate.Session.SlotParticipants()
	target := ranking.profile.Target
	occurrences := 0
	for _, slot := range slots {
		if slot == target {
			occurrences++
		}
	}
	if occurrences == 0 {
		return 0, targetAbsent
	}
	if gates.blocks(target, ranking.queued) != blockNone {
		return 0, targetBlocked
	}
	load := ranking.targeted[index]
	untargetedAhead := ranking.ahead[index] - min(ranking.ahead[index], load.anyHost)
	cursor := int((candidate.Session.LatestNonce() + 1 + untargetedAhead) % uint64(len(slots)))
	wanted := int(load.sameHost % uint64(occurrences))
	cycles := int(load.sameHost / uint64(occurrences))
	for step := range slots {
		if slots[(cursor+step)%len(slots)] != target {
			continue
		}
		if wanted == 0 {
			return cycles*len(slots) + step, targetReached
		}
		wanted--
	}
	return 0, targetAbsent
}
