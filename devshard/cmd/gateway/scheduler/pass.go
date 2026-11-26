package scheduler

import (
	"sync"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/share"
)

const maxUnresolvedPasses = 100

func (outcome PassOutcome) shareKind() share.Kind {
	switch outcome {
	case PassAnswered:
		return share.ProbesAnswered
	case PassMissed:
		return share.ProbesMissed
	case PassOwed:
		return share.VotesOwed
	}
	return share.PassUnvoted
}

// PassSender delivers the committed placeholder nonce of a pass: a one-token probe where Send allows, otherwise an owed vote. See docs/proposals/gateway-weight-proportional-load.md, "A pass is a one-token probe or an owed vote".
type PassSender interface {
	// Pass takes ownership of the committed nonce and releases the assignment's escrow hold once it holds its own; false leaves the hold to the caller.
	Pass(request PassRequest) bool
}

// PassRequest is a committed placeholder nonce the gateway did not send because of its own judgement of the host.
type PassRequest struct {
	Assignment Assignment
	Model      string
	StartedAt  int64
	Send       bool
	Resolved   func(PassOutcome)
}

type passBook struct {
	mu         sync.Mutex
	unresolved map[string]int
}

func (b *passBook) room(escrowID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return maxUnresolvedPasses - b.unresolved[escrowID]
}

func (b *passBook) begin(escrowID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.unresolved == nil {
		b.unresolved = map[string]int{}
	}
	b.unresolved[escrowID]++
}

func (b *passBook) end(escrowID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.unresolved[escrowID]--; b.unresolved[escrowID] <= 0 {
		delete(b.unresolved, escrowID)
	}
}

func (d *dispatcher) passing() bool {
	if d.passer == nil || d.passes == nil || d.hostLoadMode == nil {
		return false
	}
	return config.RoutesByLoad(d.hostLoadMode())
}

func (k GhostKind) passable() bool {
	return shareKindFor[k] == share.Passed
}

func (d *dispatcher) sendPass(passed burn, taken reservation, prepared Prepared, startedAt int64) {
	if prepared == nil {
		taken.releaseHold()
		return
	}
	escrowID, participant := d.escrowID, taken.participant
	d.passes.begin(escrowID)
	request := PassRequest{
		Assignment: Assignment{Escrow: escrowID, Host: participant, Nonce: prepared, EscrowHold: taken.escrowHold},
		Model:      d.model,
		StartedAt:  startedAt,
		Send:       passed.kind != ghostCutOff && !d.session.ChatBudgetSpent(participant),
		Resolved: func(outcome PassOutcome) {
			d.passes.end(escrowID)
			d.count(participant, outcome.shareKind())
		},
	}
	if d.passer.Pass(request) {
		return
	}
	d.passes.end(escrowID)
	d.recordGhost(Burn{Nonce: prepared.Nonce(), Participant: participant, Reason: passed.kind.reason()})
	d.count(participant, share.PassUnvoted)
	taken.releaseHold()
}
