package main

import (
	"context"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
	"devshard/user"
)

const (
	ghostTimeoutKind = "refused"
	ghostNoPoster    = "no_poster"
)

// The collaborators are declared here rather than borrowed: a burn is not a warmup, and each side
// naming the two methods it calls keeps one from moving the other's.
type chargeEscrows interface {
	Acquire(escrowID string) (registry.EscrowSession, func(), bool)
}

type chargeTimeouts interface {
	RecordTimeout(event engine.TimeoutEvent)
}

// accountableBurns are the burns the host earned. A host under PoC is unavailable by the protocol's own
// grant, and an excluded or abandoned burn is this gateway's own scheduling; neither is a refusal.
var accountableBurns = map[string]bool{
	scheduler.GhostReasonThrottled:     true,
	scheduler.GhostReasonStateDiverged: true,
}

// ghostAccountability charges a host for a nonce it would not take: without the vote the escrow pays
// the reserve and the host reaches no miss. The burn already committed a MsgStart, so the protocol has
// everything the vote needs.
type ghostAccountability struct {
	escrows  chargeEscrows
	posters  func(escrowID string, params any) (engine.TimeoutPoster, bool)
	timeouts chargeTimeouts
	enabled  func() bool
	now      func() time.Time
}

func (g *ghostAccountability) burned(escrowID string, nonce uint64, participant, reason string) {
	if g == nil || g.posters == nil || nonce == 0 || !accountableBurns[reason] {
		return
	}
	if g.enabled != nil && !g.enabled() {
		return
	}
	go g.charge(escrowID, nonce, participant, reason)
}

// charge votes on the burned nonce. HandleTimeout sleeps out the refusal deadline before it talks to
// verifiers, so it holds the escrow for that whole wait: settlement must not conclude while a vote for
// one of its nonces is still in flight.
func (g *ghostAccountability) charge(escrowID string, nonce uint64, participant, reason string) {
	if _, release, live := g.escrows.Acquire(escrowID); live {
		defer release()
	}

	event := engine.TimeoutEvent{
		EscrowID: escrowID, Participant: participant, Nonce: nonce,
		Kind: ghostTimeoutKind, Action: engine.TimeoutActionStarted, Reason: reason,
	}
	poster, resolved := g.posters(escrowID, user.InferenceParams{Prompt: registry.GhostPrompt()})
	if !resolved {
		skipped := event
		skipped.Action, skipped.Reason = engine.TimeoutActionSkipped, ghostNoPoster
		g.record(skipped)
		return
	}
	g.record(event)

	vote, err := poster.SettleTimeout(context.Background(), nonce, g.now())
	posted := event
	posted.Action, posted.Reason = engine.TimeoutOutcome(vote, err, false)
	g.record(posted)

	logging.Info("host charged for a nonce it refused", logkey.Escrow, escrowID, logkey.Nonce, nonce,
		logkey.Host, logkey.ShortHost(participant), logkey.Action, posted.Action, logkey.Reason, posted.Reason)
}

func (g *ghostAccountability) record(event engine.TimeoutEvent) {
	if g.timeouts != nil {
		g.timeouts.RecordTimeout(event)
	}
}
