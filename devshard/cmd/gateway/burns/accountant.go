// Package burns charges a host for the nonces it would not take, spending each burn on a real request
// first so a host that is merely busy clears itself. See README.md.
package burns

import (
	"context"
	"io"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/host"
	"devshard/logging"
	"devshard/user"
)

// Posters resolves the vote poster for one escrow and the params its nonce committed.
type Posters func(escrowID string, params any) (engine.TimeoutPoster, bool)

type Escrows interface {
	Acquire(escrowID string) (registry.EscrowSession, func(), bool)
}

type Timeouts interface {
	RecordTimeout(event engine.TimeoutEvent)
}

// Session is how a probe reaches the host, carrying the request the escrow has already paid for.
type Session interface {
	SendOnly(ctx context.Context, prepared *user.PreparedInference, stream io.Writer, onReceipt func()) (*host.HostResponse, error)
	ProcessResponse(hostIdx int, reply *host.HostResponse, inferenceNonce uint64) error
}

// accountableBurns are the burns the host earned; the rest are this gateway's own doing.
var accountableBurns = map[string]bool{
	scheduler.GhostReasonThrottled:     true,
	scheduler.GhostReasonStateDiverged: true,
}

type Accountant struct {
	escrows  Escrows
	posters  Posters
	timeouts Timeouts
	enabled  func() bool
	now      func() time.Time
	probes   *probeGate
	// Behind a field because the committed inference has no constructor outside its package.
	send func(session registry.EscrowSession, burned scheduler.Burn) bool
}

func (g *Accountant) Burned(escrowID string, burned scheduler.Burn) {
	if g == nil || g.posters == nil || burned.Nonce == 0 || !accountableBurns[burned.Reason] {
		return
	}
	if g.enabled != nil && !g.enabled() {
		return
	}
	// One goroutine per burn: a queue would hold the escrow's drain barrier once per nonce.
	go g.charge(escrowID, burned)
}

// charge votes on the burned nonce, holding the escrow for the whole refusal wait.
func (g *Accountant) charge(escrowID string, burned scheduler.Burn) {
	nonce, participant := burned.Nonce, burned.Participant
	event := engine.TimeoutEvent{
		EscrowID: escrowID, Participant: participant, Nonce: nonce,
		Kind: engine.TimeoutKindRefused, Action: engine.TimeoutActionStarted, Reason: burned.Reason,
	}

	// Without the hold, settlement reads the escrow as idle and can conclude while the vote is in flight.
	session, release, live := g.escrows.Acquire(escrowID)
	if !live {
		skipped := event
		skipped.Action, skipped.Reason = engine.TimeoutActionSkipped, engine.TimeoutReasonEscrowNotLive
		g.record(skipped)
		return
	}
	defer release()
	if g.served(session, burned) {
		served := event
		served.Action, served.Reason = engine.TimeoutActionSkipped, engine.TimeoutReasonHostServedProbe
		g.record(served)
		logging.Info("host answered the nonce it was about to be charged for", logkey.Escrow, escrowID,
			logkey.Nonce, nonce, logkey.Host, logkey.ShortHost(participant))
		return
	}
	poster, resolved := g.posters(escrowID, user.InferenceParams{Prompt: registry.GhostPrompt()})
	if !resolved {
		skipped := event
		skipped.Action, skipped.Reason = engine.TimeoutActionSkipped, engine.TimeoutReasonNoPoster
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

// served spends the burn on the request it already committed, and reports whether the host answered.
func (g *Accountant) served(session registry.EscrowSession, burned scheduler.Burn) bool {
	if session == nil || g.send == nil || g.probes == nil {
		return false
	}
	if !g.probes.enter(burned.Participant, g.now()) {
		return false
	}
	defer g.probes.leave(burned.Participant)
	return g.send(session, burned)
}

// dispatchProbe sends the nonce the burn committed to the host it was bound to.
func dispatchProbe(session registry.EscrowSession, burned scheduler.Burn) bool {
	prepared, dispatchable := burned.Prepared.(*user.PreparedInference)
	if !dispatchable {
		return false
	}
	var dispatch Session = session.UserSession()
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()

	reply, err := dispatch.SendOnly(ctx, prepared, nil, nil)
	if reply == nil {
		return false
	}
	// Applied even beside an error: a reply that arrived carries the receipt the chain still needs.
	applyErr := dispatch.ProcessResponse(prepared.HostIdx(), reply, prepared.Nonce())
	if applyErr != nil {
		logging.Warn("the probe answer did not apply", logkey.Nonce, burned.Nonce,
			logkey.Host, logkey.ShortHost(burned.Participant), logkey.Error, applyErr)
	}
	return err == nil && applyErr == nil
}

func (g *Accountant) record(event engine.TimeoutEvent) {
	if g.timeouts != nil {
		g.timeouts.RecordTimeout(event)
	}
}

// Serve supplies the rest: the registry and the sessions exist only after the scheduler has the charge.
func New(now func() time.Time, enabled func() bool) *Accountant {
	return &Accountant{now: now, enabled: enabled, probes: newProbeGate(), send: dispatchProbe}
}

func (g *Accountant) Serve(escrows Escrows, posters Posters, timeouts Timeouts) {
	if g != nil {
		g.escrows, g.posters, g.timeouts = escrows, posters, timeouts
	}
}
