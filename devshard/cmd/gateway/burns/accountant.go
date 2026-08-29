// Package burns charges a host for the nonces it would not take: without the vote the escrow pays the
// reserve and the host reaches no miss. The burn is spent on a real request first, so a host that is
// merely busy clears itself rather than earning a miss it did not deserve.
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

const (
	timeoutKind = "refused"
	noPoster    = "no_poster"
	// hostServedProbe is the ledger's own name for a charge the host talked its way out of. It has to be
	// a name the vocabulary admits, or the reason normalises to "unknown" and the withdrawal is invisible.
	hostServedProbe = "host_served_probe"
)

// The collaborators are declared here rather than borrowed: a burn is not a warmup, and each side
// naming the two methods it calls keeps one from moving the other's.
// Posters resolves the vote poster for one escrow and the params its nonce committed.
type Posters func(escrowID string, params any) (engine.TimeoutPoster, bool)

type Escrows interface {
	Acquire(escrowID string) (registry.EscrowSession, func(), bool)
}

type Timeouts interface {
	RecordTimeout(event engine.TimeoutEvent)
}

// Session is how a probe reaches the host: the burn already committed the nonce, so the request
// it carries is the one the escrow has already paid for.
type Session interface {
	SendOnly(ctx context.Context, prepared *user.PreparedInference, stream io.Writer, onReceipt func()) (*host.HostResponse, error)
	ProcessResponse(hostIdx int, reply *host.HostResponse, inferenceNonce uint64) error
}

// accountableBurns are the burns the host earned. A host under PoC is unavailable by the protocol's own
// grant, and an excluded or abandoned burn is this gateway's own scheduling; neither is a refusal.
var accountableBurns = map[string]bool{
	scheduler.GhostReasonThrottled:     true,
	scheduler.GhostReasonStateDiverged: true,
}

// Accountant charges a host for a nonce it would not take: without the vote the escrow pays
// the reserve and the host reaches no miss. The burn already committed a MsgStart, so the protocol has
// everything the vote needs.
type Accountant struct {
	escrows  Escrows
	posters  Posters
	timeouts Timeouts
	enabled  func() bool
	now      func() time.Time
	probes   *probeGate
	// send is the dispatch itself, kept behind a field for the same reason the warmup keeps its own:
	// the committed inference has no constructor outside its package, so only the decision above it is
	// reachable from a unit test.
	send func(session registry.EscrowSession, burned scheduler.Burn) bool
}

func (g *Accountant) Burned(escrowID string, burned scheduler.Burn) {
	if g == nil || g.posters == nil || burned.Nonce == 0 || !accountableBurns[burned.Reason] {
		return
	}
	if g.enabled != nil && !g.enabled() {
		return
	}
	// One goroutine per burn: charging waits out the refusal deadline, and a queue of burns settled in
	// turn would hold the escrow's drain barrier for that wait once per nonce.
	go g.charge(escrowID, burned)
}

// charge votes on the burned nonce. HandleTimeout sleeps out the refusal deadline before it talks to
// verifiers, so it holds the escrow for that whole wait: settlement must not conclude while a vote for
// one of its nonces is still in flight.
func (g *Accountant) charge(escrowID string, burned scheduler.Burn) {
	nonce, participant := burned.Nonce, burned.Participant
	session, release, live := g.escrows.Acquire(escrowID)
	if live {
		defer release()
	}

	event := engine.TimeoutEvent{
		EscrowID: escrowID, Participant: participant, Nonce: nonce,
		Kind: timeoutKind, Action: engine.TimeoutActionStarted, Reason: burned.Reason,
	}
	if g.served(session, burned) {
		served := event
		served.Action, served.Reason = engine.TimeoutActionSkipped, hostServedProbe
		g.record(served)
		logging.Info("host answered the nonce it was about to be charged for", logkey.Escrow, escrowID,
			logkey.Nonce, nonce, logkey.Host, logkey.ShortHost(participant))
		return
	}
	poster, resolved := g.posters(escrowID, user.InferenceParams{Prompt: registry.GhostPrompt()})
	if !resolved {
		skipped := event
		skipped.Action, skipped.Reason = engine.TimeoutActionSkipped, noPoster
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

// served spends the burn on the request it already committed instead of on silence, and reports whether
// the host answered. A host that is merely busy clears itself; one that stays silent earns exactly the
// timeout the quiet burn would have raised.
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
	if applyErr := dispatch.ProcessResponse(prepared.HostIdx(), reply, prepared.Nonce()); applyErr != nil {
		logging.Warn("the probe answer did not apply", logkey.Nonce, burned.Nonce,
			logkey.Host, logkey.ShortHost(burned.Participant), logkey.Error, applyErr)
	}
	return err == nil
}

func (g *Accountant) record(event engine.TimeoutEvent) {
	if g.timeouts != nil {
		g.timeouts.RecordTimeout(event)
	}
}

// New builds the charge with what the process decides; Serve supplies the rest once the registry and
// the sessions exist, which is after the charge itself must be handed to the scheduler.
func New(now func() time.Time, enabled func() bool) *Accountant {
	return &Accountant{now: now, enabled: enabled, probes: newProbeGate(), send: dispatchProbe}
}

func (g *Accountant) Serve(escrows Escrows, posters Posters, timeouts Timeouts) {
	if g != nil {
		g.escrows, g.posters, g.timeouts = escrows, posters, timeouts
	}
}
