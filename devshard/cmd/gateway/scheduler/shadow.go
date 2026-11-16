package scheduler

// ShadowProber sends the probe a shadow participant's nonce is spent on. See routing.md, "Shadow participants".
type ShadowProber interface {
	// Params builds the probe's dispatch params for model; StartedAt is in seconds.
	Params(model string, startedAt int64) any
	// Probe takes ownership of the committed nonce and releases the assignment's escrow hold once it holds its own; false leaves the hold to the caller.
	Probe(request ProbeRequest) bool
}

// ProbeRequest is a committed nonce bound to a shadow participant, with the params it was committed with.
type ProbeRequest struct {
	Assignment Assignment
	Model      string
	Params     any
}

func (d *dispatcher) shadowDecision() Decision {
	if d.shadow == nil {
		return burn{kind: ghostUntrusted}
	}
	return probe{params: d.shadow.Params(d.model, d.now().Unix())}
}

func (d *dispatcher) sendProbe(sent probe, taken reservation, prepared Prepared) {
	if prepared == nil {
		taken.releaseHold()
		return
	}
	assignment := Assignment{Escrow: d.escrowID, Host: taken.participant, Nonce: prepared, EscrowHold: taken.escrowHold}
	if d.shadow.Probe(ProbeRequest{Assignment: assignment, Model: d.model, Params: sent.params}) {
		return
	}
	d.recordGhost(Burn{Nonce: prepared.Nonce(), Participant: taken.participant, Reason: ghostUntrusted.reason(), RequestID: d.burnedDuring(nil)})
	taken.releaseHold()
}
