package burns

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
)

// The burn charge has its own doubles: it borrowed the warmup's while they shared a package, and a
// test that reaches into another subsystem's fixtures binds the two together again.
// Burned charges on its own goroutine, so the count the test reads is written from another one.
type stubPoster struct {
	vote  string
	err   error
	votes atomic.Int64
}

func (p *stubPoster) SettleTimeout(context.Context, uint64, time.Time) (engine.TimeoutVote, error) {
	p.votes.Add(1)
	return engine.TimeoutVote{Kind: p.vote}, p.err
}

func (p *stubPoster) calls() int { return int(p.votes.Load()) }

type spyTimeouts struct {
	events []engine.TimeoutEvent
}

func (s *spyTimeouts) RecordTimeout(event engine.TimeoutEvent) {
	s.events = append(s.events, event)
}

type stubEscrows struct {
	session  registry.EscrowSession
	live     bool
	released int
}

func (s *stubEscrows) Acquire(string) (registry.EscrowSession, func(), bool) {
	if !s.live {
		return nil, nil, false
	}
	return s.session, func() { s.released++ }, true
}

type stubSession struct {
	registry.EscrowSession
	nonce uint64
}

func (s stubSession) Nonce() uint64 { return s.nonce }

func chargingFor(poster *stubPoster, timeouts *spyTimeouts, enabled bool) *Accountant {
	return &Accountant{
		escrows:  &stubEscrows{session: stubSession{}, live: true},
		timeouts: timeouts,
		posters: func(string, any) (engine.TimeoutPoster, bool) {
			return poster, true
		},
		enabled: func() bool { return enabled },
		now:     func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
}

func TestOnlyTheBurnsAHostEarnedAreCharged(t *testing.T) {
	tests := []struct {
		reason string
		charge bool
	}{
		{scheduler.GhostReasonThrottled, true},
		{scheduler.GhostReasonStateDiverged, true},
		{"poc_unavailable_host", false},
		{"no_compatible_request_after_stale", false},
		{"request_abandoned_before_dispatch", false},
		{"participant_ejected_no_send", false},
	}
	for _, testCase := range tests {
		t.Run(testCase.reason, func(t *testing.T) {
			poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
			charges := chargingFor(poster, timeouts, true)

			charges.Burned("escrow-1", scheduler.Burn{Nonce: 4, Participant: "host-a", Reason: testCase.reason})
			waitForCharge(t, func() bool { return poster.calls() > 0 || !testCase.charge })

			if got := poster.calls() > 0; got != testCase.charge {
				t.Errorf("charged = %v, want %v", got, testCase.charge)
			}
		})
	}
}

func TestAnOperatorCanStopChargingWithoutARebuild(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, false)

	charges.Burned("escrow-1", scheduler.Burn{Nonce: 4, Participant: "host-a", Reason: scheduler.GhostReasonThrottled})
	waitForCharge(t, func() bool { return true })

	if poster.calls() != 0 {
		t.Errorf("SettleTimeout calls = %d, want 0 while the switch is off", poster.calls())
	}
}

// A burn decided before the session could commit has no nonce, so there is nothing to vote on.
func TestABurnWithNoNonceIsNotCharged(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, true)

	charges.Burned("escrow-1", scheduler.Burn{Nonce: 0, Participant: "host-a", Reason: scheduler.GhostReasonThrottled})
	waitForCharge(t, func() bool { return true })

	if poster.calls() != 0 {
		t.Errorf("SettleTimeout calls = %d, want 0 for a nonce that was never committed", poster.calls())
	}
}

func waitForCharge(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			// The charge runs in its own goroutine; give a negative case time to prove itself wrong.
			time.Sleep(20 * time.Millisecond)
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the charge never ran")
}

func TestAHostThatAnswersTheProbeIsNotCharged(t *testing.T) {
	tests := []struct {
		name        string
		answered    bool
		wantVotes   int
		wantOutcome string
	}{
		{name: "the host answered", answered: true, wantOutcome: engine.TimeoutReasonHostServedProbe},
		{name: "the host stayed silent", wantVotes: 1, wantOutcome: "none"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
			charges := chargingFor(poster, timeouts, true)
			charges.probes = newProbeGate()
			charges.send = func(registry.EscrowSession, scheduler.Burn) bool { return testCase.answered }

			charges.charge("escrow-1", scheduler.Burn{Nonce: 4, Participant: "host-a", Reason: scheduler.GhostReasonThrottled})

			if poster.calls() != testCase.wantVotes {
				t.Errorf("posted %d votes, want %d", poster.calls(), testCase.wantVotes)
			}
			last := timeouts.events[len(timeouts.events)-1]
			if testCase.answered && (last.Action != engine.TimeoutActionSkipped || last.Reason != engine.TimeoutReasonHostServedProbe) {
				t.Errorf("recorded %s/%s, want the charge withdrawn as %s", last.Action, last.Reason, engine.TimeoutReasonHostServedProbe)
			}
			if !testCase.answered && last.Reason == engine.TimeoutReasonHostServedProbe {
				t.Error("a silent host was recorded as having served the probe")
			}
		})
	}
}

func TestAChargeWithoutAProbeStillVotes(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, true)

	charges.charge("escrow-1", scheduler.Burn{Nonce: 4, Participant: "host-a", Reason: scheduler.GhostReasonThrottled})

	if poster.calls() != 1 {
		t.Errorf("posted %d votes with no probe configured, want 1", poster.calls())
	}
}

func TestAChargeVotesWhenTheGateHoldsTheProbeBack(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, true)
	charges.probes = newProbeGate()
	charges.send = func(registry.EscrowSession, scheduler.Burn) bool { return true }
	if !charges.probes.enter("host-a", time.Unix(1_700_000_000, 0)) {
		t.Fatal("the fixture could not take the participant's only probe slot")
	}

	charges.charge("escrow-1", scheduler.Burn{Nonce: 4, Participant: "host-a", Reason: scheduler.GhostReasonThrottled})

	if poster.calls() != 1 {
		t.Errorf("posted %d votes while the gate held the probe, want 1", poster.calls())
	}
	if last := timeouts.events[len(timeouts.events)-1]; last.Reason == engine.TimeoutReasonHostServedProbe {
		t.Error("a nonce nobody probed was recorded as served")
	}
}

// The vote needs the escrow held for the whole refusal wait. An escrow the registry no longer routes to
// gives no hold, so settlement reads it as idle and can conclude -- or close the session -- mid-vote.
func TestAnEscrowThatIsNoLongerRoutableIsNotVotedThrough(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, true)
	charges.escrows = &stubEscrows{live: false}

	charges.charge("escrow-1", scheduler.Burn{Nonce: 4, Participant: "host-a", Reason: scheduler.GhostReasonThrottled})

	if poster.calls() != 0 {
		t.Errorf("posted %d votes without holding the escrow, want none", poster.calls())
	}
	if last := timeouts.events[len(timeouts.events)-1]; last.Reason != engine.TimeoutReasonEscrowNotLive {
		t.Errorf("the skipped charge was recorded as %q, want %q so the ledger shows why", last.Reason, engine.TimeoutReasonEscrowNotLive)
	}
}
