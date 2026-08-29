package main

import (
	"context"
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
)

// The burn charge has its own doubles: it borrowed the warmup's while they shared a package, and a
// test that reaches into another subsystem's fixtures binds the two together again.
type stubPoster struct {
	vote  string
	err   error
	calls int
}

func (p *stubPoster) SettleTimeout(context.Context, uint64, time.Time) (engine.TimeoutVote, error) {
	p.calls++
	return engine.TimeoutVote{Kind: p.vote}, p.err
}

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

func chargingFor(poster *stubPoster, timeouts *spyTimeouts, enabled bool) *ghostAccountability {
	return &ghostAccountability{
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

			charges.burned("escrow-1", 4, "host-a", testCase.reason)
			waitForCharge(t, func() bool { return poster.calls > 0 || !testCase.charge })

			if got := poster.calls > 0; got != testCase.charge {
				t.Errorf("charged = %v, want %v", got, testCase.charge)
			}
		})
	}
}

func TestAnOperatorCanStopChargingWithoutARebuild(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, false)

	charges.burned("escrow-1", 4, "host-a", scheduler.GhostReasonThrottled)
	waitForCharge(t, func() bool { return true })

	if poster.calls != 0 {
		t.Errorf("SettleTimeout calls = %d, want 0 while the switch is off", poster.calls)
	}
}

// A burn decided before the session could commit has no nonce, so there is nothing to vote on.
func TestABurnWithNoNonceIsNotCharged(t *testing.T) {
	poster, timeouts := &stubPoster{vote: "refused"}, &spyTimeouts{}
	charges := chargingFor(poster, timeouts, true)

	charges.burned("escrow-1", 0, "host-a", scheduler.GhostReasonThrottled)
	waitForCharge(t, func() bool { return true })

	if poster.calls != 0 {
		t.Errorf("SettleTimeout calls = %d, want 0 for a nonce that was never committed", poster.calls)
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
