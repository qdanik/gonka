package main

import (
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/scheduler"
)

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
