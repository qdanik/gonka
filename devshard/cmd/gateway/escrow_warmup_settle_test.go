package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/user"
)

type stubPoster struct {
	vote  string
	err   error
	calls int
}

func (p *stubPoster) SettleTimeout(context.Context, uint64, time.Time) (string, error) {
	p.calls++
	return p.vote, p.err
}

type spyTimeouts struct {
	events []engine.TimeoutEvent
}

func (s *spyTimeouts) RecordTimeout(event engine.TimeoutEvent) {
	s.events = append(s.events, event)
}

func refusedWarmup(poster *stubPoster, timeouts *spyTimeouts, resolved bool) *escrowWarmup {
	return &escrowWarmup{
		escrows:  &stubEscrows{session: stubSession{}, live: true},
		ledger:   &spyLedger{},
		timeouts: timeouts,
		posters: func(string, any) (engine.TimeoutPoster, bool) {
			return poster, resolved
		},
		probe: func(_ context.Context, _ registry.EscrowSession, _ user.InferenceParams, nonceCommitted func()) (uint64, bool, error) {
			nonceCommitted()
			return 7, false, errors.New("host refused")
		},
		catchUp: func(context.Context, registry.EscrowSession) error { return nil },
		stop:    make(chan struct{}),
		now:     warmupClock(),
	}
}

func TestARefusedWarmupProbeIsVotedOn(t *testing.T) {
	poster := &stubPoster{vote: "refused"}
	timeouts := &spyTimeouts{}

	refusedWarmup(poster, timeouts, true).warm("escrow-1", "model-a")

	if poster.calls != 1 {
		t.Fatalf("SettleTimeout calls = %d, want 1", poster.calls)
	}
	if len(timeouts.events) != 2 {
		t.Fatalf("recorded %d timeout events, want a started and a completed one: %+v", len(timeouts.events), timeouts.events)
	}
	if got := timeouts.events[0].Action; got != engine.TimeoutActionStarted {
		t.Errorf("first event action = %q, want %q", got, engine.TimeoutActionStarted)
	}
	if got := timeouts.events[1].Action; got != engine.TimeoutActionCompleted {
		t.Errorf("second event action = %q, want %q", got, engine.TimeoutActionCompleted)
	}
	if got := timeouts.events[1].Nonce; got != 7 {
		t.Errorf("event nonce = %d, want the nonce the probe spent", got)
	}
}

func TestAServedWarmupProbeIsNotVotedOn(t *testing.T) {
	poster := &stubPoster{}
	timeouts := &spyTimeouts{}
	warmup := refusedWarmup(poster, timeouts, true)
	warmup.probe = func(_ context.Context, _ registry.EscrowSession, _ user.InferenceParams, nonceCommitted func()) (uint64, bool, error) {
		nonceCommitted()
		return 7, true, nil
	}

	warmup.warm("escrow-1", "model-a")

	if poster.calls != 0 {
		t.Errorf("SettleTimeout calls = %d, want 0: the host served it", poster.calls)
	}
	if len(timeouts.events) != 0 {
		t.Errorf("recorded %+v, want no timeout event for a served probe", timeouts.events)
	}
}

// A retired escrow resolves no poster. The nonce still has to read as something other than in flight.
func TestAWarmupProbeWithNoPosterIsRecordedAsSkipped(t *testing.T) {
	poster := &stubPoster{}
	timeouts := &spyTimeouts{}

	refusedWarmup(poster, timeouts, false).warm("escrow-1", "model-a")

	if poster.calls != 0 {
		t.Errorf("SettleTimeout calls = %d, want 0: no poster resolved", poster.calls)
	}
	if len(timeouts.events) != 1 || timeouts.events[0].Action != engine.TimeoutActionSkipped {
		t.Fatalf("recorded %+v, want one skipped event", timeouts.events)
	}
	if got := timeouts.events[0].Reason; got != warmupTimeoutNoPoster {
		t.Errorf("reason = %q, want %q", got, warmupTimeoutNoPoster)
	}
}
