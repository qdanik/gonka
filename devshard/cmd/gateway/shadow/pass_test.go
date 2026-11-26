package shadow

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/warmup"
	"devshard/host"
	"devshard/user"
)

type passFixture struct {
	runner   *PassRunner
	escrows  *stubEscrows
	session  *stubSession
	probes   *spyProbes
	poster   *stubPoster
	timeouts *spyTimeouts
	held     atomic.Int64
	faulted  atomic.Int64
	outcomes chan scheduler.PassOutcome
}

func newPassFixture(t *testing.T, send sendFunc, capacity int, voted bool) *passFixture {
	t.Helper()
	session := &stubSession{}
	fixture := &passFixture{
		runner:   NewPassRunner(steppedClock()),
		escrows:  &stubEscrows{session: session, held: true, voteReleased: make(chan struct{})},
		session:  session,
		probes:   &spyProbes{},
		poster:   &stubPoster{},
		timeouts: &spyTimeouts{},
		outcomes: make(chan scheduler.PassOutcome, 4),
	}
	fixture.runner.send = send
	fixture.runner.Bind(PassBinding{
		Escrows:  fixture.escrows,
		Lifetime: t.Context(),
		Voter: warmup.Voter{
			Posters:  func(string, any) (engine.TimeoutPoster, bool) { return fixture.poster, voted },
			Timeouts: fixture.timeouts,
		},
		Probes:  fixture.probes,
		Timeout: func(string) time.Duration { return time.Hour },
		Cap:     func(string, string) int { return capacity },
		Faulted: func(string, string, time.Time) { fixture.faulted.Add(1) },
	})
	return fixture
}

func (f *passFixture) request(send bool) scheduler.PassRequest {
	return scheduler.PassRequest{
		Assignment: scheduler.Assignment{
			Escrow:     probeEscrow,
			Host:       probeParticipant,
			Nonce:      &user.PreparedInference{},
			EscrowHold: func() { f.held.Add(1) },
		},
		Model:     probeModel,
		StartedAt: 1_700_000_000,
		Send:      send,
		Resolved:  func(outcome scheduler.PassOutcome) { f.outcomes <- outcome },
	}
}

func (f *passFixture) await(t *testing.T) scheduler.PassOutcome {
	t.Helper()
	select {
	case outcome := <-f.outcomes:
		return outcome
	case <-time.After(awaitBudget):
		t.Fatal("timed out waiting for the pass to resolve")
		return 0
	}
}

func refusedSend(t *testing.T) sendFunc {
	return func(context.Context, registry.EscrowSession, *user.PreparedInference, io.Writer, func()) (*host.HostResponse, error) {
		t.Error("a probe was sent, want none")
		return nil, errors.New("must not send")
	}
}

// Test flow:
//  1. Bind a pass runner whose send answers with a receipt, and pass a nonce with Send true.
//  2. Await the outcome.
//  3. Assert it is PassAnswered, the ledger got one attempt under `TerminalBusyProbe`, sent and acknowledged, no vote was cast, the assignment's hold was given back and the runner's own released.
func TestAnsweredPassIsRecordedAsABusyProbeWithoutAVote(t *testing.T) {
	fixture := newPassFixture(t, servedAnswer(`data: {"choices":[{"delta":{"content":"."}}]}`+"\n\n"), 1, true)

	if !fixture.runner.Pass(fixture.request(true)) {
		t.Fatal("Pass = false, want the probe taken")
	}

	if outcome := fixture.await(t); outcome != scheduler.PassAnswered {
		t.Fatalf("outcome = %v, want PassAnswered", outcome)
	}
	if len(fixture.probes.attempts) != 1 || fixture.probes.attempts[0].Terminal != accounting.TerminalBusyProbe || !fixture.probes.attempts[0].Acknowledged || !fixture.probes.attempts[0].Sent {
		t.Errorf("ledger attempts = %+v, want one sent and acknowledged busy probe", fixture.probes.attempts)
	}
	if fixture.poster.calls.Load() != 0 || fixture.faulted.Load() != 0 || fixture.held.Load() != 1 || fixture.escrows.released.Load() != 1 {
		t.Errorf("votes %d, faults %d, assignment holds %d, runner holds %d, want 0, 0, 1, 1", fixture.poster.calls.Load(), fixture.faulted.Load(), fixture.held.Load(), fixture.escrows.released.Load())
	}
}

// Test flow:
//  1. Bind a pass runner whose send fails before any receipt, then one whose send fails after a receipt.
//  2. Pass a nonce with Send true to each and await the outcomes.
//  3. Assert both are PassMissed, the first votes a refused timeout and the second an execution timeout, and each failed probe is counted toward the cut-off once.
func TestUnansweredPassVotesTheNonceAndCountsAFault(t *testing.T) {
	failing := func(withReceipt bool) sendFunc {
		return func(_ context.Context, _ registry.EscrowSession, _ *user.PreparedInference, _ io.Writer, onReceipt func()) (*host.HostResponse, error) {
			if withReceipt {
				onReceipt()
			}
			return nil, errors.New("stream cut")
		}
	}
	for withReceipt, wantKind := range map[bool]engine.TimeoutKind{false: engine.TimeoutKindRefused, true: engine.TimeoutKindExecution} {
		fixture := newPassFixture(t, failing(withReceipt), 1, true)

		fixture.runner.Pass(fixture.request(true))

		if outcome := fixture.await(t); outcome != scheduler.PassMissed {
			t.Fatalf("outcome with receipt %v = %v, want PassMissed", withReceipt, outcome)
		}
		if len(fixture.timeouts.events) == 0 || fixture.timeouts.events[0].Kind != wantKind || fixture.poster.calls.Load() != 1 || fixture.faulted.Load() != 1 {
			t.Errorf("with receipt %v: events %+v, votes %d, faults %d, want a %s vote and one fault", withReceipt, fixture.timeouts.events, fixture.poster.calls.Load(), fixture.faulted.Load(), wantKind)
		}
	}
}

// Test flow:
//  1. Bind a pass runner whose send fails the test, and pass a nonce with Send false.
//  2. Await the outcome.
//  3. Assert it is PassOwed, a refused vote was cast, nothing was sent or recorded as a probe, and no fault was counted.
func TestAPassWithNoSendIsAnOwedVote(t *testing.T) {
	fixture := newPassFixture(t, refusedSend(t), 1, true)

	if !fixture.runner.Pass(fixture.request(false)) {
		t.Fatal("Pass = false, want the vote taken")
	}

	if outcome := fixture.await(t); outcome != scheduler.PassOwed {
		t.Fatalf("outcome = %v, want PassOwed", outcome)
	}
	if fixture.poster.calls.Load() != 1 || len(fixture.timeouts.events) == 0 || fixture.timeouts.events[0].Kind != engine.TimeoutKindRefused || len(fixture.probes.attempts) != 0 || fixture.faulted.Load() != 0 {
		t.Errorf("votes %d, events %+v, probes %d, faults %d, want one refused vote and nothing else", fixture.poster.calls.Load(), fixture.timeouts.events, len(fixture.probes.attempts), fixture.faulted.Load())
	}
}

// Test flow:
//  1. Bind a pass runner with a cap of one probe per host and a send that blocks until released.
//  2. Pass a first nonce with Send true, then a second with Send true while the first is in flight.
//  3. Assert the second is an owed vote with no send, the first still unresolved, and after the release the first answers and the host's slot is free for a third probe.
func TestTheProbeCapTurnsASecondPassIntoAnOwedVote(t *testing.T) {
	release := make(chan struct{})
	var sends atomic.Int64
	send := func(ctx context.Context, _ registry.EscrowSession, _ *user.PreparedInference, _ io.Writer, onReceipt func()) (*host.HostResponse, error) {
		sends.Add(1)
		onReceipt()
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(awaitBudget):
			return nil, errors.New("the test never released the first probe")
		}
		return &host.HostResponse{Receipt: []byte("receipt")}, nil
	}
	fixture := newPassFixture(t, send, 1, true)

	fixture.runner.Pass(fixture.request(true))
	fixture.runner.Pass(fixture.request(true))

	if outcome := fixture.await(t); outcome != scheduler.PassOwed {
		t.Fatalf("second pass outcome = %v, want PassOwed past the cap", outcome)
	}
	close(release)
	if outcome := fixture.await(t); outcome != scheduler.PassAnswered {
		t.Fatalf("first pass outcome = %v, want PassAnswered", outcome)
	}
	fixture.runner.Pass(fixture.request(true))
	if outcome := fixture.await(t); outcome != scheduler.PassAnswered || sends.Load() != 2 {
		t.Errorf("third pass outcome %v after %d sends, want an answered probe once the slot is free", outcome, sends.Load())
	}
}

// Test flow:
//  1. Bind a pass runner whose voter resolves no poster, and pass a nonce with Send false, then one that fails to send.
//  2. Assert both resolve as PassUnvoted.
//  3. Assert Pass is refused for a runner never bound, and for an escrow nobody holds, leaving the assignment's hold to the caller.
func TestAPassWhoseVoteCannotBePostedIsUnvotedAndAnUnheldEscrowIsRefused(t *testing.T) {
	failing := func(context.Context, registry.EscrowSession, *user.PreparedInference, io.Writer, func()) (*host.HostResponse, error) {
		return nil, errors.New("connection refused")
	}
	fixture := newPassFixture(t, failing, 1, false)
	fixture.runner.Pass(fixture.request(false))
	if outcome := fixture.await(t); outcome != scheduler.PassUnvoted {
		t.Errorf("owed vote with no poster = %v, want PassUnvoted", outcome)
	}
	fixture.runner.Pass(fixture.request(true))
	if outcome := fixture.await(t); outcome != scheduler.PassUnvoted {
		t.Errorf("failed probe with no poster = %v, want PassUnvoted", outcome)
	}

	if NewPassRunner(steppedClock()).Pass(fixture.request(true)) {
		t.Error("Pass = true on a runner never bound, want false")
	}
	fixture.escrows.held = false
	before := fixture.held.Load()
	if fixture.runner.Pass(fixture.request(true)) || fixture.runner.Pass(fixture.request(false)) || fixture.held.Load() != before {
		t.Error("Pass took a nonce on an escrow nobody holds, want false and the hold left to the caller")
	}
}
