package engine

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/perf"
	"devshard/host"
	"devshard/user"
)

var errVoteCause = errors.New("collect timeout votes")

// scriptedTimeoutHandler reproduces Session.HandleTimeout's four returns verbatim, so the adapter is
// pinned against the shapes it actually normalizes rather than a paraphrase of them.
type scriptedTimeoutHandler struct {
	result user.TimeoutResult
	err    error
	calls  int
}

func (h *scriptedTimeoutHandler) HandleTimeout(context.Context, uint64, time.Time, *host.InferencePayload) (user.TimeoutResult, error) {
	h.calls++
	return h.result, h.err
}

// Every one of these returns carries a non-nil error, including the settled one, so only the handler's
// own Applied flag separates a vote that reached the escrow state from one that did not.
func TestSessionTimeoutsReadsAPostedVoteThroughTheHandlersAppliedFlag(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		result     user.TimeoutResult
		err        error
		wantVote   string
		wantFailed bool
	}{
		{
			name:     "vote_reached_the_escrow_state",
			result:   user.TimeoutResult{Reason: "execution", Applied: true},
			err:      fmt.Errorf("inference %d timed out: %s", 7, "execution"),
			wantVote: "execution",
		},
		{
			// The shape a settled vote has, minus the settlement: trusting the error here is the whole bug.
			name:       "unsettled_but_shaped_like_a_settled_vote",
			result:     user.TimeoutResult{Reason: "execution"},
			err:        fmt.Errorf("inference %d timed out: %s", 7, "execution"),
			wantVote:   "execution",
			wantFailed: true,
		},
		{
			name:       "votes_sufficed_but_the_diff_landed_no_timeout",
			result:     user.TimeoutResult{Reason: "execution"},
			err:        fmt.Errorf("inference %d: %w: diff landed no timeout", 7, user.ErrTimeoutNotApplied),
			wantVote:   "execution",
			wantFailed: true,
		},
		{
			name:       "insufficient_votes_reads_as_a_failure",
			result:     user.TimeoutResult{Reason: "refused"},
			err:        fmt.Errorf("inference %d: %w: insufficient votes", 7, user.ErrTimeoutNotApplied),
			wantVote:   "refused",
			wantFailed: true,
		},
		{
			name:       "diff_send_failure_wraps_its_cause",
			result:     user.TimeoutResult{Reason: "execution"},
			err:        fmt.Errorf("send timeout diff: %w", errVoteCause),
			wantVote:   "execution",
			wantFailed: true,
		},
		{
			name:       "vote_collection_failure_wraps_its_cause",
			result:     user.TimeoutResult{Reason: "refused"},
			err:        fmt.Errorf("collect timeout votes: %w", errVoteCause),
			wantVote:   "refused",
			wantFailed: true,
		},
		{
			name:       "deadline_never_reached_reports_no_reason_and_fails",
			result:     user.TimeoutResult{},
			err:        context.Canceled,
			wantFailed: true,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			handler := &scriptedTimeoutHandler{result: testCase.result, err: testCase.err}
			poster := &SessionTimeouts{handler: handler}

			vote, err := poster.SettleTimeout(context.Background(), 7, testEpoch)

			if vote != testCase.wantVote {
				t.Errorf("vote = %q, want %q", vote, testCase.wantVote)
			}
			if (err != nil) != testCase.wantFailed {
				t.Errorf("err = %v, want failed = %v", err, testCase.wantFailed)
			}
			if handler.calls != 1 {
				t.Errorf("handler calls = %d, want 1", handler.calls)
			}
		})
	}
}

// The adapter is the only route from a live session to the vote poster, so the wiring is pinned here
// rather than reached through a struct literal no caller can build.
func TestNewSessionTimeoutsWiresTheSessionAndItsPayload(t *testing.T) {
	t.Parallel()
	session := &user.Session{}
	payload := &host.InferencePayload{}

	poster := NewSessionTimeouts(session, payload)

	if poster.handler != timeoutHandler(session) {
		t.Errorf("handler = %v, want the session itself", poster.handler)
	}
	if poster.payload != payload {
		t.Errorf("payload = %v, want the request's own", poster.payload)
	}
}

// SettleTimeouts is where a mislabelled vote would surface, so the normalization is asserted through
// it rather than only on the adapter's own return.
func TestSettleTimeoutsRecordsAPostedVoteAsCompleted(t *testing.T) {
	t.Parallel()
	handler := &scriptedTimeoutHandler{
		result: user.TimeoutResult{Reason: "execution", Applied: true},
		err:    fmt.Errorf("inference %d timed out: %s", 7, "execution"),
	}
	outcome := race(failedAttempt(TerminalDialFailure))

	events := SettleTimeouts(context.Background(), &SessionTimeouts{handler: handler}, outcome)

	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	if events[1].Action != TimeoutActionCompleted {
		t.Errorf("action = %q, want %q", events[1].Action, TimeoutActionCompleted)
	}
	if events[1].Kind != timeoutKindExecution {
		t.Errorf("kind = %q, want execution", events[1].Kind)
	}
}

// A timeout the diff never carried is not a collection failure, and an operator reading the reason has
// only this label to tell the two apart.
func TestSettleTimeoutsNamesADiffThatCarriedNoTimeout(t *testing.T) {
	t.Parallel()
	handler := &scriptedTimeoutHandler{
		result: user.TimeoutResult{Reason: "execution"},
		err:    fmt.Errorf("inference %d: %w: diff landed no timeout", 7, user.ErrTimeoutNotApplied),
	}

	events := SettleTimeouts(context.Background(), &SessionTimeouts{handler: handler}, race(failedAttempt(TerminalDialFailure)))

	settled := events[len(events)-1]
	if settled.Action != TimeoutActionFailed {
		t.Fatalf("action = %q, want %q", settled.Action, TimeoutActionFailed)
	}
	if settled.Reason != timeoutReasonNotApplied {
		t.Fatalf("reason = %q, want %q", settled.Reason, timeoutReasonNotApplied)
	}
}

// Routing and settlement must both receive the caller's own params value. The engine never reads it, so
// a payload it substituted here would fail only at the far end, where the request is already lost.
func TestRunHandsTheCallersParamsToRoutingAndToSettlement(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{receipt: true, err: errors.New("host refused")})
	want := sim.profile().Params

	if _, err := sim.run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want the failed attempt's error")
	}
	sim.reported(t)
	sim.settleAll()

	routed := sim.picker.profiles
	if len(routed) != 1 || routed[0].Params != want {
		t.Fatalf("routing saw %d picks carrying %#v, want one carrying %#v", len(routed), routed, want)
	}
	settled := sim.poster.paramsSeen()
	if len(settled) != 1 || settled[0] != want {
		t.Fatalf("settlement saw params %#v, want one %#v", settled, want)
	}
}

// The pool-wide ejection cap deliberately leaves hosts in rotation once too many fail at once, so the
// routing gate cannot be the whole protection: the race hedges a primary the detector wanted out. Nothing
// else in this policy can start a second attempt — both timeouts are an hour out and the primary answers.
func TestRunHedgesAPrimaryTheDetectorWantedOutOfRotation(t *testing.T) {
	sim := newSimulator(t, speculativePolicy(2), 2, qwenModel)
	settings := config.Defaults()
	sim.perf.health = perf.NewTracker(config.NewHolder(&settings), sim.clock.Now)
	for range settings.Perf.ConsecutiveFailThreshold {
		for _, participant := range []string{"host-0", "host-1"} {
			sim.perf.health.RecordSample(perf.Sample{ParticipantKey: participant, Model: qwenModel})
		}
	}
	if sim.perf.health.Ejected("host-1", qwenModel) {
		t.Fatal("host-1 was withheld from routing, so the routing gate would already have covered it")
	}
	sim.host(10, 1, "host-1", &hostScript{
		receipt:   true,
		chunks:    []string{roleEvent, contentEvent("hedged")},
		confirmed: true,
		finished:  true,
	})
	sim.host(11, 0, "host-0", &hostScript{receipt: true, err: errors.New("host refused")})

	if _, err := sim.run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	reported := sim.reported(t)
	sim.settleAll()

	if reported.Decision != StartPrimaryDegraded {
		t.Fatalf("decision = %q, want %q", reported.Decision, StartPrimaryDegraded)
	}
	if len(reported.Attempts) != 2 {
		t.Fatalf("attempts = %d, want the primary and the hedge it earned", len(reported.Attempts))
	}
}

func TestTheRecordingPointAccountsEveryRaceExactlyOnce(t *testing.T) {
	sim := newSimulator(t, settledPolicy(), 1, qwenModel)
	sim.host(10, 0, "host-0", &hostScript{receipt: true, err: errors.New("host refused")})

	if _, err := sim.run(context.Background()); err == nil {
		t.Fatal("Run() error = nil, want the failed attempt's error")
	}
	reported := sim.reported(t)
	sim.settleAll()

	select {
	case accounted := <-sim.ledger.rows:
		if accounted.RequestID != reported.RequestID || accounted.WinnerNonce != reported.WinnerNonce {
			t.Fatalf("ledger saw %+v, want the reported outcome %+v", accounted, reported)
		}
		if len(accounted.Attempts) != len(reported.Attempts) {
			t.Fatalf("ledger saw %d attempts, want %d", len(accounted.Attempts), len(reported.Attempts))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the race was never accounted")
	}
	if extra := len(sim.ledger.rows); extra != 0 {
		t.Fatalf("rows accounted = %d, want 1", 1+extra)
	}
}

func TestCrownStrikesDenyOnlyAfterRepeatedContentlessAnswers(t *testing.T) {
	t.Parallel()
	gate := newCrownStrikes()

	for range crownDenialStrikes - 1 {
		gate.Observe(testParticipant, testModel, true)
		if gate.Denied(testParticipant, testModel) {
			t.Fatal("crowning denied before the strike threshold")
		}
	}
	gate.Observe(testParticipant, testModel, true)

	if !gate.Denied(testParticipant, testModel) {
		t.Fatal("crowning still allowed at the strike threshold")
	}
	if gate.Denied(testParticipant, "another-model") {
		t.Error("a strike on one model denied crowning on another")
	}

	gate.Observe(testParticipant, testModel, false)

	if gate.Denied(testParticipant, testModel) {
		t.Error("a content-bearing answer did not restore crowning")
	}
}

func TestOutcomeFailureNamesWhatTheClientLost(t *testing.T) {
	t.Parallel()
	streamedThenFailed := failedAttempt(TerminalUnexpectedEOF)
	streamedThenFailed.ContentChunks = 4

	hostRefused := failedAttempt(TerminalErrorStream)
	hostRefused.ErrorSource = "error.BadRequestError"
	hostRefused.ErrorType = "BadRequestError"
	hostRefused.ErrorMessage = "model does not exist"

	cases := []struct {
		name    string
		outcome RaceOutcome
		want    error
	}{
		{name: "a_served_race_has_no_failure", outcome: race(cleanAttempt())},
		{
			name:    "no_attempt_ever_started",
			outcome: RaceOutcome{Model: testModel},
			want:    ErrAllAttemptsFailed,
		},
		{
			name:    "the_winner_broke_after_streaming",
			outcome: failedRace(streamedThenFailed),
			want:    ErrWinnerIncomplete,
		},
		{
			name:    "the_host_refused_in_words_the_client_must_see",
			outcome: failedRace(hostRefused),
			want:    &HostApplicationError{Type: "BadRequestError", Message: "model does not exist"},
		},
		{
			name:    "every_host_answered_with_nothing",
			outcome: failedRace(failedAttempt(TerminalEmptyStream)),
			want:    ErrEmptyStream,
		},
		{
			name:    "nothing_upstream_explained_itself",
			outcome: failedRace(failedAttempt(TerminalDialFailure)),
			want:    ErrAllAttemptsFailed,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			got := testCase.outcome.failure()

			var hostErr *HostApplicationError
			if errors.As(testCase.want, &hostErr) {
				var found *HostApplicationError
				if !errors.As(got, &found) || found.Message != hostErr.Message || found.Type != hostErr.Type {
					t.Fatalf("failure() = %v, want host error %v", got, hostErr)
				}
				return
			}
			if !errors.Is(got, testCase.want) {
				t.Fatalf("failure() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func failedRace(attempt AttemptOutcome) RaceOutcome {
	outcome := race(attempt)
	outcome.Succeeded = false
	return outcome
}

// A vote can fail because the network blinked or because the hosts no longer have the escrow. Only the
// second is permanent, and it is the one that leaves the nonce paying its full reserve at settlement.
// The verifier's own error never reaches this far, so the fact comes from the attempts instead.
func TestSettleTimeoutsSeparatesAGoneEscrowFromACollectionFailure(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		escrowMissing bool
		wantReason    string
	}{
		{name: "the hosts still have the escrow", wantReason: timeoutReasonCollectionError},
		{name: "the hosts have dropped the escrow", escrowMissing: true, wantReason: timeoutReasonEscrowGone},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			handler := &scriptedTimeoutHandler{
				result: user.TimeoutResult{Reason: "refused"},
				err:    fmt.Errorf("collect timeout votes: %w", errVoteCause),
			}
			outcome := race(failedAttempt(TerminalDialFailure))
			outcome.Lifecycle.EscrowMissing = testCase.escrowMissing

			events := SettleTimeouts(context.Background(), &SessionTimeouts{handler: handler}, outcome)

			settled := events[len(events)-1]
			if settled.Action != TimeoutActionFailed {
				t.Fatalf("action = %q, want %q", settled.Action, TimeoutActionFailed)
			}
			if settled.Reason != testCase.wantReason {
				t.Fatalf("reason = %q, want %q", settled.Reason, testCase.wantReason)
			}
		})
	}
}

// A vote that succeeded must not be renamed by an escrow that went missing for some other attempt.
func TestAGoneEscrowDoesNotRenameASettledVote(t *testing.T) {
	t.Parallel()
	handler := &scriptedTimeoutHandler{
		result: user.TimeoutResult{Reason: "execution", Applied: true},
		err:    fmt.Errorf("inference %d timed out: %s", 7, "execution"),
	}
	outcome := race(failedAttempt(TerminalDialFailure))
	outcome.Lifecycle.EscrowMissing = true

	events := SettleTimeouts(context.Background(), &SessionTimeouts{handler: handler}, outcome)

	settled := events[len(events)-1]
	if settled.Action != TimeoutActionCompleted {
		t.Fatalf("action = %q, want %q", settled.Action, TimeoutActionCompleted)
	}
	if settled.Reason == timeoutReasonEscrowGone {
		t.Fatal("a settled vote was named as a gone escrow")
	}
}
