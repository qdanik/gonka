package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"devshard/user"
)

type recordedPost struct {
	nonce  uint64
	sentAt time.Time
}

type stubPoster struct {
	posts []recordedPost
	vote  string
	err   error
}

func (p *stubPoster) SettleTimeout(_ context.Context, nonce uint64, sentAt time.Time) (string, error) {
	p.posts = append(p.posts, recordedPost{nonce: nonce, sentAt: sentAt})
	return p.vote, p.err
}

func unsettledAttempt() AttemptOutcome {
	attempt := failedAttempt(TerminalNoReceipt)
	attempt.ReceiptTime = time.Time{}
	attempt.FirstToken = time.Time{}
	return attempt
}

func settleEvents(outcome RaceOutcome, poster TimeoutPoster) []TimeoutEvent {
	return SettleTimeouts(context.Background(), poster, outcome)
}

func TestTimeoutLadderPostsWhenNoSkipConditionHolds(t *testing.T) {
	poster := &stubPoster{vote: "refused"}

	events := settleEvents(race(unsettledAttempt()), poster)

	if len(poster.posts) != 1 {
		t.Fatalf("posts = %d, want 1", len(poster.posts))
	}
	if poster.posts[0].nonce != 7 || !poster.posts[0].sentAt.Equal(testEpoch) {
		t.Fatalf("post = %+v, want nonce 7 waited out from the record's StartedAt", poster.posts[0])
	}
	if len(events) != 2 {
		t.Fatalf("events = %+v, want a started and a completed event", events)
	}
	if events[0].Action != TimeoutActionStarted || events[1].Action != TimeoutActionCompleted {
		t.Fatalf("actions = %q then %q, want started then completed", events[0].Action, events[1].Action)
	}
}

func TestTimeoutLadderSkipConditions(t *testing.T) {
	// ContentSource is what makes these "after content": ContentChunks alone counts error events too.
	longResponse := unsettledAttempt()
	longResponse.ContentChunks = 3
	longResponse.ContentSource = "delta.content"
	longResponse.Completed = testEpoch.Add(longResponseExemption)

	justUnderLongResponse := unsettledAttempt()
	justUnderLongResponse.ContentChunks = 3
	justUnderLongResponse.ContentSource = "delta.content"
	justUnderLongResponse.Completed = testEpoch.Add(longResponseExemption - time.Nanosecond)

	stateDivergent := unsettledAttempt()
	stateDivergent.StateDivergent = true

	phaseAborted := unsettledAttempt()
	phaseAborted.PhaseTransitionAborted = true

	emptyFinished := unsettledAttempt()
	emptyFinished.Terminal = TerminalEmptyStream
	emptyFinished.ReceiptTime = testEpoch.Add(time.Second)
	emptyFinished.NonceFinished = true

	errorStreamFinished := unsettledAttempt()
	errorStreamFinished.Terminal = TerminalErrorStream
	errorStreamFinished.ErrorSource = "error"
	errorStreamFinished.NonceFinished = true

	won := cleanAttempt()

	testCases := []struct {
		name       string
		attempt    AttemptOutcome
		wantPosted bool
		wantReason string
	}{
		{name: "no_skip_condition", attempt: unsettledAttempt(), wantPosted: true, wantReason: timeoutReasonNone},
		{name: "phase_transition_aborted", attempt: phaseAborted, wantReason: timeoutReasonPhaseAborted},
		{name: "empty_stream_already_finished", attempt: emptyFinished, wantReason: timeoutReasonEmptyStream},
		{name: "error_stream_already_finished", attempt: errorStreamFinished, wantReason: timeoutReasonNonceFinished},
		{name: "long_response_after_content", attempt: longResponse, wantReason: timeoutReasonLongResponse},
		{name: "just_under_long_response_exemption", attempt: justUnderLongResponse, wantPosted: true, wantReason: timeoutReasonNone},
		{name: "state_divergent_host_is_still_settled", attempt: stateDivergent, wantPosted: true, wantReason: timeoutReasonNone},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			poster := &stubPoster{}

			events := settleEvents(race(testCase.attempt), poster)

			if posted := len(poster.posts) == 1; posted != testCase.wantPosted {
				t.Fatalf("posted = %v, want %v", posted, testCase.wantPosted)
			}
			if len(events) == 0 {
				t.Fatalf("events = %+v, want at least one", events)
			}
			if events[0].Reason != testCase.wantReason {
				t.Fatalf("reason = %q, want %q", events[0].Reason, testCase.wantReason)
			}
		})
	}

	for _, testCase := range []struct {
		name    string
		attempt AttemptOutcome
	}{
		{name: "attempt_that_finished_its_nonce", attempt: won},
	} {
		t.Run(testCase.name+"_is_not_in_the_plan", func(t *testing.T) {
			t.Parallel()
			poster := &stubPoster{}

			events := settleEvents(race(testCase.attempt), poster)

			if len(poster.posts) != 0 {
				t.Fatalf("posts = %d, want 0", len(poster.posts))
			}
			if len(events) != 0 {
				t.Fatalf("events = %+v, want none", events)
			}
		})
	}
}

func TestTimeoutSkipLadderOrder(t *testing.T) {
	attempt := unsettledAttempt()
	attempt.PhaseTransitionAborted = true
	attempt.Terminal = TerminalEmptyStream
	attempt.ReceiptTime = testEpoch.Add(time.Second)
	attempt.NonceFinished = true
	attempt.ContentChunks = 3
	attempt.Completed = testEpoch.Add(longResponseExemption)

	events := settleEvents(race(attempt), &stubPoster{})

	if events[0].Reason != timeoutReasonPhaseAborted {
		t.Fatalf("reason = %q, want the first matching rung %q", events[0].Reason, timeoutReasonPhaseAborted)
	}
}

func TestTimeoutKindFollowsReceipt(t *testing.T) {
	received := unsettledAttempt()
	received.ReceiptTime = testEpoch.Add(300 * time.Millisecond)

	testCases := []struct {
		name     string
		attempt  AttemptOutcome
		wantKind string
	}{
		{name: "never_receipted", attempt: unsettledAttempt(), wantKind: timeoutKindRefused},
		{name: "receipted_then_failed", attempt: received, wantKind: timeoutKindExecution},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			events := settleEvents(race(testCase.attempt), &stubPoster{})

			if events[0].Kind != testCase.wantKind {
				t.Fatalf("kind = %q, want %q", events[0].Kind, testCase.wantKind)
			}
		})
	}
}

func TestTimeoutVoteOverridesKind(t *testing.T) {
	poster := &stubPoster{vote: "execution"}

	events := settleEvents(race(unsettledAttempt()), poster)

	if events[0].Kind != timeoutKindRefused {
		t.Fatalf("started kind = %q, want %q", events[0].Kind, timeoutKindRefused)
	}
	if events[1].Kind != timeoutKindExecution {
		t.Fatalf("completed event = %+v, want the posted vote to set the kind", events[1])
	}
}

func TestTimeoutPostFailureIsReported(t *testing.T) {
	poster := &stubPoster{vote: "refused", err: errors.New("collect timeout votes")}

	events := settleEvents(race(unsettledAttempt()), poster)

	if events[1].Action != TimeoutActionFailed || events[1].Reason != timeoutReasonCollectionError {
		t.Fatalf("failure event = %+v, want a failed action with the collection reason", events[1])
	}
}

func TestEveryUnsettledNonceIsPostedOnce(t *testing.T) {
	first := unsettledAttempt()
	first.Nonce = 11
	second := unsettledAttempt()
	second.Nonce = 12
	second.Participant = "gonka1other"
	settled := cleanAttempt()
	settled.Nonce = 13
	outcome := race(first)
	outcome.Attempts = []AttemptOutcome{first, second, settled}

	poster := &stubPoster{}
	settleEvents(outcome, poster)

	if len(poster.posts) != 2 {
		t.Fatalf("posts = %+v, want one per unsettled nonce", poster.posts)
	}
	if poster.posts[0].nonce != 11 || poster.posts[1].nonce != 12 {
		t.Fatalf("posts = %+v, want nonces 11 and 12", poster.posts)
	}
}

func TestTimeoutEventCarriesRaceIdentity(t *testing.T) {
	events := settleEvents(race(unsettledAttempt()), &stubPoster{})

	if events[0].Participant != testParticipant || events[0].Model != testModel || events[0].Nonce != 7 {
		t.Fatalf("event = %+v, want the attempt's participant, the race model and the nonce", events[0])
	}
}

// A host that emits one SSE error event and then holds the stream open past the long-response exemption
// leaves its nonce committed, unfinished, and unvoted: the exemption counts error chunks as content, so
// the timeout vote it exists to defer is dropped instead. Nothing else sweeps an unfinished nonce, so
// the escrow can never reclaim what that nonce cost. See gateway-invariants.md, invariant 1.
func TestAnErrorOnlyStreamStillVotesItsTimeout(t *testing.T) {
	attempt := failedAttempt(TerminalErrorStream)
	attempt.ContentChunks = 1 // the error event itself, counted as a chunk
	attempt.ContentSource = ""
	attempt.Completed = attempt.StartedAt.Add(longResponseExemption)
	outcome := RaceOutcome{Attempts: []AttemptOutcome{attempt}}

	plan := outcome.TimeoutPlan()

	if len(plan) != 1 {
		t.Fatalf("plan has %d steps, want the unfinished nonce in it", len(plan))
	}
	if !plan[0].Post {
		t.Fatal("timeout vote skipped: the nonce is committed, unfinished and now unreclaimable")
	}
}

// The exemption it must not break: a host genuinely streaming content for longer than the window is
// still working, and voting a timeout against it would settle a race that has not finished.
func TestALongRunningContentStreamKeepsItsExemption(t *testing.T) {
	attempt := failedAttempt(TerminalStreamTruncated)
	attempt.ContentChunks = 40
	attempt.ContentSource = "delta.content"
	attempt.Completed = attempt.StartedAt.Add(longResponseExemption)
	outcome := RaceOutcome{Attempts: []AttemptOutcome{attempt}}

	plan := outcome.TimeoutPlan()

	if len(plan) != 1 || plan[0].Post {
		t.Fatalf("a long content stream lost its exemption: %+v", plan)
	}
}

// A nonce the host finished during the wait owes no timeout. Reported as a collection failure it
// would read as a broken vote, and the verifiers that refused it were right to.
func TestANonceFinishedWhileWaitingIsSkippedRatherThanFailed(t *testing.T) {
	poster := &stubPoster{vote: "execution", err: user.ErrNonceFinishedWhileWaiting}

	events := settleEvents(race(unsettledAttempt()), poster)

	if events[1].Action != TimeoutActionSkipped || events[1].Reason != timeoutReasonNonceFinished {
		t.Fatalf("event = %+v, want skipped/%s", events[1], timeoutReasonNonceFinished)
	}
}
