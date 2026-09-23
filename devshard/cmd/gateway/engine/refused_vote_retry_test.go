package engine

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/user"
)

type failingPoster struct {
	mu       sync.Mutex
	failures []error
	posts    int
}

func (p *failingPoster) VoteDeadline(uint64, time.Time) time.Time { return time.Time{} }

func (p *failingPoster) SettleTimeout(context.Context, TimeoutStep) (TimeoutVote, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.posts++
	if len(p.failures) == 0 {
		return TimeoutVote{Kind: TimeoutKindRefused}, nil
	}
	failure := p.failures[0]
	p.failures = p.failures[1:]
	return TimeoutVote{}, failure
}

func (p *failingPoster) postCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.posts
}

func alwaysFailing(count int) []error {
	failures := make([]error, count)
	for position := range failures {
		failures[position] = fmt.Errorf("send timeout diff: %w", errVoteCause)
	}
	return failures
}

func retryEngine(t *testing.T, poster TimeoutPoster) (*Engine, *queueHarness) {
	t.Helper()
	deps := completeEngineDeps(t)
	deps.Timeouts = func(string, any) (TimeoutPoster, bool) { return poster, true }
	races, err := NewEngine(deps)
	require.NoError(t, err)
	harness := newQueueHarness()
	races.settles = harness.queue
	return races, harness
}

func (h *queueHarness) advance(by time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clock = h.clock.Add(by)
}

func awaitPosts(t *testing.T, poster *failingPoster, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return poster.postCount() == want }, 2*time.Second, time.Millisecond)
}

func awaitScheduled(t *testing.T, harness *queueHarness, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return len(harness.scheduledDelays()) == want }, 2*time.Second, time.Millisecond)
}

func awaitReleased(t *testing.T, registration *raceRegistration) {
	t.Helper()
	require.Eventually(t, registration.released.Load, 2*time.Second, time.Millisecond,
		"the race's registration must be released after its last post")
}

func TestAFailedRefusedVoteIsPostedAgainAfterTheFirstDelay(t *testing.T) {
	poster := &failingPoster{failures: alwaysFailing(1)}
	races, harness := retryEngine(t, poster)
	registration := races.admit()

	races.settle(race(unsettledAttempt()), nil, registration)
	harness.fire(0)
	awaitPosts(t, poster, 1)
	awaitScheduled(t, harness, 2)

	require.Equal(t, 30*time.Second, harness.scheduledDelays()[1], "the first retry waits the first delay")
	require.False(t, registration.released.Load(), "a race with a retry still owed keeps its registration")
	require.Eventually(t, func() bool { return races.OwedTimeoutVotes() == 1 }, 2*time.Second, time.Millisecond,
		"a scheduled retry is owed")

	harness.advance(30 * time.Second)
	harness.fire(1)
	awaitPosts(t, poster, 2)
	awaitReleased(t, registration)
	require.Len(t, harness.scheduledDelays(), 2, "a vote that landed is not retried")
	require.Eventually(t, func() bool { return races.OwedTimeoutVotes() == 0 }, 2*time.Second, time.Millisecond)
}

func TestAVoteARetryCannotChangeIsNotRetried(t *testing.T) {
	executionAttempt := unsettledAttempt()
	executionAttempt.ReceiptTime = testEpoch.Add(200 * time.Millisecond)
	cases := []struct {
		name          string
		attempt       AttemptOutcome
		escrowMissing bool
		failure       error
	}{
		{name: "the group judged the vote", attempt: unsettledAttempt(),
			failure: fmt.Errorf("inference 7: %w: insufficient votes", user.ErrTimeoutNotApplied)},
		{name: "the hosts dropped the escrow", attempt: unsettledAttempt(), escrowMissing: true,
			failure: fmt.Errorf("collect timeout votes: %w", errVoteCause)},
		{name: "an execution vote is the sweep's", attempt: executionAttempt,
			failure: fmt.Errorf("collect timeout votes: %w", errVoteCause)},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			poster := &failingPoster{failures: []error{testCase.failure}}
			races, harness := retryEngine(t, poster)
			registration := races.admit()
			outcome := race(testCase.attempt)
			outcome.Lifecycle.EscrowMissing = testCase.escrowMissing

			races.settle(outcome, nil, registration)
			harness.fire(0)
			awaitPosts(t, poster, 1)
			awaitReleased(t, registration)

			require.Len(t, harness.scheduledDelays(), 1, "nothing is scheduled for a vote a retry cannot change")
			require.Eventually(t, func() bool { return races.OwedTimeoutVotes() == 0 }, 2*time.Second, time.Millisecond)
		})
	}
}

func TestARefusedVoteIsRetriedFourTimesAtMost(t *testing.T) {
	poster := &failingPoster{failures: alwaysFailing(10)}
	races, harness := retryEngine(t, poster)
	registration := races.admit()

	races.settle(race(unsettledAttempt()), nil, registration)
	for round := range 4 {
		harness.advance(4 * time.Minute)
		harness.fire(round)
		awaitPosts(t, poster, round+1)
		awaitScheduled(t, harness, round+2)
		require.False(t, registration.released.Load(), "round %d still owes a retry", round)
	}
	harness.advance(4 * time.Minute)
	harness.fire(4)
	awaitPosts(t, poster, 5)
	awaitReleased(t, registration)

	delays := harness.scheduledDelays()
	require.Equal(t, []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second, 240 * time.Second}, delays[1:])
	require.Eventually(t, func() bool { return races.OwedTimeoutVotes() == 0 }, 2*time.Second, time.Millisecond)
	require.NotPanics(t, registration.release, "a second release must be a no-op")
	races.Stop()
}

func TestStopPostsAWaitingRetryAtOnceAndSchedulesNoMore(t *testing.T) {
	poster := &failingPoster{failures: alwaysFailing(10)}
	races, harness := retryEngine(t, poster)
	registration := races.admit()

	races.settle(race(unsettledAttempt()), nil, registration)
	harness.fire(0)
	awaitScheduled(t, harness, 2)

	stopped := make(chan struct{})
	go func() { races.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop waited on a retry's timer")
	}

	require.Equal(t, 2, poster.postCount(), "the waiting retry is posted once at Stop")
	require.Len(t, harness.scheduledDelays(), 2, "a stopped engine schedules no further retry")
	require.True(t, registration.released.Load())
}

func TestARetryRoundReportsItsOwnStartedAndFinishedEvents(t *testing.T) {
	poster := &failingPoster{failures: alwaysFailing(1)}
	steps := race(unsettledAttempt()).TimeoutPlan()
	var events []TimeoutEvent

	retries := postTimeouts(context.Background(), poster, steps, false, func(event TimeoutEvent) { events = append(events, event) })

	require.Len(t, retries, 1, "a failed refused vote is handed back for the next round")
	require.Len(t, events, 2)
	require.Equal(t, TimeoutActionStarted, events[0].Action)
	require.Equal(t, TimeoutActionFailed, events[1].Action)
}
