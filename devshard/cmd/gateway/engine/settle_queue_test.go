package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/config"
)

// queueHarness fires the queue's timers on command, so nothing in a test waits on the wall clock.
type queueHarness struct {
	queue  *settleQueue
	posted chan string

	mu      sync.Mutex
	clock   time.Time
	pending []scheduled
}

type scheduled struct {
	delay time.Duration
	fire  func()
}

func newQueueHarness() *queueHarness {
	harness := &queueHarness{posted: make(chan string, 32), clock: time.Unix(1_700_000_000, 0)}
	harness.queue = newSettleQueue(harness.now)
	harness.queue.after = harness.schedule
	return harness
}

func (h *queueHarness) now() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.clock
}

func (h *queueHarness) schedule(delay time.Duration, fire func()) *time.Timer {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pending = append(h.pending, scheduled{delay: delay, fire: fire})
	return nil
}

func (h *queueHarness) scheduledDelays() []time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	delays := make([]time.Duration, 0, len(h.pending))
	for _, entry := range h.pending {
		delays = append(delays, entry.delay)
	}
	return delays
}

// fire runs a scheduled timer the way the runtime does, on a goroutine of its own.
func (h *queueHarness) fire(position int) {
	h.mu.Lock()
	entry := h.pending[position]
	h.mu.Unlock()
	go entry.fire()
}

func (h *queueHarness) add(name string, dueIn time.Duration, limit int) {
	deadline := h.now().Add(dueIn)
	h.queue.Add(settleTask{
		deadline: func() time.Time { return deadline },
		post:     func() { h.posted <- name },
	}, limit)
}

// blocking adds a vote whose post holds its poster until the returned release is called.
func (h *queueHarness) blocking(name string, dueIn time.Duration, limit int) (release func()) {
	held := make(chan struct{})
	deadline := h.now().Add(dueIn)
	h.queue.Add(settleTask{
		deadline: func() time.Time { return deadline },
		post:     func() { h.posted <- name; <-held },
	}, limit)
	return func() { close(held) }
}

func (h *queueHarness) nextPosted(t *testing.T) string {
	t.Helper()
	select {
	case name := <-h.posted:
		return name
	case <-time.After(2 * time.Second):
		t.Fatal("no vote was posted")
		return ""
	}
}

func (h *queueHarness) assertNothingPosted(t *testing.T) {
	t.Helper()
	select {
	case name := <-h.posted:
		t.Fatalf("posted %q, want nothing yet", name)
	case <-time.After(50 * time.Millisecond):
	}
}

// Waiting for a deadline must cost a timer and no goroutine.
func TestAVoteWaitsOnATimerSetToItsOwnDeadline(t *testing.T) {
	harness := newQueueHarness()

	harness.add("execution-in-an-hour", time.Hour, 4)
	harness.add("refused-in-a-minute", time.Minute, 4)

	require.Equal(t, []time.Duration{time.Hour, time.Minute}, harness.scheduledDelays())
	harness.assertNothingPosted(t)
}

// A vote whose deadline has not come holds no poster, or one due in an hour would stall every vote behind it.
func TestAVoteStillWaitingHoldsNoPoster(t *testing.T) {
	harness := newQueueHarness()

	harness.add("due-in-an-hour", time.Hour, 1)
	harness.add("due-now", 0, 1)
	harness.fire(1)

	if posted := harness.nextPosted(t); posted != "due-now" {
		t.Fatalf("posted %q, want the due vote through the only poster", posted)
	}
}

// A vote past the limit waits for a place; it is never dropped, because only the vote undoes the charge.
func TestEveryVotePastTheLimitIsStillPosted(t *testing.T) {
	harness := newQueueHarness()
	names := []string{"first", "second", "third", "fourth", "fifth"}

	for _, name := range names {
		harness.add(name, 0, 1)
	}
	for position := range names {
		harness.fire(position)
	}

	posted := map[string]bool{}
	for range names {
		posted[harness.nextPosted(t)] = true
	}
	require.Len(t, posted, len(names), "every vote the queue was given must be posted")
	require.Zero(t, harness.queue.Owed(), "a posted vote is no longer owed")
}

// The limit is on posting, which is what reaches the hosts: one place means one vote in flight.
func TestTheLimitBoundsTheVotesInFlight(t *testing.T) {
	harness := newQueueHarness()

	release := harness.blocking("holding-the-place", 0, 1)
	harness.add("waiting-for-a-place", 0, 1)
	harness.fire(0)
	if posted := harness.nextPosted(t); posted != "holding-the-place" {
		t.Fatalf("posted %q, want the first vote", posted)
	}
	harness.fire(1)

	harness.assertNothingPosted(t)
	release()
	if posted := harness.nextPosted(t); posted != "waiting-for-a-place" {
		t.Fatalf("posted %q, want the queued vote once the place was free", posted)
	}
}

// duePoster answers that every vote may be posted at once, so a test measures the queue and not a deadline.
type duePoster struct {
	mu    sync.Mutex
	posts []uint64
}

func (p *duePoster) VoteDeadline(uint64, time.Time) time.Time { return time.Time{} }

func (p *duePoster) SettleTimeout(_ context.Context, nonce uint64, _ time.Time) (TimeoutVote, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.posts = append(p.posts, nonce)
	return TimeoutVote{Kind: TimeoutKindRefused}, nil
}

func (p *duePoster) posted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.posts)
}

// A shard that owes more votes than it may post at once still posts every one of them.
func TestNoVoteIsLostWhenRacesOweMoreThanTheLimit(t *testing.T) {
	poster := &duePoster{}
	settings := engineSettings(EscalationPolicy{}, config.Modes{})
	settings.Engine.MaxConcurrentTimeoutVotes = 1
	deps := completeEngineDeps(t)
	deps.Config = config.NewHolder(settings)
	deps.Timeouts = func(string, any) (TimeoutPoster, bool) { return poster, true }
	races, err := NewEngine(deps)
	require.NoError(t, err)

	for nonce := range uint64(6) {
		attempt := unsettledAttempt()
		attempt.Nonce = nonce + 11
		races.settle(race(attempt), nil, races.admit())
	}
	races.Stop()

	require.Equal(t, 6, poster.posted(), "every vote a race owed must reach the chain")
	require.Zero(t, races.settles.Owed(), "no vote may be left owed once the engine has stopped")
}

// A deadline that moved out must be waited out on a timer, not inside a poster.
func TestAVoteWhoseDeadlineMovedOutIsArmedAgainRatherThanPosted(t *testing.T) {
	harness := newQueueHarness()
	var deadline time.Time
	deadline = harness.now().Add(time.Minute)

	harness.queue.Add(settleTask{
		deadline: func() time.Time { return deadline },
		post:     func() { harness.posted <- "posted" },
	}, 1)
	deadline = harness.now().Add(time.Hour)
	harness.fire(0)

	harness.assertNothingPosted(t)
	require.Equal(t, []time.Duration{time.Minute, time.Hour}, harness.scheduledDelays(),
		"the vote must be armed again for the deadline it now has")
	deadline = harness.now().Add(-time.Second)
	harness.fire(1)
	require.Equal(t, "posted", harness.nextPosted(t))
}
