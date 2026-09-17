package scheduler

import (
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
)

// forecastGates is the ladder as the pick reads it, with the window rung a test can move.
func forecastGates(blocked ...string) availability {
	refused := asSet(blocked)
	gates := openAvailability()
	gates.congested = windowFullWhen(func(participant string) bool { return refused[participant] })
	return gates
}

func forecastEscrow(latestNonce uint64, slots ...string) Escrow {
	return Escrow{ID: "escrow-forecast", Model: modelA, Session: &fakeSession{latestNonce: latestNonce, slots: slots}}
}

// The distance from the cursor to the first host that can take the request is what this escrow will spend on nobody.
func TestTheForecastCountsTheNoncesBetweenTheCursorAndAUsableHost(t *testing.T) {
	t.Parallel()

	group := []string{"host-0", "host-1", "host-2", "host-3"}
	testCases := []struct {
		name        string
		latestNonce uint64
		blocked     []string
		excluded    []string
		queuedAhead uint64
		want        int
	}{
		{
			name:        "the next nonce already names a host that can take it",
			latestNonce: 3,
			want:        0,
		},
		{
			name:        "one blocked host costs the one nonce bound to it",
			latestNonce: 3,
			blocked:     []string{"host-0"},
			want:        1,
		},
		{
			name:        "the walk crosses every blocked host in turn",
			latestNonce: 3,
			blocked:     []string{"host-0", "host-1", "host-2"},
			want:        3,
		},
		{
			name:        "a group nothing can serve is priced at one lap, not at infinity",
			latestNonce: 3,
			blocked:     group,
			want:        4,
		},
		{
			name:        "the walk wraps past the end of the group",
			latestNonce: 2,
			blocked:     []string{"host-3", "host-0"},
			want:        2,
		},
		{
			name:        "waiters already queued spend the nonces ahead of ours",
			latestNonce: 3,
			queuedAhead: 2,
			blocked:     []string{"host-2"},
			want:        1,
		},
		{
			name:        "a host this request excluded is no landing place either",
			latestNonce: 3,
			excluded:    []string{"host-0"},
			want:        1,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			queued := newWaiter(RequestProfile{Model: modelA, Exclude: testCase.excluded}, time.Time{})

			got := expectedBurns(forecastEscrow(testCase.latestNonce, group...), forecastGates(testCase.blocked...), queued, testCase.queuedAhead)

			if got != testCase.want {
				t.Fatalf("expectedBurns = %v, want %v", got, testCase.want)
			}
		})
	}
}

// An escrow whose group cannot be read is priced neutrally rather than on a guess.
func TestTheForecastIsNeutralWithoutAGroupToWalk(t *testing.T) {
	t.Parallel()
	queued := newWaiter(RequestProfile{Model: modelA}, time.Time{})

	if got := expectedBurns(Escrow{ID: "no-session"}, forecastGates(), queued, 0); got != 0 {
		t.Fatalf("expectedBurns without a session = %v, want 0", got)
	}
	if got := expectedBurns(forecastEscrow(3), forecastGates(), queued, 0); got != 0 {
		t.Fatalf("expectedBurns over an empty group = %v, want 0", got)
	}
}

// Same weight and same in-flight count, so only the cursor's distance separates these two.
func TestThePickPrefersTheEscrowThatServesOnItsNextNonce(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "far", activeUsers: 1, weight: 10, latestNonce: 3, slots: []string{"far-0", "far-1", "far-2", "far-3"}},
		candidate{id: "near", activeUsers: 1, weight: 10, latestNonce: 3, slots: []string{"near-0", "near-1", "near-2", "near-3"}},
	)
	limiter := scheduler.limiter.(*fakeLimiter)
	for _, blocked := range []string{"far-0", "far-1", "far-2"} {
		limiter.block(blocked)
	}

	picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

	if err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}
	if picked.ID != "near" {
		t.Fatalf("picked %q, want the escrow whose next nonce lands on a usable host", picked.ID)
	}
}

// The forecast ranks and never refuses: the drain's sweep answers a busy group without spending a nonce.
func TestAnEscrowWhoseWholeGroupIsBusyIsStillPicked(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "only", activeUsers: 0, weight: 10, latestNonce: 3, slots: []string{"only-0", "only-1"}},
	)
	limiter := scheduler.limiter.(*fakeLimiter)
	limiter.block("only-0")
	limiter.block("only-1")

	picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

	if err != nil {
		t.Fatalf("pickEscrow = %v, want the busy escrow so the drain can answer for it", err)
	}
	if picked.ID != "only" {
		t.Fatalf("picked %q, want only", picked.ID)
	}
}

// The forecast is counted in the same unit as the load, so a burn and a request in flight trade against each other.
func TestABurnCostsTheSameAsARequestAlreadyInFlight(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name              string
		loadedActiveUsers int
		want              string
	}{
		{name: "two burns are cheaper than three requests in flight", loadedActiveUsers: 3, want: "burning"},
		{name: "two burns are dearer than one request in flight", loadedActiveUsers: 1, want: "loaded"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, _, _ := newScheduler(
				candidate{id: "burning", activeUsers: 0, weight: 10, latestNonce: 3, slots: []string{"burning-0", "burning-1", "burning-2", "burning-3"}},
				candidate{id: "loaded", activeUsers: testCase.loadedActiveUsers, weight: 10, latestNonce: 3, slots: []string{"loaded-0", "loaded-1"}},
			)
			limiter := scheduler.limiter.(*fakeLimiter)
			limiter.block("burning-0")
			limiter.block("burning-1")

			picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

			if err != nil {
				t.Fatalf("pickEscrow: %v", err)
			}
			if picked.ID != testCase.want {
				t.Fatalf("picked %q, want %q", picked.ID, testCase.want)
			}
		})
	}
}

// The forecast must read the whole ladder the drain reads, not the congestion rung alone: a host the chain has
// stopped, one perf ejected, and one this escrow blocked all cost a nonce to step past exactly like a full window.
func TestTheForecastStepsOverEveryRungTheDrainReads(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		block func(scheduler *Scheduler, participant string)
	}{
		{
			name:  "a full congestion window",
			block: func(scheduler *Scheduler, participant string) { scheduler.limiter.(*fakeLimiter).block(participant) },
		},
		{
			name: "a cut-off host",
			block: func(scheduler *Scheduler, participant string) {
				scheduler.limiter.(*fakeLimiter).cutOffHost(participant)
			},
		},
		{
			name:  "an ejected host",
			block: func(scheduler *Scheduler, participant string) { scheduler.perf.(*fakePerf).eject(participant) },
		},
		{
			name:  "a host this escrow blocked for diverging",
			block: func(scheduler *Scheduler, participant string) { scheduler.BlockHost("far", participant) },
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, _, _ := newScheduler(
				candidate{id: "far", activeUsers: 1, weight: 10, latestNonce: 3, slots: []string{"far-0", "far-1", "far-2", "far-3"}},
				candidate{id: "near", activeUsers: 1, weight: 10, latestNonce: 3, slots: []string{"near-0", "near-1", "near-2", "near-3"}},
			)
			for _, blocked := range []string{"far-0", "far-1", "far-2"} {
				testCase.block(scheduler, blocked)
			}

			picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

			if err != nil {
				t.Fatalf("pickEscrow: %v", err)
			}
			if picked.ID != "near" {
				t.Fatalf("picked %q, want the escrow the forecast can reach", picked.ID)
			}
		})
	}
}

// One escrow's divergence block is not another's, so the forecast must ask it per candidate rather than once for
// the fleet: a shared answer would price every escrow against the blocks of whichever one was walked first.
func TestADivergenceBlockOnlyCostsTheEscrowThatCarriesIt(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "blocked", activeUsers: 0, weight: 10, latestNonce: 3, slots: []string{"shared-0", "shared-1"}},
		candidate{id: "clean", activeUsers: 0, weight: 10, latestNonce: 3, slots: []string{"shared-0", "shared-1"}},
	)
	scheduler.BlockHost("blocked", "shared-0")
	scheduler.BlockHost("blocked", "shared-1")

	picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

	if err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}
	if picked.ID != "clean" {
		t.Fatalf("picked %q, want the escrow that did not block these hosts", picked.ID)
	}
}

// The waiters a dispatcher already holds draw their nonces first, so the cursor the pick forecasts from is past
// them. Reading that count from the live registry is what the escrow score actually depends on.
func TestThePickReadsAQueuesDepthFromTheLiveDispatcher(t *testing.T) {
	t.Parallel()
	scheduler, escrows, _ := newScheduler(
		candidate{id: "queued", activeUsers: 0, weight: 10, latestNonce: 3, slots: []string{"queued-0", "queued-1", "queued-2", "queued-3"}},
		candidate{id: "quiet", activeUsers: 1, weight: 10, latestNonce: 3, slots: []string{"quiet-0", "quiet-1", "quiet-2", "quiet-3"}},
	)
	scheduler.limiter.(*fakeLimiter).block("queued-1")
	scheduler.limiter.(*fakeLimiter).block("queued-2")

	if picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{}); err != nil || picked.ID != "queued" {
		t.Fatalf("picked %q (%v) before any queue, want the idle escrow whose cursor is clear", picked.ID, err)
	}

	queuedEscrow := escrows.byModel[modelA][0]
	waiting := newDispatcher(dispatcherDeps{escrowID: queuedEscrow.ID, sessionID: queuedEscrow.SessionID})
	waiting.pendingSubmits.Store(1)
	scheduler.dispatchers[queuedEscrow.ID] = waiting

	picked, err := pickWith(scheduler, RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

	if err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}
	if picked.ID != "quiet" {
		t.Fatalf("picked %q, want the escrow whose cursor is not behind a queue", picked.ID)
	}
}
