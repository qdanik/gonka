package scheduler

import (
	"errors"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

const modelA = "model-a"

type fakeSession struct {
	balance      uint64
	tokenPrice   uint64
	latestNonce  uint64
	groupSize    int
	participants []string
	calls        []string
}

func (f *fakeSession) Advance(func(HostBinding) NonceIntent) (Prepared, error) {
	f.calls = append(f.calls, "Advance")
	return nil, nil
}

func (f *fakeSession) ParticipantKeys() []string {
	f.calls = append(f.calls, "ParticipantKeys")
	return f.participants
}

func (f *fakeSession) GroupSize() int {
	f.calls = append(f.calls, "GroupSize")
	return f.groupSize
}

func (f *fakeSession) LatestNonce() uint64 {
	f.calls = append(f.calls, "LatestNonce")
	return f.latestNonce
}

type fakeEscrows struct {
	mu      sync.Mutex
	byModel map[string][]Escrow
	queries []string
}

func (f *fakeEscrows) Candidates(model string) []Escrow {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, model)
	return f.byModel[model]
}

type fakeWeights struct {
	mu       sync.Mutex
	byEscrow map[string]float64
	lookups  []string
}

func (f *fakeWeights) EscrowWeight(escrowID, model string) float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups = append(f.lookups, escrowID+"/"+model)
	return f.byEscrow[escrowID]
}

type candidate struct {
	id          string
	activeUsers int
	weight      float64
	latestNonce uint64
	groupSize   int
}

func newScheduler(candidates ...candidate) (*Scheduler, *fakeEscrows, *fakeWeights) {
	escrows := &fakeEscrows{byModel: map[string][]Escrow{}}
	weights := &fakeWeights{byEscrow: map[string]float64{}}
	for _, entry := range candidates {
		groupSize := entry.groupSize
		if groupSize == 0 {
			groupSize = 4
		}
		escrows.byModel[modelA] = append(escrows.byModel[modelA], Escrow{
			ID:          entry.id,
			Model:       modelA,
			Session:     &fakeSession{latestNonce: entry.latestNonce, groupSize: groupSize},
			ActiveUsers: entry.activeUsers,
		})
		weights.byEscrow[entry.id] = entry.weight
	}
	return &Scheduler{escrows: escrows, capacity: weights}, escrows, weights
}

func TestPickEscrowPinned(t *testing.T) {
	t.Parallel()

	t.Run("returns the pinned escrow directly", func(t *testing.T) {
		t.Parallel()
		// The pinned escrow is the worst by every score, and still wins because it is pinned.
		scheduler, _, weights := newScheduler(
			candidate{id: "escrow-1", activeUsers: 0, weight: 10},
			candidate{id: "escrow-2", activeUsers: 99, weight: 1, latestNonce: 19_000},
		)

		picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA, Escrow: "escrow-2"}, chain.PhaseSnapshot{})
		if err != nil {
			t.Fatalf("pickEscrow: %v", err)
		}
		if picked.ID != "escrow-2" {
			t.Fatalf("picked %q, want escrow-2", picked.ID)
		}
		if len(weights.lookups) != 0 {
			t.Fatalf("pinned pick weighed escrows: %v", weights.lookups)
		}
	})

	t.Run("errors when the pinned escrow is gone", func(t *testing.T) {
		t.Parallel()
		scheduler, _, _ := newScheduler(candidate{id: "escrow-1", weight: 10})

		_, err := scheduler.pickEscrow(RequestProfile{Model: modelA, Escrow: "escrow-gone"}, chain.PhaseSnapshot{})
		if !errors.Is(err, ErrEscrowGone) {
			t.Fatalf("err = %v, want ErrEscrowGone", err)
		}
	})
}

func TestPickEscrowLowestUtilisationWins(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		candidates []candidate
		want       string
	}{
		{
			name: "equal weight sorts by in-flight requests",
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 8, weight: 10},
				{id: "escrow-2", activeUsers: 2, weight: 10},
			},
			want: "escrow-2",
		},
		{
			name: "equal in-flight sorts by weight",
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 4, weight: 5},
				{id: "escrow-2", activeUsers: 4, weight: 20},
			},
			want: "escrow-2",
		},
		{
			name: "ratio beats raw weight",
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 90, weight: 100},
				{id: "escrow-2", activeUsers: 1, weight: 10},
			},
			want: "escrow-2",
		},
		{
			name: "ratio beats raw in-flight count",
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 1, weight: 1},
				{id: "escrow-2", activeUsers: 10, weight: 100},
			},
			want: "escrow-2",
		},
		{
			name: "an unusable escrow never wins over a loaded usable one",
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 0},
				{id: "escrow-2", activeUsers: 500, weight: 1},
			},
			want: "escrow-2",
		},
		{
			name: "a negative weight never wins",
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: -5},
				{id: "escrow-2", activeUsers: 500, weight: 1},
			},
			want: "escrow-2",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, _, _ := newScheduler(testCase.candidates...)

			picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})
			if err != nil {
				t.Fatalf("pickEscrow: %v", err)
			}
			if picked.ID != testCase.want {
				t.Fatalf("picked %q, want %q", picked.ID, testCase.want)
			}
		})
	}
}

func TestPickEscrowTieBreakAdvancesSharedCounter(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "escrow-1", activeUsers: 3, weight: 10},
		candidate{id: "escrow-2", activeUsers: 3, weight: 10},
	)
	profile := RequestProfile{Model: modelA}

	first, err := scheduler.pickEscrow(profile, chain.PhaseSnapshot{})
	if err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}
	second, err := scheduler.pickEscrow(profile, chain.PhaseSnapshot{})
	if err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}
	third, err := scheduler.pickEscrow(profile, chain.PhaseSnapshot{})
	if err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}

	if first.ID == second.ID {
		t.Fatalf("tie-break returned %q twice in a row", first.ID)
	}
	if third.ID != first.ID {
		t.Fatalf("third pick %q, want the counter to wrap back to %q", third.ID, first.ID)
	}
}

func TestPickEscrowTieBreakDoesNotAdvanceWithoutATie(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "escrow-1", activeUsers: 1, weight: 10},
		candidate{id: "escrow-2", activeUsers: 9, weight: 10},
	)
	profile := RequestProfile{Model: modelA}

	for attempt := 0; attempt < 3; attempt++ {
		picked, err := scheduler.pickEscrow(profile, chain.PhaseSnapshot{})
		if err != nil {
			t.Fatalf("pickEscrow: %v", err)
		}
		if picked.ID != "escrow-1" {
			t.Fatalf("attempt %d picked %q, want escrow-1", attempt, picked.ID)
		}
	}
}

func TestPickEscrowNonceCap(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		maxNonce   uint64
		candidates []candidate
		want       string
		wantErr    error
	}{
		{
			name:     "governance cap drops the capped escrow",
			maxNonce: 1_000,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: 995, groupSize: 4},
				{id: "escrow-2", activeUsers: 50, weight: 1, latestNonce: 10, groupSize: 4},
			},
			want: "escrow-2",
		},
		{
			name:     "governance cap keeps an escrow one nonce below the cutoff",
			maxNonce: 1_000,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: 994, groupSize: 4},
			},
			want: "escrow-1",
		},
		{
			name:     "governance cap drops an escrow exactly at the cutoff",
			maxNonce: 1_000,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: 995, groupSize: 4},
			},
			wantErr: ErrNoEscrowCapacity,
		},
		{
			name:     "unfetched cap keeps an escrow just below the fallback ceiling",
			maxNonce: 0,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: 19_799},
			},
			want: "escrow-1",
		},
		{
			name:     "unfetched cap drops an escrow at the fallback ceiling",
			maxNonce: 0,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: 19_800},
			},
			wantErr: ErrNoEscrowCapacity,
		},
		{
			name:     "unfetched cap drops an escrow above the fallback ceiling",
			maxNonce: 0,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: 19_801},
				{id: "escrow-2", activeUsers: 50, weight: 1, latestNonce: 5},
			},
			want: "escrow-2",
		},
		{
			name:     "an oversized cap clamps instead of wrapping",
			maxNonce: uint64(math.MaxUint32) + 1,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: math.MaxUint32, groupSize: 4},
			},
			wantErr: ErrNoEscrowCapacity,
		},
		{
			name:     "an oversized cap still admits an escrow below the clamped cutoff",
			maxNonce: uint64(math.MaxUint32) + 1,
			candidates: []candidate{
				{id: "escrow-1", activeUsers: 0, weight: 100, latestNonce: math.MaxUint32 - 10, groupSize: 4},
			},
			want: "escrow-1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, _, _ := newScheduler(testCase.candidates...)

			picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{MaxNonce: testCase.maxNonce})
			if testCase.wantErr != nil {
				if !errors.Is(err, testCase.wantErr) {
					t.Fatalf("err = %v, want %v", err, testCase.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("pickEscrow: %v", err)
			}
			if picked.ID != testCase.want {
				t.Fatalf("picked %q, want %q", picked.ID, testCase.want)
			}
		})
	}
}

func TestPickEscrowNoCapacity(t *testing.T) {
	t.Parallel()

	t.Run("every candidate at non-positive weight", func(t *testing.T) {
		t.Parallel()
		scheduler, _, _ := newScheduler(
			candidate{id: "escrow-1", activeUsers: 0, weight: 0},
			candidate{id: "escrow-2", activeUsers: 0, weight: -1},
		)

		_, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})
		if !errors.Is(err, ErrNoEscrowCapacity) {
			t.Fatalf("err = %v, want ErrNoEscrowCapacity", err)
		}
		for _, secret := range []string{"escrow-1", "escrow-2", modelA} {
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("error %q names %q", err, secret)
			}
		}
	})

	t.Run("empty candidate set", func(t *testing.T) {
		t.Parallel()
		scheduler, _, _ := newScheduler()

		_, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})
		if !errors.Is(err, ErrNoEscrowCapacity) {
			t.Fatalf("err = %v, want ErrNoEscrowCapacity", err)
		}
	})

	t.Run("no candidate for the requested model", func(t *testing.T) {
		t.Parallel()
		scheduler, _, _ := newScheduler(candidate{id: "escrow-1", weight: 10})

		_, err := scheduler.pickEscrow(RequestProfile{Model: "model-b"}, chain.PhaseSnapshot{})
		if !errors.Is(err, ErrNoEscrowCapacity) {
			t.Fatalf("err = %v, want ErrNoEscrowCapacity", err)
		}
	})
}

func TestPickEscrowTouchesOnlyEnumerationAndWeights(t *testing.T) {
	t.Parallel()
	scheduler, escrows, weights := newScheduler(
		candidate{id: "escrow-1", activeUsers: 1, weight: 10},
		candidate{id: "escrow-2", activeUsers: 9, weight: 10, latestNonce: 19_900},
	)

	if _, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{}); err != nil {
		t.Fatalf("pickEscrow: %v", err)
	}

	if len(escrows.queries) != 1 || escrows.queries[0] != modelA {
		t.Fatalf("candidate enumeration = %v, want one query for %q", escrows.queries, modelA)
	}
	// The nonce-capped escrow must be dropped without being weighed, and without any rotation or
	// replacement side effect reaching its session.
	if len(weights.lookups) != 1 || weights.lookups[0] != "escrow-1/"+modelA {
		t.Fatalf("weight lookups = %v, want only escrow-1", weights.lookups)
	}
	for _, escrow := range escrows.byModel[modelA] {
		for _, call := range escrow.Session.(*fakeSession).calls {
			if call != "LatestNonce" && call != "GroupSize" {
				t.Fatalf("escrow %q session saw %q", escrow.ID, call)
			}
		}
	}
}

// Routing declines a nonce-exhausted escrow but cannot replace it, so it must tell the rotation
// lifecycle. Without this the escrow is simply never picked again and drains away silently.
func TestPickEscrowReportsANonceExhaustedCandidate(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "escrow-spent", weight: 100, latestNonce: fallbackNonceCeiling},
		candidate{id: "escrow-fresh", weight: 100, latestNonce: 1},
	)
	var reported []string
	scheduler.onEscrowExhausted = func(escrowID, reason string) { reported = append(reported, escrowID) }

	picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})
	if err != nil {
		t.Fatalf("pickEscrow() = %v, want the fresh escrow", err)
	}
	if picked.ID != "escrow-fresh" {
		t.Fatalf("picked %q, want escrow-fresh", picked.ID)
	}
	if len(reported) != 1 || reported[0] != "escrow-spent" {
		t.Fatalf("reported = %v, want exactly [escrow-spent]", reported)
	}
}

func TestPickEscrowWithoutANonceExhaustedReporterStillPicks(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(
		candidate{id: "escrow-spent", weight: 100, latestNonce: fallbackNonceCeiling},
		candidate{id: "escrow-fresh", weight: 100, latestNonce: 1},
	)

	picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})
	if err != nil || picked.ID != "escrow-fresh" {
		t.Fatalf("pickEscrow() = %q, %v; want escrow-fresh with a nil reporter", picked.ID, err)
	}
}

// A client names the escrow on the per-escrow route, so the pinned path takes untrusted input. The
// nonce ceiling reserves room for the finalize and settlement that follow; past it the chain rejects
// the settlement and the escrow's funds can never be reclaimed.
func TestAPinnedEscrowStillHonoursTheNonceCeiling(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(candidate{id: "escrow-1", weight: 1, latestNonce: 19_900})

	_, err := scheduler.pickEscrow(RequestProfile{Model: modelA, Escrow: "escrow-1"}, chain.PhaseSnapshot{})

	if !errors.Is(err, ErrNoEscrowCapacity) {
		t.Fatalf("pickEscrow() = %v, want the exhausted escrow refused", err)
	}
}

func TestAPinnedEscrowUnderTheCeilingIsServed(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(candidate{id: "escrow-1", weight: 1, latestNonce: 10})

	picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA, Escrow: "escrow-1"}, chain.PhaseSnapshot{})

	if err != nil || picked.ID != "escrow-1" {
		t.Fatalf("pickEscrow() = %v, %v, want the pinned escrow served", picked.ID, err)
	}
}

func (f *fakeSession) Balance() uint64    { return f.balance }
func (f *fakeSession) TokenPrice() uint64 { return f.tokenPrice }

// An escrow must leave routing while it can still refuse cleanly, not once it fails requests.
func TestPickEscrowSkipsAnEscrowBelowItsBalanceFloor(t *testing.T) {
	t.Parallel()
	const reserveTokens, price = 20, 10 // one request reserves 200

	poor := Escrow{ID: "poor", Session: &fakeSession{balance: 500, tokenPrice: price}, ActiveUsers: 4}
	rich := Escrow{ID: "rich", Session: &fakeSession{balance: 1 << 30, tokenPrice: price}, ActiveUsers: 4}
	single := Escrow{ID: "empty", Session: &fakeSession{balance: 100, tokenPrice: price}, ActiveUsers: 0}

	if !belowBalanceFloor(poor, reserveTokens) {
		t.Fatal("an escrow holding 500 with four requests in flight and 200 apiece was kept in routing")
	}
	if belowBalanceFloor(rich, reserveTokens) {
		t.Fatal("a funded escrow was taken out of routing")
	}
	if !belowBalanceFloor(single, reserveTokens) {
		t.Fatal("an escrow that cannot afford one request was kept in routing")
	}
}

// The reserve is priced by the escrow itself rather than by a number an operator had to guess, so a dearer
// escrow leaves routing on a balance a cheaper one still serves from.
func TestTheBalanceFloorIsPricedByTheEscrowsOwnTokenPrice(t *testing.T) {
	t.Parallel()
	const reserveTokens = 100

	cheap := Escrow{ID: "cheap", Session: &fakeSession{balance: 5_000, tokenPrice: 10}, ActiveUsers: 0}
	dear := Escrow{ID: "dear", Session: &fakeSession{balance: 5_000, tokenPrice: 100}, ActiveUsers: 0}

	if belowBalanceFloor(cheap, reserveTokens) {
		t.Fatal("an escrow that affords ten more requests at its own price was taken out of routing")
	}
	if !belowBalanceFloor(dear, reserveTokens) {
		t.Fatal("an escrow that cannot afford one request at its own price was kept in routing")
	}
}

// A price that overflows the reserve is one no balance could ever cover, and it must not wrap into a small
// affordable number.
func TestABalanceFloorThatOverflowsTakesTheEscrowOutOfRouting(t *testing.T) {
	t.Parallel()
	ruinous := Escrow{ID: "ruinous", Session: &fakeSession{balance: math.MaxUint64, tokenPrice: math.MaxUint64}, ActiveUsers: 0}

	if !belowBalanceFloor(ruinous, 1<<62) {
		t.Fatal("a reserve too large to represent was read as affordable")
	}
}

// Without a token cap loaded the gateway cannot price a request at all, and a floor it cannot price must
// not eject anyone.
func TestBalanceFloorIsInertWithoutAReserve(t *testing.T) {
	t.Parallel()
	if belowBalanceFloor(Escrow{ID: "any", Session: &fakeSession{balance: 0, tokenPrice: 10}, ActiveUsers: 99}, 0) {
		t.Fatal("an unpriced floor took an escrow out of routing")
	}
}

// One request's reserve is what it already sent plus the most this gateway will let a host answer with:
// pricing the answer alone would admit a prompt the escrow cannot pay for.
func TestReserveTokensCountsThePromptAndTheAnswerCap(t *testing.T) {
	t.Parallel()
	settings := config.Defaults()
	settings.Limits.MaxTokensCap = 4_096
	priced := &Scheduler{settings: config.NewHolder(&settings)}

	if got := priced.reserveTokens(RequestProfile{InputTokens: 1_000}); got != 5_096 {
		t.Fatalf("reserveTokens = %d, want the prompt and the answer cap together", got)
	}
	if got := (&Scheduler{}).reserveTokens(RequestProfile{InputTokens: 1_000}); got != 0 {
		t.Fatalf("reserveTokens = %d before any configuration loaded, want an unpriced 0", got)
	}
}

// schedulerWithAllowlist wires the settings the picker reads, which the plain constructor leaves nil.
func schedulerWithAllowlist(t *testing.T, allowlist []string, groups map[string][]string) *Scheduler {
	t.Helper()
	configuration := config.Defaults()
	configuration.Scheduler.ParticipantAllowlist = allowlist
	holder := &config.Holder{}
	holder.Swap(&configuration)

	escrows := &fakeEscrows{byModel: map[string][]Escrow{}}
	weights := &fakeWeights{byEscrow: map[string]float64{}}
	for _, escrowID := range slices.Sorted(maps.Keys(groups)) {
		escrows.byModel[modelA] = append(escrows.byModel[modelA], Escrow{
			ID:      escrowID,
			Model:   modelA,
			Session: &fakeSession{groupSize: 4, participants: groups[escrowID]},
		})
		weights.byEscrow[escrowID] = 1
	}
	return &Scheduler{escrows: escrows, capacity: weights, settings: holder}
}

// The escrow is chosen before its participants are consulted, so an escrow whose whole group the
// allowlist refuses must not be a candidate at all: picking it by load spends the request on a group
// holding nobody, and the caller sees "no available host" for a routing decision it cannot influence.
func TestPickEscrowSkipsAnEscrowHoldingNoAllowedParticipant(t *testing.T) {
	scheduler := schedulerWithAllowlist(t, []string{"allowed"}, map[string][]string{
		"crowded": {"someone-else", "another"},
		"lonely":  {"allowed"},
	})

	picked, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

	if err != nil {
		t.Fatalf("pickEscrow(): %v", err)
	}
	if picked.ID != "lonely" {
		t.Fatalf("picked %q, want the only escrow whose group holds an allowed participant", picked.ID)
	}
}

func TestPickEscrowReportsAnAllowlistNoEscrowCanReach(t *testing.T) {
	scheduler := schedulerWithAllowlist(t, []string{"nobody-holds-this"}, map[string][]string{
		"one": {"someone-else"},
		"two": {"another"},
	})

	_, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{})

	if !errors.Is(err, ErrAllowlistUnreachable) {
		t.Fatalf("pickEscrow() = %v, want ErrAllowlistUnreachable", err)
	}
}

func TestPickEscrowIgnoresTheAllowlistWhenItIsEmpty(t *testing.T) {
	scheduler := schedulerWithAllowlist(t, nil, map[string][]string{"one": {"anybody"}})

	if _, err := scheduler.pickEscrow(RequestProfile{Model: modelA}, chain.PhaseSnapshot{}); err != nil {
		t.Fatalf("pickEscrow() with no allowlist: %v", err)
	}
}
