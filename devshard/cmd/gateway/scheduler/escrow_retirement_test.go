package scheduler

import (
	"errors"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/types"
)

// retirementHarness builds a scheduler with one candidate escrow funded to the given balance, with a max-tokens cap of 4096.
func retirementHarness(t *testing.T, balance uint64) (*Scheduler, *[]exhaustionReport) {
	t.Helper()
	settings := config.Defaults()
	settings.Limits.MaxTokensCap = 4_096
	scheduler, _, _ := newScheduler(
		candidate{id: "only", weight: 100, latestNonce: 1, balance: balance, tokenPrice: 1},
	)
	scheduler.settings = config.NewHolder(&settings)
	var reported []exhaustionReport
	scheduler.onEscrowExhausted = func(escrowID string, reason ExhaustionReason) {
		reported = append(reported, exhaustionReport{escrowID: escrowID, reason: reason})
	}
	return scheduler, &reported
}

// Test flow:
//  1. Build a `retirementHarness` funded well enough for a capped request.
//  2. Pick an escrow for a request whose output tokens are far larger than the cap allows.
//  3. Assert the pick fails with `types.ErrInsufficientBalance`.
//  4. Assert no exhaustion was reported, since a solvent escrow is not retired over one oversized request.
func TestARequestTooDearForAnEscrowDoesNotRetireIt(t *testing.T) {
	t.Parallel()
	scheduler, reported := retirementHarness(t, 1<<20)

	_, err := pickWith(scheduler, RequestProfile{Model: modelA, InputBytes: 8_192, OutputTokens: 10_000_000}, chain.PhaseSnapshot{})

	if !errors.Is(err, types.ErrInsufficientBalance) {
		t.Fatalf("pickEscrow = %v, want the request refused as unaffordable", err)
	}
	if len(*reported) != 0 {
		t.Fatalf("one request retired %v; an escrow that still affords the retirement floor is not finished", *reported)
	}
}

// Test flow:
//  1. Build a `retirementHarness` funded below what even a capped request costs.
//  2. Pick an escrow for a small request.
//  3. Assert the pick fails with `types.ErrInsufficientBalance`.
//  4. Assert the escrow was reported exhausted with `ExhaustionBalanceFloor`.
func TestAnEscrowThatCannotAffordACappedAnswerIsRetired(t *testing.T) {
	t.Parallel()
	scheduler, reported := retirementHarness(t, 100)

	_, err := pickWith(scheduler, RequestProfile{Model: modelA, InputBytes: 10, OutputTokens: 16}, chain.PhaseSnapshot{})

	if !errors.Is(err, types.ErrInsufficientBalance) {
		t.Fatalf("pickEscrow = %v, want the dry escrow refused", err)
	}
	want := []exhaustionReport{{escrowID: "only", reason: ExhaustionBalanceFloor}}
	if len(*reported) != 1 || (*reported)[0] != want[0] {
		t.Fatalf("reported = %v, want %v", *reported, want)
	}
}

// Test flow:
//  1. Build a `retirementHarness` funded well enough for a capped request.
//  2. Pick the escrow by name for a request whose output tokens are far larger than the cap allows.
//  3. Assert the pick fails with `types.ErrInsufficientBalance`.
//  4. Assert no exhaustion was reported for the pinned escrow.
func TestAPinnedEscrowIsRefusedWithoutBeingRetired(t *testing.T) {
	t.Parallel()
	scheduler, reported := retirementHarness(t, 1<<20)

	_, err := pickWith(scheduler, RequestProfile{Model: modelA, Escrow: "only", InputBytes: 8_192, OutputTokens: 10_000_000}, chain.PhaseSnapshot{})

	if !errors.Is(err, types.ErrInsufficientBalance) {
		t.Fatalf("pickEscrow = %v, want the pinned request refused as unaffordable", err)
	}
	if len(*reported) != 0 {
		t.Fatalf("a pinned request retired %v", *reported)
	}
}

const (
	fullContextTokens  = 180_000
	fullContextReserve = 4 * fullContextTokens
)

// pinContextLength makes the operator pin modelA's context length, the way model_limits does.
func pinContextLength(scheduler *Scheduler) {
	settings := *scheduler.settings.Load()
	contextLength := int64(fullContextTokens)
	settings.Limits.ModelLimits = map[string]config.ModelLimits{modelA: {MaxModelLen: &contextLength}}
	scheduler.settings.Swap(&settings)
}

// Test flow:
//  1. For each table case, build a `retirementHarness` funded for the model's context length in tokens but not in prompt bytes, naming the context length by an operator pin or by the chain's --max-model-len.
//  2. Pick an escrow for a small request.
//  3. Assert the pick fails with `types.ErrInsufficientBalance` and the escrow is reported exhausted with `ExhaustionBalanceFloor`.
func TestAnEscrowThatCannotAffordItsModelsFloorIsRetired(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		pinned   bool
		snapshot chain.PhaseSnapshot
	}{
		{name: "operator_pin", pinned: true},
		{name: "chain_length", snapshot: chain.PhaseSnapshot{Models: map[string]chain.ModelParams{modelA: {MaxModelLen: fullContextTokens}}}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, reported := retirementHarness(t, 200_000)
			if testCase.pinned {
				pinContextLength(scheduler)
			}

			_, err := pickWith(scheduler, RequestProfile{Model: modelA, InputBytes: 10, OutputTokens: 16}, testCase.snapshot)

			if !errors.Is(err, types.ErrInsufficientBalance) {
				t.Fatalf("pickEscrow = %v, want the escrow refused as finished", err)
			}
			want := exhaustionReport{escrowID: "only", reason: ExhaustionBalanceFloor}
			if len(*reported) != 1 || (*reported)[0] != want {
				t.Fatalf("reported = %v, want [%v]", *reported, want)
			}
		})
	}
}

// Test flow:
//  1. Build a scheduler whose only escrow holds a million with five requests on it, its model pinned at 180 000 tokens.
//  2. Pick an escrow for a small request.
//  3. Assert the pick succeeds and nothing is reported: the model's floor is counted once, and each request on the escrow adds one capped answer.
func TestABusyEscrowThatCoversItsModelsFloorOnceStaysInService(t *testing.T) {
	t.Parallel()
	scheduler, _, _ := newScheduler(candidate{id: "busy", weight: 100, latestNonce: 1, balance: 1_000_000, tokenPrice: 1, activeUsers: 5})
	settings := config.Defaults()
	settings.Limits.MaxTokensCap = 4_096
	scheduler.settings = config.NewHolder(&settings)
	pinContextLength(scheduler)
	var reported []exhaustionReport
	scheduler.onEscrowExhausted = func(escrowID string, reason ExhaustionReason) {
		reported = append(reported, exhaustionReport{escrowID: escrowID, reason: reason})
	}

	picked, err := pickWith(scheduler, RequestProfile{Model: modelA, InputBytes: 10, OutputTokens: 16}, chain.PhaseSnapshot{})

	if err != nil || picked.ID != "busy" {
		t.Fatalf("pickEscrow = %+v, %v; want the busy escrow", picked, err)
	}
	if len(reported) != 0 {
		t.Fatalf("reported = %v, want nothing: five requests do not cost five full-context floors", reported)
	}
}

// Test flow:
//  1. Build a `retirementHarness` whose model the operator pins at 180 000 tokens.
//  2. Ask for the resume floor of 32 answers.
//  3. Assert it is the model's floor in prompt bytes plus 31 capped answers: the same headroom over the floor an unpinned model gets.
func TestTheResumeFloorClearsTheModelsFloorByTheSameHeadroom(t *testing.T) {
	t.Parallel()
	scheduler, _ := retirementHarness(t, 100_000)
	pinContextLength(scheduler)
	scheduler.snapshots = &fakeSnapshots{}

	floor, priced := scheduler.ResumeFloor(scheduler.escrows.Candidates(modelA)[0], 32)

	if want := uint64(fullContextReserve + 31*4_096); !priced || floor != want {
		t.Fatalf("ResumeFloor(32) = %d, %v, want %d priced", floor, priced, want)
	}
}

// Test flow:
//  1. For each table case, build a `retirementHarness` with the given balance, its model pinned at 180 000 tokens.
//  2. Ask `ResumeReadiness` for 32 answers.
//  3. Assert an escrow above the model's floor but short of its headroom is not ready, and one past both is.
func TestAHeldEscrowResumesOnlyPastItsModelsFloorAndHeadroom(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		balance uint64
		ready   bool
	}{
		{name: "above_the_floor_short_of_the_headroom", balance: 800_000},
		{name: "past_the_floor_and_the_headroom", balance: 900_000, ready: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, _ := retirementHarness(t, testCase.balance)
			pinContextLength(scheduler)
			scheduler.snapshots = &fakeSnapshots{}

			ready, nonceSpent := scheduler.ResumeReadiness(scheduler.escrows.Candidates(modelA)[0], 32)

			if ready != testCase.ready || nonceSpent {
				t.Fatalf("ResumeReadiness(32) = %v, %v; want %v, false", ready, nonceSpent, testCase.ready)
			}
		})
	}
}

// Test flow:
//  1. For each table case, build a `retirementHarness` with the given balance, its model pinned at 180 000 tokens.
//  2. Ask `Exhaustion` for the escrow without any request arriving.
//  3. Assert an escrow short of one full-context request reads `ExhaustionBalanceFloor`, one that covers it reads nothing, and nothing is reported.
func TestExhaustionPricesAnEscrowByItsModelsFloorWithoutARequest(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		balance uint64
		want    ExhaustionReason
	}{
		{name: "short_of_one_full_context_request", balance: 200_000, want: ExhaustionBalanceFloor},
		{name: "covers_one_full_context_request", balance: 800_000},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, reported := retirementHarness(t, testCase.balance)
			pinContextLength(scheduler)
			scheduler.snapshots = &fakeSnapshots{}

			if reason := scheduler.Exhaustion(scheduler.escrows.Candidates(modelA)[0]); reason != testCase.want {
				t.Fatalf("Exhaustion = %q, want %q", reason, testCase.want)
			}
			if len(*reported) != 0 {
				t.Fatalf("reported = %v, want nothing: Exhaustion only reads, the caller decides", *reported)
			}
		})
	}
}

// Test flow:
//  1. Build a funded escrow whose cursor sits at the fallback nonce ceiling.
//  2. Ask `Exhaustion` before governance `max_nonce` is known, then with a known cap below the cursor.
//  3. Assert the fallback ceiling reads nothing, since it is not the hosts' cap, and the known cap reads `ExhaustionNonceCap`.
func TestExhaustionReportsTheHostsNonceCapButNotTheFallbackCeiling(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		maxNonce uint64
		want     ExhaustionReason
	}{
		{name: "max_nonce_unknown", want: ""},
		{name: "max_nonce_known", maxNonce: 1_000, want: ExhaustionNonceCap},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			scheduler, _, _ := newScheduler(candidate{id: "worn", weight: 100, latestNonce: fallbackNonceCeiling, balance: 1 << 30, tokenPrice: 1})
			settings := config.Defaults()
			settings.Limits.MaxTokensCap = 16
			scheduler.settings = config.NewHolder(&settings)
			scheduler.snapshots = &fakeSnapshots{snapshot: chain.PhaseSnapshot{MaxNonce: testCase.maxNonce}}

			if reason := scheduler.Exhaustion(scheduler.escrows.Candidates(modelA)[0]); reason != testCase.want {
				t.Fatalf("Exhaustion = %q, want %q", reason, testCase.want)
			}
		})
	}
}
