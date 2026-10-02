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

const fullContextTokens = 180_000

// pinContextLength makes the operator pin modelA's context length, the way model_limits does.
func pinContextLength(scheduler *Scheduler) {
	settings := *scheduler.settings.Load()
	contextLength := int64(fullContextTokens)
	settings.Limits.ModelLimits = map[string]config.ModelLimits{modelA: {MaxModelLen: &contextLength}}
	scheduler.settings.Swap(&settings)
}

// Test flow:
//  1. For each table case, build a `retirementHarness` funded for a capped answer but not for one full-context request, naming the context length by an operator pin or by the chain's --max-model-len.
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
			scheduler, reported := retirementHarness(t, 100_000)
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
//  3. Assert it is the model's floor plus 31 capped answers: the same headroom over the floor an unpinned model gets.
func TestTheResumeFloorClearsTheModelsFloorByTheSameHeadroom(t *testing.T) {
	t.Parallel()
	scheduler, _ := retirementHarness(t, 100_000)
	pinContextLength(scheduler)
	scheduler.snapshots = &fakeSnapshots{}

	floor, priced := scheduler.ResumeFloor(scheduler.escrows.Candidates(modelA)[0], 32)

	if want := uint64(fullContextTokens + 31*4_096); !priced || floor != want {
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
		{name: "above_the_floor_short_of_the_headroom", balance: 200_000},
		{name: "past_the_floor_and_the_headroom", balance: 400_000, ready: true},
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
