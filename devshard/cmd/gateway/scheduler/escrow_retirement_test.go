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
		t.Fatalf("one request retired %v; an escrow that still affords a capped answer is not finished", *reported)
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
