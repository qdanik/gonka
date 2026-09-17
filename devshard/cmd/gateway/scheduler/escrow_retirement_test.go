package scheduler

import (
	"errors"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/types"
)

// retirementHarness funds an escrow well enough to answer a capped request and nothing like well enough to
// answer a huge one, which is the gap the two prices have to tell apart.
func retirementHarness(t *testing.T, balance uint64) (*Scheduler, *[]exhaustionReport) {
	t.Helper()
	settings := config.Defaults()
	settings.Limits.MaxTokensCap = 4_096
	scheduler, _, _ := newScheduler(
		candidate{id: "only", weight: 100, latestNonce: 1, balance: balance, tokenPrice: 1},
	)
	scheduler.settings = config.NewHolder(&settings)
	var reported []exhaustionReport
	scheduler.onEscrowExhausted = func(escrowID, reason string) {
		reported = append(reported, exhaustionReport{escrowID: escrowID, reason: reason})
	}
	return scheduler, &reported
}

// A request the escrow cannot pay for is a fact about the request. Retiring the escrow over it mints a
// replacement and deactivates a perfectly solvent escrow, and one oversized arrival does it to every
// candidate of the model at once, because the pick prices them all against the same request.
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

// The escrow is genuinely below one capped answer, which is what being finished means.
func TestAnEscrowThatCannotAffordACappedAnswerIsRetired(t *testing.T) {
	t.Parallel()
	scheduler, reported := retirementHarness(t, 100)

	_, err := pickWith(scheduler, RequestProfile{Model: modelA, InputBytes: 10, OutputTokens: 16}, chain.PhaseSnapshot{})

	if !errors.Is(err, types.ErrInsufficientBalance) {
		t.Fatalf("pickEscrow = %v, want the dry escrow refused", err)
	}
	want := []exhaustionReport{{escrowID: "only", reason: exhaustionBalanceFloor}}
	if len(*reported) != 1 || (*reported)[0] != want[0] {
		t.Fatalf("reported = %v, want %v", *reported, want)
	}
}

// A pinned escrow takes the same two prices: an escalation naming an escrow it cannot pay for is refused
// without the escrow being marked, or one oversized escalation retires the escrow its own race is pinned to.
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
