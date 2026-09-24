package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"devshard/user"
)

func attemptEndingInError(proof *MissProof) AttemptOutcome {
	return AttemptOutcome{
		Nonce:       7,
		Participant: "host-a",
		StartedAt:   time.Unix(1000, 0),
		ReceiptTime: time.Unix(1001, 0),
		Terminal:    TerminalErrorStream,
		MissProof:   proof,
	}
}

func raceEndingIn(attempt AttemptOutcome) RaceOutcome {
	return RaceOutcome{RequestID: "req-1", EscrowID: "escrow-1", Model: "qwen", Attempts: []AttemptOutcome{attempt}}
}

// A host that answered with its own signed error is not a host that went silent: claiming the miss
// settles the nonce on the evidence, where a timeout vote would ask the group to guess.
func TestAnErrorTheHostSignedIsSettledAsAMiss(t *testing.T) {
	proof := &MissProof{ResponsePayload: []byte(`{"events":["data: {\"error\":{}}"]}`), Complete: true}

	steps := raceEndingIn(attemptEndingInError(proof)).TimeoutPlan()

	if len(steps) != 1 {
		t.Fatalf("plan = %d steps, want 1", len(steps))
	}
	if !steps[0].Post {
		t.Fatal("the step must be posted")
	}
	if steps[0].Kind != SettleByMissClaim {
		t.Fatalf("kind = %v, want the miss", steps[0].Kind)
	}
	if steps[0].Proof != proof {
		t.Fatal("the step must carry the proof the attempt retained")
	}
	if steps[0].Event.Kind != TimeoutKindErrorMiss {
		t.Fatalf("event kind = %q, want %q", steps[0].Event.Kind, TimeoutKindErrorMiss)
	}
}

// Without the body a verifier recomputes, there is nothing to claim: the nonce goes back to the
// ordinary vote rather than being posted as a miss no one can check.
func TestAnErrorWithoutProofFallsBackToTheVote(t *testing.T) {
	steps := raceEndingIn(attemptEndingInError(nil)).TimeoutPlan()

	if len(steps) != 1 || !steps[0].Post {
		t.Fatalf("plan = %+v, want one posted step", steps)
	}
	if steps[0].Kind != SettleByVote {
		t.Fatalf("kind = %v, want the ordinary vote", steps[0].Kind)
	}
	if steps[0].Event.Kind != TimeoutKindExecution {
		t.Fatalf("event kind = %q, want %q", steps[0].Event.Kind, TimeoutKindExecution)
	}
}

// The miss exists precisely because the host did finish -- with its own error. Skipping the nonce for
// being finished is what would pay a host for an error, so proof overrides that skip.
func TestAMissIsClaimedEvenThoughTheHostFinished(t *testing.T) {
	attempt := attemptEndingInError(&MissProof{ResponsePayload: []byte(`{"events":["data: {\"error\":{}}"]}`)})
	attempt.NonceFinished = true

	steps := raceEndingIn(attempt).TimeoutPlan()

	if len(steps) != 1 {
		t.Fatalf("plan = %d steps, want 1", len(steps))
	}
	if !steps[0].Post || steps[0].Kind != SettleByMissClaim {
		t.Fatalf("a finished nonce with proof is still claimed as a miss: %+v", steps[0])
	}
}

// Without proof the ordinary skip stands: a finished nonce is settled and asks the group for nothing.
func TestAFinishedNonceWithoutProofIsStillSkipped(t *testing.T) {
	attempt := attemptEndingInError(nil)
	attempt.NonceFinished = true

	steps := raceEndingIn(attempt).TimeoutPlan()

	if len(steps) != 1 {
		t.Fatalf("plan = %d steps, want 1", len(steps))
	}
	if steps[0].Post {
		t.Fatalf("a finished nonce without proof must not be posted: %+v", steps[0])
	}
}

// The claim is only worth making with the host's signature behind it: the session holds the Finish,
// the plan holds the body, and the verifier checks one against the other.
func TestAClaimedMissGoesToTheMissHandler(t *testing.T) {
	handler := &scriptedTimeoutHandler{finishTx: []byte("signed-finish")}
	poster := &SessionTimeouts{handler: handler}
	step := TimeoutStep{
		Nonce: 7,
		Kind:  SettleByMissClaim,
		Proof: &MissProof{ResponsePayload: []byte(`{"events":[]}`)},
	}

	if _, err := poster.SettleTimeout(context.Background(), step); err != nil {
		t.Fatalf("SettleTimeout() = %v", err)
	}

	if handler.missCalls != 1 {
		t.Errorf("the miss handler ran %d times, want 1", handler.missCalls)
	}
	if handler.calls != 0 {
		t.Errorf("the vote ran %d times, want the miss instead", handler.calls)
	}
}

// A Finish the session does not hold means every verifier would reject the claim for no_finish_tx,
// so the nonce is better served by the ordinary vote than by a claim that cannot land.
func TestAMissWithNoSignedFinishFallsBackToTheVote(t *testing.T) {
	handler := &scriptedTimeoutHandler{}
	poster := &SessionTimeouts{handler: handler}
	step := TimeoutStep{
		Nonce: 7,
		Kind:  SettleByMissClaim,
		Proof: &MissProof{ResponsePayload: []byte(`{"events":[]}`)},
	}

	if _, err := poster.SettleTimeout(context.Background(), step); err != nil {
		t.Fatalf("SettleTimeout() = %v", err)
	}

	if handler.missCalls != 0 {
		t.Errorf("the miss handler ran %d times, want none without a Finish", handler.missCalls)
	}
	if handler.calls != 1 {
		t.Errorf("the vote ran %d times, want 1", handler.calls)
	}
}

// A landed miss comes back as ErrInferenceMissed: the protocol says so and the caller must read it as
// the settlement it is. Reading it as a failure would report every successful miss as a broken vote.
func TestALandedMissIsReadAsSettledNotFailed(t *testing.T) {
	action, reason := TimeoutOutcome(
		TimeoutVote{Kind: TimeoutKindErrorMiss},
		fmt.Errorf("inference 7 timed out: error: %w", user.ErrInferenceMissed),
		false,
	)

	if action != TimeoutActionCompleted {
		t.Fatalf("action = %q, want %q", action, TimeoutActionCompleted)
	}
	if reason != TimeoutReasonNone {
		t.Fatalf("reason = %q, want %q", reason, TimeoutReasonNone)
	}
}

// A refused claim has to say why the group refused it and how whole the proof was, or a
// reconstruction that keeps drifting is indistinguishable from one more failed vote.
func TestARefusedClaimReportsWhatTheVerifiersSaid(t *testing.T) {
	handler := &scriptedTimeoutHandler{
		finishTx: []byte("signed-finish"),
		result:   user.TimeoutResult{Reason: "error", VerifyRejects: []string{"hash_mismatch", "no_payload"}},
	}
	poster := &SessionTimeouts{handler: handler}
	step := TimeoutStep{
		Nonce: 7,
		Kind:  SettleByMissClaim,
		Proof: &MissProof{ResponsePayload: []byte(`{"events":[]}`), Truncated: true},
	}

	vote, _ := poster.SettleTimeout(context.Background(), step)

	if len(vote.VerifyRejects) != 2 {
		t.Fatalf("verify rejects = %q, want both causes", vote.VerifyRejects)
	}
	if vote.Completeness != MissProofTruncated {
		t.Fatalf("completeness = %q, want %q", vote.Completeness, MissProofTruncated)
	}
}

// An ordinary vote carries no proof, so it names no completeness rather than guessing one.
func TestAnOrdinaryVoteNamesNoCompleteness(t *testing.T) {
	poster := &SessionTimeouts{handler: &scriptedTimeoutHandler{}}

	vote, _ := poster.SettleTimeout(context.Background(), TimeoutStep{Nonce: 7})

	if vote.Completeness != "" {
		t.Fatalf("completeness = %q, want it unnamed", vote.Completeness)
	}
}
