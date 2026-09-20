package engine

import (
	"context"
	"errors"
	"time"

	"devshard/logging"
	"devshard/user"
)

// TimeoutPoster posts one nonce's vote and says when the protocol will accept it, so the queue can hold
// the vote without a goroutine sleeping for it. See README, "Timeout votes".
type TimeoutPoster interface {
	SettleTimeout(ctx context.Context, step TimeoutStep) (TimeoutVote, error)
	VoteDeadline(nonce uint64, startedAt time.Time) time.Time
}

type TimeoutVote struct {
	Kind          string
	Detail        string
	VerifyRejects []string
	Completeness  string
}

type TimeoutEvent struct {
	RequestID     string
	EscrowID      string
	Participant   string
	Model         string
	Nonce         uint64
	Kind          string
	Action        string
	Reason        string
	VerifyRejects []string
	Completeness  string
}

// SettleKind is how a nonce is settled. See ../docs/race.md, "Error misses".
type SettleKind int

const (
	SettleTimeout SettleKind = iota
	SettleErrorMiss
)

// TimeoutStep is one nonce's vote; StartedAt is the record's, not the attempt's dispatch. See README, "Timeout votes".
type TimeoutStep struct {
	Nonce     uint64
	StartedAt time.Time
	Post      bool
	Kind      SettleKind
	Proof     *MissProof
	Event     TimeoutEvent
}

func timeoutKind(a AttemptOutcome) string {
	if a.claimsMiss() {
		return TimeoutKindErrorMiss
	}
	if a.ReceiptTime.IsZero() {
		return TimeoutKindRefused
	}
	return TimeoutKindExecution
}

func timeoutVoteKind(vote, fallback string) string {
	switch vote {
	case TimeoutKindRefused, TimeoutKindExecution:
		return vote
	}
	return fallback
}

func (o RaceOutcome) nonceSettled(a AttemptOutcome) bool {
	return a.NonceFinished && !a.emptyStream() && !a.errorStream()
}

// timeoutSkipReason names every skip; a diverged escrow state is deliberately not one. See race.md, "Timeout votes".
func (o RaceOutcome) timeoutSkipReason(a AttemptOutcome) (string, bool) {
	if a.claimsMiss() {
		return "", false
	}
	switch {
	case a.PhaseTransitionAborted:
		return TimeoutReasonPhaseAborted, true
	case a.emptyStream() && a.NonceFinished:
		return TimeoutReasonEmptyStream, true
	case a.NonceFinished:
		return TimeoutReasonNonceFinished, true
	case o.longResponseExempt(a):
		return TimeoutReasonLongResponse, true
	}
	return "", false
}

func (o RaceOutcome) TimeoutPlan() []TimeoutStep {
	steps := make([]TimeoutStep, 0, len(o.Attempts))
	for _, attempt := range o.Attempts {
		if o.nonceSettled(attempt) {
			continue
		}
		step := TimeoutStep{
			Nonce:     attempt.Nonce,
			StartedAt: attempt.StartedAt,
			Event: TimeoutEvent{
				RequestID:   o.RequestID,
				EscrowID:    o.EscrowID,
				Participant: attempt.Participant,
				Model:       o.Model,
				Nonce:       attempt.Nonce,
				Kind:        timeoutKind(attempt),
				Action:      TimeoutActionSkipped,
			},
		}
		if reason, skip := o.timeoutSkipReason(attempt); skip {
			step.Event.Reason = reason
		} else {
			step.Post = true
			if attempt.claimsMiss() {
				step.Kind, step.Proof = SettleErrorMiss, attempt.MissProof
			}
			step.Event.Action = TimeoutActionStarted
			step.Event.Reason = TimeoutReasonNone
		}
		steps = append(steps, step)
	}
	return steps
}

// SettleTimeouts reports a posted vote's started event before the post and its result after. See README, "Timeout votes".
func SettleTimeouts(ctx context.Context, poster TimeoutPoster, outcome RaceOutcome, report func(TimeoutEvent)) {
	for _, step := range outcome.TimeoutPlan() {
		// A started event for a vote nobody attempts reads as a hung settle when no completion follows.
		if !step.Post || poster == nil {
			skipped := step.Event
			skipped.Action = TimeoutActionSkipped
			if skipped.Reason == TimeoutReasonNone {
				skipped.Reason = TimeoutReasonNoPoster
			}
			report(skipped)
			continue
		}
		report(step.Event)
		vote, err := poster.SettleTimeout(ctx, step)
		posted := step.Event
		posted.Kind = timeoutVoteKind(vote.Kind, posted.Kind)
		posted.Action, posted.Reason = TimeoutOutcome(vote, err, outcome.Lifecycle.EscrowMissing)
		posted.VerifyRejects, posted.Completeness = vote.VerifyRejects, vote.Completeness
		report(posted)
	}
}

// TimeoutOutcome classifies what a posted vote came back as, preferring the handler's own detail. See README, "Timeout votes".
func TimeoutOutcome(vote TimeoutVote, err error, escrowMissing bool) (action, reason string) {
	switch {
	case errors.Is(err, user.ErrInferenceMissed):
		return TimeoutActionCompleted, TimeoutReasonNone
	case errors.Is(err, user.ErrNonceFinishedWhileWaiting):
		return TimeoutActionSkipped, TimeoutReasonNonceFinished
	case errors.Is(err, user.ErrTimeoutNotApplied):
		return TimeoutActionFailed, firstNamed(vote.Detail, TimeoutReasonNotApplied)
	case err != nil && escrowMissing:
		return TimeoutActionFailed, TimeoutReasonEscrowGone
	case err != nil:
		return TimeoutActionFailed, firstNamed(vote.Detail, TimeoutReasonCollectionError)
	}
	return TimeoutActionCompleted, TimeoutReasonNone
}

func firstNamed(detail, fallback string) string {
	if detail != "" {
		return detail
	}
	return fallback
}

// settleContext carries the race's request id into the shared session's timeout stages; an empty id adds none. See README, "Timeout votes".
func settleContext(requestID string) context.Context {
	if requestID == "" {
		return context.Background()
	}
	withRequest, _ := logging.WithRequestID(context.Background(), requestID)
	return withRequest
}
