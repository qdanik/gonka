package engine

import (
	"context"
	"errors"
	"time"

	"devshard/user"
)

type TimeoutPoster interface {
	SettleTimeout(ctx context.Context, nonce uint64, startedAt time.Time) (TimeoutVote, error)
}

type TimeoutVote struct {
	Kind   string
	Detail string
}

type TimeoutEvent struct {
	EscrowID    string
	Participant string
	Model       string
	Nonce       uint64
	Kind        string
	Action      string
	Reason      string
}

// TimeoutStep is one nonce's vote; StartedAt is the record's, not the attempt's dispatch. See README, "Timeout votes".
type TimeoutStep struct {
	Nonce     uint64
	StartedAt time.Time
	Post      bool
	Event     TimeoutEvent
}

func timeoutKind(a AttemptOutcome) string {
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
	return a.NonceFinished && !a.emptyStream() &&
		a.Terminal != TerminalErrorStream && a.Terminal != TerminalCapabilityRefused
}

// timeoutSkipReason names every skip; a diverged escrow state is deliberately not one. See race.md, "Timeout votes".
func (o RaceOutcome) timeoutSkipReason(a AttemptOutcome) (string, bool) {
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
			step.Event.Action = TimeoutActionStarted
			step.Event.Reason = TimeoutReasonNone
		}
		steps = append(steps, step)
	}
	return steps
}

func SettleTimeouts(ctx context.Context, poster TimeoutPoster, outcome RaceOutcome) []TimeoutEvent {
	steps := outcome.TimeoutPlan()
	events := make([]TimeoutEvent, 0, len(steps))
	for _, step := range steps {
		// A started event for a vote nobody attempts reads as a hung settle when no completion follows.
		if !step.Post || poster == nil {
			skipped := step.Event
			skipped.Action = TimeoutActionSkipped
			if skipped.Reason == TimeoutReasonNone {
				skipped.Reason = TimeoutReasonNoPoster
			}
			events = append(events, skipped)
			continue
		}
		events = append(events, step.Event)
		vote, err := poster.SettleTimeout(ctx, step.Nonce, step.StartedAt)
		posted := step.Event
		posted.Kind = timeoutVoteKind(vote.Kind, posted.Kind)
		posted.Action, posted.Reason = TimeoutOutcome(vote, err, outcome.Lifecycle.EscrowMissing)
		events = append(events, posted)
	}
	return events
}

// TimeoutOutcome classifies what a posted vote came back as, preferring the handler's own detail. See README, "Timeout votes".
func TimeoutOutcome(vote TimeoutVote, err error, escrowMissing bool) (action, reason string) {
	switch {
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
