package engine

import (
	"context"
	"errors"
	"time"

	"devshard/user"
)

const (
	timeoutKindRefused   = "refused"
	timeoutKindExecution = "execution"

	TimeoutActionSkipped   = "skipped"
	TimeoutActionStarted   = "started"
	TimeoutActionCompleted = "completed"
	TimeoutActionFailed    = "failed"

	// timeoutReasonNoPoster names a step whose escrow no longer has a session to vote through.
	timeoutReasonNoPoster = "no_poster"

	timeoutReasonNone            = "none"
	timeoutReasonPhaseAborted    = "phase_transition_aborted"
	timeoutReasonEmptyStream     = "empty_stream_without_non_empty_winner"
	timeoutReasonNonceFinished   = "nonce_already_finished"
	timeoutReasonLongResponse    = "long_response_after_content"
	timeoutReasonCollectionError = "timeout_collection_error"
	timeoutReasonNotApplied      = "timeout_not_applied"
)

type TimeoutPoster interface {
	SettleTimeout(ctx context.Context, nonce uint64, startedAt time.Time) (vote string, err error)
}

type TimeoutEvent struct {
	Participant string
	Model       string
	Nonce       uint64
	Kind        string
	Action      string
	Reason      string
}

// TimeoutStep is one nonce's vote. StartedAt is the record's, not the attempt's dispatch, so a nonce
// no attempt ever spent still waits out the deadline a verifier recomputes from that record.
type TimeoutStep struct {
	Nonce     uint64
	StartedAt time.Time
	Post      bool
	Event     TimeoutEvent
}

func timeoutKind(a AttemptOutcome) string {
	if a.ReceiptTime.IsZero() {
		return timeoutKindRefused
	}
	return timeoutKindExecution
}

func timeoutVoteKind(vote, fallback string) string {
	switch vote {
	case timeoutKindRefused, timeoutKindExecution:
		return vote
	}
	return fallback
}

func (o RaceOutcome) nonceSettled(a AttemptOutcome) bool {
	return a.NonceFinished && !a.emptyStream() &&
		a.Terminal != TerminalErrorStream && a.Terminal != TerminalCapabilityRefused
}

// timeoutSkipReason names every skip; a host whose escrow state diverged is deliberately not one of
// them. See gateway-speculative-race.md, "Timeout votes".
func (o RaceOutcome) timeoutSkipReason(a AttemptOutcome) (string, bool) {
	switch {
	case a.PhaseTransitionAborted:
		return timeoutReasonPhaseAborted, true
	case a.emptyStream() && a.NonceFinished:
		return timeoutReasonEmptyStream, true
	case a.NonceFinished:
		return timeoutReasonNonceFinished, true
	case o.longResponseExempt(a):
		return timeoutReasonLongResponse, true
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
			step.Event.Reason = timeoutReasonNone
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
			if skipped.Reason == timeoutReasonNone {
				skipped.Reason = timeoutReasonNoPoster
			}
			events = append(events, skipped)
			continue
		}
		events = append(events, step.Event)
		vote, err := poster.SettleTimeout(ctx, step.Nonce, step.StartedAt)
		posted := step.Event
		posted.Kind = timeoutVoteKind(vote, posted.Kind)
		posted.Action = TimeoutActionCompleted
		switch {
		case errors.Is(err, user.ErrNonceFinishedWhileWaiting):
			posted.Action = TimeoutActionSkipped
			posted.Reason = timeoutReasonNonceFinished
		case errors.Is(err, user.ErrTimeoutNotApplied):
			posted.Action = TimeoutActionFailed
			posted.Reason = timeoutReasonNotApplied
		case err != nil:
			posted.Action = TimeoutActionFailed
			posted.Reason = timeoutReasonCollectionError
		}
		events = append(events, posted)
	}
	return events
}
