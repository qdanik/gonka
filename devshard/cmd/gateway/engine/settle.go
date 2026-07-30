package engine

import (
	"context"
	"time"
)

const (
	timeoutKindRefused   = "refused"
	timeoutKindExecution = "execution"

	timeoutActionSkipped   = "skipped"
	timeoutActionStarted   = "started"
	timeoutActionCompleted = "completed"
	timeoutActionFailed    = "failed"

	timeoutReasonNone            = "none"
	timeoutReasonPhaseAborted    = "phase_transition_aborted"
	timeoutReasonEmptyStream     = "empty_stream_without_non_empty_winner"
	timeoutReasonNonceFinished   = "nonce_already_finished"
	timeoutReasonLongResponse    = "long_response_after_content"
	timeoutReasonCollectionError = "timeout_collection_error"
)

type TimeoutPoster interface {
	SettleTimeout(ctx context.Context, nonce uint64, sentAt time.Time) (vote string, err error)
}

type TimeoutEvent struct {
	Participant string
	Model       string
	Nonce       uint64
	Kind        string
	Action      string
	Reason      string
	Vote        string
}

type TimeoutStep struct {
	Nonce    uint64
	SendTime time.Time
	Post     bool
	Event    TimeoutEvent
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

// A host whose escrow state diverged still gets its timeout posted: divergence is a routing fact,
// while an unposted vote leaves an orphaned MsgStart that settlement can never resolve.
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
			Nonce:    attempt.Nonce,
			SendTime: attempt.SendTime,
			Event: TimeoutEvent{
				Participant: attempt.Participant,
				Model:       o.Model,
				Nonce:       attempt.Nonce,
				Kind:        timeoutKind(attempt),
				Action:      timeoutActionSkipped,
			},
		}
		if reason, skip := o.timeoutSkipReason(attempt); skip {
			step.Event.Reason = reason
		} else {
			step.Post = true
			step.Event.Action = timeoutActionStarted
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
		events = append(events, step.Event)
		if !step.Post || poster == nil {
			continue
		}
		vote, err := poster.SettleTimeout(ctx, step.Nonce, step.SendTime)
		posted := step.Event
		posted.Vote = vote
		posted.Kind = timeoutVoteKind(vote, posted.Kind)
		posted.Action = timeoutActionCompleted
		if err != nil {
			posted.Action = timeoutActionFailed
			posted.Reason = timeoutReasonCollectionError
		}
		events = append(events, posted)
	}
	return events
}
