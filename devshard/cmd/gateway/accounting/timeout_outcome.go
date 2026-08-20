package accounting

type TimeoutOutcome string

const (
	TimeoutSkipped              TimeoutOutcome = "skipped"
	TimeoutVoteCollectionFailed TimeoutOutcome = "vote_collection_failed"
	TimeoutInsufficientVotes    TimeoutOutcome = "insufficient_votes"
	TimeoutDiffSendFailed       TimeoutOutcome = "diff_send_failed"
	TimeoutApplied              TimeoutOutcome = "applied"
)

const (
	timeoutActionSkipped   = "skipped"
	timeoutActionCompleted = "completed"
	timeoutActionFailed    = "failed"

	timeoutReasonCollectionError = "timeout_collection_error"
	timeoutReasonNotApplied      = "timeout_not_applied"
	timeoutReasonEscrowGone      = "escrow_gone_from_hosts"
)

// timeoutOutcomeOf names a settled round the way the old ledger names it, so one dashboard reads both.
// The engine reports an action and a reason where the old tree reports a single outcome, and the two
// vocabularies meet only here: `failed` alone says nothing a reader can act on, and its reason is what
// tells a round that gathered too few votes from one that could not gather any.
func timeoutOutcomeOf(action, reason string) (TimeoutOutcome, bool) {
	switch action {
	case timeoutActionSkipped:
		return TimeoutSkipped, true
	case timeoutActionCompleted:
		return TimeoutApplied, true
	case timeoutActionFailed:
		switch reason {
		case timeoutReasonNotApplied:
			return TimeoutInsufficientVotes, true
		case timeoutReasonCollectionError, timeoutReasonEscrowGone:
			return TimeoutVoteCollectionFailed, true
		}
		return TimeoutVoteCollectionFailed, true
	}
	return "", false
}
