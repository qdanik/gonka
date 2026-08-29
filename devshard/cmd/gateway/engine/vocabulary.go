package engine

// The strings the engine puts on the wire: metric label values, log fields, and the facts the nonce
// ledger classifies on. They are a contract with everything downstream -- a dashboard query, an alert,
// a stored counter -- so they are declared here once and referenced by name. Renaming a constant breaks
// the build; editing its value silently moves the wire string under every panel that reads it.

// Why an attempt started, reported on every nonce the race commits.
const (
	StartPrimary           = "primary"
	StartPrimarySuspicious = "primary_suspicious"
	StartPrimaryDegraded   = "primary_degraded"
)

// EscalationStage names the condition that earned an attempt one more attempt beside it.
const (
	StageNone           EscalationStage = ""
	StageSuspicious     EscalationStage = "suspicious_host_immediate_escalation"
	StageAttemptFailed  EscalationStage = "attempt_failed"
	StageReceiptTimeout EscalationStage = "receipt_timeout_wait_elapsed"
	StageFirstToken     EscalationStage = "first_token_timeout_wait_elapsed"
)

// An attempt's place in its race, and why the crown fell to it.
const (
	RolePrimary     = "primary"
	RoleSpeculative = "speculative"

	crownFirstClaim = "first_claim"
	crownNoRival    = "no_rival"
)

// How an attempt ended, and what the client saw of it. Visibility is the only account of a race that
// produced an answer nobody received.
const (
	AttemptOutcomeSuccess = "success"
	AttemptOutcomeFailed  = "failed"

	VisibilityWinner            = "user_visible_winner"
	VisibilityWinnerClientGone  = "winner_client_gone"
	VisibilityNoWinner          = "no_winner"
	VisibilitySuppressedLoser   = "suppressed_loser"
	VisibilityFailedNotFinished = "failed_not_finished"

	// An answer that arrived complete and was given to nobody.
	reasonCrownDenied = "crown_denied"
)

// The vocabulary of a TimeoutEvent: what a nonce was owed, what the gateway did about it, and why. A
// reason outside this set normalises to "unknown" in the ledger's report, and the fact becomes invisible.
const (
	TimeoutKindRefused   = "refused"
	TimeoutKindExecution = "execution"

	TimeoutActionSkipped   = "skipped"
	TimeoutActionStarted   = "started"
	TimeoutActionCompleted = "completed"
	TimeoutActionFailed    = "failed"
	// A nonce whose race died with the process: no vote was ever posted on it.
	TimeoutActionAbandoned = "abandoned_by_restart"

	TimeoutReasonNone            = "none"
	TimeoutReasonNoPoster        = "no_poster"
	TimeoutReasonPhaseAborted    = "phase_transition_aborted"
	TimeoutReasonEmptyStream     = "empty_stream_without_non_empty_winner"
	TimeoutReasonNonceFinished   = "nonce_already_finished"
	TimeoutReasonLongResponse    = "long_response_after_content"
	TimeoutReasonCollectionError = "timeout_collection_error"
	TimeoutReasonNotApplied      = "timeout_not_applied"
	TimeoutReasonHostServedProbe = "host_served_probe"
	TimeoutReasonEscrowNotLive   = "escrow_not_routable"
	// A vote no retry can win: the hosts dropped the escrow, so the nonce it would have settled is
	// unsettleable and pays its full reserve.
	TimeoutReasonEscrowGone = "escrow_gone_from_hosts"
)
