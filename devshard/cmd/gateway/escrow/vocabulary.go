package escrow

const (
	roleTemp    = "temp"
	roleRegular = "regular"
)

const (
	stagePrepareTemp   = "prepare_temp"
	stageFinishRegular = "finish_regular"
)

const (
	commitmentClearedNoEscrow   = "transaction created no escrow"
	commitmentClearedCannotLand = "transaction can no longer land"
)

type HoldVerdict int

const (
	HoldKeep HoldVerdict = iota
	HoldResume
	HoldNonceSpent
)

// depletionReasonNonceCap is the scheduler's exhaustionNonceCap wire string; a nonce-capped escrow can never recover.
const depletionReasonNonceCap = "nonce_cap"

const (
	holdEndedDisabled     = "hold_disabled"
	holdEndedRotationOff  = "rotation_off"
	holdEndedEpochPassed  = "epoch_passed"
	holdEndedEpochUnknown = "epoch_unknown"
	holdEndedNonceSpent   = "nonce_cap"
)
