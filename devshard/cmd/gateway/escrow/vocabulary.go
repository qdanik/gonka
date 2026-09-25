package escrow

import "devshard/cmd/gateway/scheduler"

const (
	roleTemp    = "temp"
	roleRegular = "regular"
	RoleReserve = "reserve"
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

type holdEnding string

const (
	holdEndedDisabled     holdEnding = "hold_disabled"
	holdEndedRotationOff  holdEnding = "rotation_off"
	holdEndedEpochPassed  holdEnding = "epoch_passed"
	holdEndedEpochUnknown holdEnding = "epoch_unknown"
	holdEndedNonceSpent   holdEnding = holdEnding(scheduler.ExhaustionNonceCap)
)
