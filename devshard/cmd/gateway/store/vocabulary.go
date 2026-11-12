package store

import "time"

// Role is what an escrow is for in the rotation, as a row stores it.
type Role string

// RequestOutcome is how a request ended, as the ledger stores it.
type RequestOutcome string

const (
	RoleUnset   Role = ""
	RoleTemp    Role = "temp"
	RoleRegular Role = "regular"
	RoleReserve Role = "reserve"
)

const (
	RequestSettled  RequestOutcome = "settled"
	RequestFailed   RequestOutcome = "failed"
	RequestNoEscrow RequestOutcome = "no_escrow"
)

const (
	ledgerQueueDepth       = 1024
	retentionSweepEvery    = time.Minute
	owedVoteQueueDepth     = 1024
	maxOwedVotePromptBytes = 1 << 20
)
