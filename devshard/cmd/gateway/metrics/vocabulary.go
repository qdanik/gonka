package metrics

// What one sweep tick did with the nonces it found.
const (
	sweepOutcomeApplied = "applied"
	sweepOutcomeFailed  = "failed"
)

// What a probe wave did.
const (
	HostPingTickStarted = "started"
	HostPingTickSkipped = "skipped"
)
