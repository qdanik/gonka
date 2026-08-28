package scheduler

// GhostKind labels why a nonce was burned as a silent ghost probe instead of served.
type GhostKind int

const (
	ghostPoC GhostKind = iota
	ghostThrottled
	ghostEjected
	ghostNotAllowed
	ghostCapability
	ghostStateDiverged
	ghostExclude
	ghostAbandoned
)

// Exported so a caller outside the scheduler can tell the burns a host earned from the ones it did not.
const (
	GhostReasonThrottled     = "participant_throttled_no_send"
	GhostReasonStateDiverged = "participant_state_diverged_no_send"
)

func (k GhostKind) reason() string {
	switch k {
	case ghostPoC:
		return "poc_unavailable_host"
	case ghostThrottled:
		return GhostReasonThrottled
	case ghostEjected:
		return "participant_ejected_no_send"
	case ghostNotAllowed:
		return "participant_outside_allowlist"
	case ghostCapability:
		return "participant_capability_no_send"
	case ghostStateDiverged:
		return GhostReasonStateDiverged
	case ghostExclude:
		return "no_compatible_request_after_stale"
	case ghostAbandoned:
		return "request_abandoned_before_dispatch"
	default:
		return ""
	}
}
