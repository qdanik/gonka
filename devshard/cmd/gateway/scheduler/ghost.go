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

func (k GhostKind) reason() string {
	switch k {
	case ghostPoC:
		return "poc_unavailable_host"
	case ghostThrottled:
		return "participant_throttled_no_send"
	case ghostEjected:
		return "participant_ejected_no_send"
	case ghostNotAllowed:
		return "participant_outside_allowlist"
	case ghostCapability:
		return "participant_capability_no_send"
	case ghostStateDiverged:
		return "participant_state_diverged_no_send"
	case ghostExclude:
		return "no_compatible_request_after_stale"
	case ghostAbandoned:
		return "request_abandoned_before_dispatch"
	default:
		return ""
	}
}
