package scheduler

// GhostKind labels why a nonce was burned as a silent ghost probe instead of served.
type GhostKind int

const (
	ghostPoC GhostKind = iota
	ghostThrottled
	ghostCapability
	ghostExclude
)

// reason returns the ghost's log/metric label.
func (k GhostKind) reason() string {
	switch k {
	case ghostPoC:
		return "poc_unavailable_host"
	case ghostThrottled:
		return "participant_throttled_no_send"
	case ghostCapability:
		return "participant_capability_no_send"
	case ghostExclude:
		return "no_compatible_request_after_stale"
	default:
		return ""
	}
}
