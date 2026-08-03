package scheduler

// GhostKind labels why a nonce was burned as a silent ghost probe instead of served.
type GhostKind int

const (
	ghostPoC GhostKind = iota
	ghostThrottled
	ghostEjected
	ghostCapability
	ghostExclude
	// ghostAbandoned is the one kind that is not a probe: the nonce carries the caller's real params.
	// ghostAbandoned accounts a nonce that was committed for a request whose caller vanished before
	// the assignment reached it; the nonce is spent either way and must still have an owner.
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
	case ghostCapability:
		return "participant_capability_no_send"
	case ghostExclude:
		return "no_compatible_request_after_stale"
	case ghostAbandoned:
		return "request_abandoned_before_dispatch"
	default:
		return ""
	}
}
