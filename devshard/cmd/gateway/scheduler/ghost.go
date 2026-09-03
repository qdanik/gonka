package scheduler

// GhostKind labels why a nonce was burned as a silent ghost probe instead of served.
type GhostKind int

const (
	ghostPoC GhostKind = iota
	ghostThrottled
	ghostEjected
	ghostNotAllowed
	ghostStateDiverged
	ghostExclude
	ghostAbandoned
)

func (k GhostKind) reason() string {
	switch k {
	case ghostPoC:
		return GhostReasonPoCUnavailable
	case ghostThrottled:
		return GhostReasonThrottled
	case ghostEjected:
		return GhostReasonEjected
	case ghostNotAllowed:
		return GhostReasonOutsideAllowlist
	case ghostStateDiverged:
		return GhostReasonStateDiverged
	case ghostExclude:
		return GhostReasonNoCompatibleRequest
	case ghostAbandoned:
		return GhostReasonAbandoned
	default:
		return ""
	}
}
