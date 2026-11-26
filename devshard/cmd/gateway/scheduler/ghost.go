package scheduler

import "devshard/cmd/gateway/share"

// GhostKind labels why a nonce was burned as a silent ghost probe instead of served.
type GhostKind int

const (
	ghostPoC GhostKind = iota
	ghostWindowFull
	ghostCutOff
	ghostEjected
	ghostUntrusted
	ghostStateDiverged
	ghostExclude
	ghostAbandoned
	ghostPrepareFailed
	ghostAbsentFromChain
)

// shareKind is the share-ledger count a burn of this kind books besides the assignment; false for a nonce no host load or host state explains.
func (k GhostKind) shareKind() (share.Kind, bool) {
	switch k {
	case ghostWindowFull, ghostCutOff, ghostEjected:
		return share.Passed, true
	case ghostPoC, ghostUntrusted, ghostStateDiverged, ghostExclude, ghostAbsentFromChain:
		return share.Excused, true
	}
	return 0, false
}

func (k GhostKind) reason() string {
	switch k {
	case ghostPoC:
		return GhostReasonPoCUnavailable
	case ghostWindowFull:
		return GhostReasonWindowFull
	case ghostCutOff:
		return GhostReasonCutOff
	case ghostEjected:
		return GhostReasonEjected
	case ghostUntrusted:
		return GhostReasonUntrusted
	case ghostStateDiverged:
		return GhostReasonStateDiverged
	case ghostExclude:
		return GhostReasonNoCompatibleRequest
	case ghostAbandoned:
		return GhostReasonAbandoned
	case ghostPrepareFailed:
		return GhostReasonPrepareFailed
	case ghostAbsentFromChain:
		return GhostReasonAbsentFromChain
	default:
		return ""
	}
}
