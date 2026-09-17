package scheduler

// The strings this package puts on the wire, declared once and referenced by name. See routing.md, "Ghost burns".

// Why a nonce was burned instead of served.
const (
	GhostReasonPoCUnavailable      = "poc_unavailable_host"
	GhostReasonWindowFull          = "participant_window_full_no_send"
	GhostReasonCutOff              = "participant_cut_off_no_send"
	GhostReasonEjected             = "participant_ejected_no_send"
	GhostReasonOutsideAllowlist    = "participant_outside_allowlist"
	GhostReasonStateDiverged       = "participant_state_diverged_no_send"
	GhostReasonNoCompatibleRequest = "no_compatible_request_after_stale"
	GhostReasonAbandoned           = "request_abandoned_before_dispatch"
)

// Why an escrow may no longer be picked.
const (
	exhaustionNonceCap     = "nonce_cap"
	exhaustionBalanceFloor = "balance_floor"
)
