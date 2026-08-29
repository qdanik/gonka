package config

import (
	"bytes"
	"fmt"

	json "github.com/goccy/go-json"
)

// Overrides is the admin-tunable subset of Config, persisted by the store and
// merged over env by Build. Nil field = not overridden.
type Overrides struct {
	DefaultMaxTokens                       *int64                 `json:"default_max_tokens,omitempty"`
	MaxTokensCap                           *int64                 `json:"max_tokens_cap,omitempty"`
	MaxConcurrentRequests                  *int64                 `json:"max_concurrent_requests,omitempty"`
	MaxConcurrentRequestsPer10000Weight    *float64               `json:"max_concurrent_requests_per_10000_weight,omitempty"`
	PoCMaxConcurrentRequestsPer10000Weight *float64               `json:"poc_max_concurrent_requests_per_10000_weight,omitempty"`
	MaxInputTokensInFlight                 *int64                 `json:"max_input_tokens_in_flight,omitempty"`
	AdmissionQueueWaitMS                   *int64                 `json:"admission_queue_wait_ms,omitempty"`
	AdmissionQueuePerSlot                  *int64                 `json:"admission_queue_per_slot,omitempty"`
	MatchWaitMS                            *int64                 `json:"match_wait_ms,omitempty"`
	ForceUpstreamStreaming                 *bool                  `json:"force_upstream_streaming,omitempty"`
	MaxBufferedResponseBytes               *int64                 `json:"max_buffered_response_bytes,omitempty"`
	WarmNewEscrows                         *bool                  `json:"warm_new_escrows,omitempty"`
	ChargeRefusedNonces                    *bool                  `json:"charge_refused_nonces,omitempty"`
	ParticipantAllowlist                   *[]string              `json:"participant_allowlist,omitempty"`
	HostInitialInflight                    *int64                 `json:"host_initial_inflight,omitempty"`
	HostMaxInflight                        *int64                 `json:"host_max_inflight,omitempty"`
	HostCutoffAfterFailures                *int64                 `json:"host_cutoff_after_failures,omitempty"`
	HostCutoffMS                           *int64                 `json:"host_cutoff_ms,omitempty"`
	HostCutoffMaxMS                        *int64                 `json:"host_cutoff_max_ms,omitempty"`
	ModelLimits                            map[string]ModelLimits `json:"model_limits,omitempty"`
	ModelAccess                            map[string]string      `json:"model_access,omitempty"`
	Disabled                               *bool                  `json:"disabled,omitempty"`
	DisabledMessage                        *string                `json:"disabled_message,omitempty"`
	DisabledRedirectURL                    *string                `json:"disabled_redirect_url,omitempty"`
	RotationEnabled                        *bool                  `json:"rotation_enabled,omitempty"`
	RotationSettlementEnabled              *bool                  `json:"rotation_settlement_enabled,omitempty"`
	RotationPrePoCBlocks                   *int64                 `json:"rotation_pre_poc_blocks,omitempty"`
	RotationModelsJSON                     *string                `json:"rotation_models_json,omitempty"`
}

// ParseOverrides decodes admin-supplied override JSON. Unknown fields are an
// error: a typo in an admin PUT must be reported, never silently ignored.
func ParseOverrides(raw []byte) (Overrides, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var overrides Overrides
	if err := decoder.Decode(&overrides); err != nil {
		return Overrides{}, fmt.Errorf("parsing overrides: %w", err)
	}
	return overrides, nil
}

func (o Overrides) EncodeOverrides() ([]byte, error) {
	encoded, err := json.Marshal(o)
	if err != nil {
		return nil, fmt.Errorf("encoding overrides: %w", err)
	}
	return encoded, nil
}
