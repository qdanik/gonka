package api

import (
	"slices"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/store"
	"devshard/types"
)

type modelListResponse struct {
	Object string            `json:"object"`
	Data   []modelDescriptor `json:"data"`
}

type modelDescriptor struct {
	ID                  string            `json:"id"`
	Object              string            `json:"object"`
	Created             int64             `json:"created"`
	OwnedBy             string            `json:"owned_by"`
	Name                string            `json:"name"`
	ContextLength       uint64            `json:"context_length,omitempty"`
	MaxCompletionTokens uint64            `json:"max_completion_tokens,omitempty"`
	Architecture        modelArchitecture `json:"architecture"`
	Pricing             modelPricing      `json:"pricing"`
	TopProvider         modelTopProvider  `json:"top_provider"`
}

type modelArchitecture struct {
	Modality         string   `json:"modality"`
	InputModalities  []string `json:"input_modalities"`
	OutputModalities []string `json:"output_modalities"`
	Tokenizer        string   `json:"tokenizer,omitempty"`
	InstructType     string   `json:"instruct_type,omitempty"`
}

type modelPricing struct {
	Prompt     string `json:"prompt"`
	Completion string `json:"completion"`
	Request    string `json:"request"`
}

type modelTopProvider struct {
	ContextLength       uint64 `json:"context_length,omitempty"`
	MaxCompletionTokens uint64 `json:"max_completion_tokens,omitempty"`
	IsModerated         bool   `json:"is_moderated"`
}

// modelTokenLimits closes over the per-model override map only when an operator configured one, so
// a deployment without overrides allocates nothing on the request path.
func modelTokenLimits(overrides map[string]config.ModelLimits) func(string) (uint64, uint64) {
	if len(overrides) == 0 {
		return nil
	}
	return func(model string) (uint64, uint64) {
		override := overrides[model]
		return uint64(override.DefaultMaxTokens), uint64(override.MaxTokensCap)
	}
}

func (s *Server) modelList(models []string) modelListResponse {
	limits := s.config.Load().Limits
	created := s.now().Unix()
	data := make([]modelDescriptor, 0, len(models))
	for _, model := range models {
		tokenCap := uint64(limits.MaxTokensCap)
		if perModel, ok := limits.ModelLimits[model]; ok && perModel.MaxTokensCap > 0 {
			tokenCap = uint64(perModel.MaxTokensCap)
		}
		data = append(data, modelDescriptor{
			ID:                  model,
			Object:              "model",
			Created:             created,
			OwnedBy:             "gonka",
			Name:                model,
			ContextLength:       tokenCap,
			MaxCompletionTokens: tokenCap,
			Architecture: modelArchitecture{
				Modality:         "text->text",
				InputModalities:  []string{"text"},
				OutputModalities: []string{"text"},
				Tokenizer:        "Other",
			},
			Pricing:     modelPricing{Prompt: "0", Completion: "0", Request: "0"},
			TopProvider: modelTopProvider{ContextLength: tokenCap, MaxCompletionTokens: tokenCap},
		})
	}
	return modelListResponse{Object: "list", Data: data}
}

type statusResponse struct {
	Mode            string            `json:"mode"`
	Version         string            `json:"version"`
	RequestsBlocked bool              `json:"requests_blocked"`
	BlockReason     chain.BlockReason `json:"block_reason,omitempty"`
	Models          []string          `json:"models"`
	Devshards       []devshardStatus  `json:"devshards"`
	Limiter         limiterStatus     `json:"limiter"`
	Capacity        []capacityStatus  `json:"capacity"`
}

// The two views below keep the storage rows out of the response: without them a column rename in the
// store silently changes the API, and the record's own field names reach the client as written.
type devshardView struct {
	EscrowID          string `json:"escrow_id"`
	PrivateKeyEnv     string `json:"private_key_env"`
	Model             string `json:"model"`
	Active            bool   `json:"active"`
	RotationRole      string `json:"rotation_role"`
	RotationEpoch     int64  `json:"rotation_epoch"`
	SettlementPending bool   `json:"settlement_pending"`
	SettleTxHash      string `json:"settle_tx_hash,omitempty"`
}

type rotationView struct {
	Model       string    `json:"model"`
	Role        string    `json:"role"`
	Stage       string    `json:"stage"`
	Epoch       uint64    `json:"epoch"`
	Completed   bool      `json:"completed"`
	CreateError string    `json:"create_error,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func devshardViews(records []store.DevshardRecord) []devshardView {
	views := make([]devshardView, 0, len(records))
	for _, record := range records {
		views = append(views, devshardView{
			EscrowID:          record.EscrowID,
			PrivateKeyEnv:     record.PrivateKeyEnv,
			Model:             record.Model,
			Active:            record.Active,
			RotationRole:      record.RotationRole,
			RotationEpoch:     record.RotationEpoch,
			SettlementPending: record.SettlementPending,
			SettleTxHash:      record.SettleTxHash,
		})
	}
	return views
}

func rotationViews(statuses []store.RotationStatus) []rotationView {
	views := make([]rotationView, 0, len(statuses))
	for _, status := range statuses {
		views = append(views, rotationView{
			Model:       status.Model,
			Role:        status.Role,
			Stage:       status.Stage,
			Epoch:       status.Epoch,
			Completed:   status.Completed,
			CreateError: status.CreateError,
			UpdatedAt:   status.UpdatedAt,
		})
	}
	return views
}

// SessionVersion is the protocol tag the session was created under and the one every settlement payload
// carries. An operator reading this endpoint has no other way to see which version an escrow is bound
// to, and a mismatch with the host it dispatches to is what makes a settlement unacceptable.
type devshardStatus struct {
	EscrowID       string `json:"escrow_id"`
	Model          string `json:"model"`
	ActiveUsers    int    `json:"active_users"`
	Nonce          uint64 `json:"nonce"`
	Phase          string `json:"phase"`
	Balance        uint64 `json:"balance,omitempty"`
	SessionVersion string `json:"session_version,omitempty"`
}

type limiterStatus struct {
	MaxConcurrentRequests  int64 `json:"max_concurrent_requests"`
	MaxInputTokensInFlight int64 `json:"max_input_tokens_in_flight"`
	DefaultMaxTokens       int64 `json:"default_max_tokens"`
	MaxTokensCap           int64 `json:"max_tokens_cap"`
}

// capacityStatus carries each weight under both spellings, old and new. See gateway-operations.md,
// "Reading the gateway's state".
type capacityStatus struct {
	Model                       string  `json:"model"`
	CurrentWeight               float64 `json:"current_weight"`
	TotalWeight                 float64 `json:"total_weight"`
	BaselineWeight              float64 `json:"baseline_weight"`
	FullWeight                  float64 `json:"full_weight"`
	ScaleFactor                 float64 `json:"scale_factor"`
	LimitShare                  float64 `json:"limit_share"`
	MaxConcurrentPer10000Weight float64 `json:"max_concurrent_per_10000_weight"`
}

func (s *Server) status(escrows []scheduler.Escrow) statusResponse {
	configuration := s.config.Load()
	snapshot := s.snapshots.Snapshot()
	models := make([]string, 0, len(escrows))
	devshards := make([]devshardStatus, 0, len(escrows))
	for _, escrow := range escrows {
		if !slices.Contains(models, escrow.Model) {
			models = append(models, escrow.Model)
		}
		devshards = append(devshards, s.devshardStatus(escrow))
	}
	capacity := make([]capacityStatus, 0, len(models))
	for _, model := range models {
		modelCapacity := s.capacity.ForModel(model)
		capacity = append(capacity, capacityStatus{
			Model:                       model,
			CurrentWeight:               modelCapacity.CurrentWeight,
			TotalWeight:                 modelCapacity.CurrentWeight,
			BaselineWeight:              modelCapacity.BaselineWeight,
			FullWeight:                  modelCapacity.BaselineWeight,
			ScaleFactor:                 modelCapacity.ScaleFactor,
			LimitShare:                  modelCapacity.ScaleFactor,
			MaxConcurrentPer10000Weight: modelCapacity.MaxConcurrentPer10000Weight,
		})
	}
	return statusResponse{
		Mode:            configuration.Modes.PoCMode,
		Version:         s.version,
		RequestsBlocked: admission(snapshot, configuration.Modes) != nil,
		BlockReason:     snapshot.BlockReason,
		Models:          models,
		Devshards:       devshards,
		Limiter: limiterStatus{
			MaxConcurrentRequests:  configuration.Limits.Concurrency.MaxRequests,
			MaxInputTokensInFlight: configuration.Limits.MaxInputTokensInFlight,
			DefaultMaxTokens:       configuration.Limits.DefaultMaxTokens,
			MaxTokensCap:           configuration.Limits.MaxTokensCap,
		},
		Capacity: capacity,
	}
}

func (s *Server) devshardStatus(escrow scheduler.Escrow) devshardStatus {
	status := devshardStatus{EscrowID: escrow.ID, Model: escrow.Model, ActiveUsers: escrow.ActiveUsers}
	session, held := s.escrows.RoutableSession(escrow.ID)
	if !held {
		return status
	}
	state := session.SnapshotState()
	status.Nonce = session.Nonce()
	status.Phase = phaseName(session.Phase())
	status.Balance = state.Balance
	status.SessionVersion = state.StateRootAndProtocolVersion
	return status
}

func phaseName(phase types.SessionPhase) string {
	switch phase {
	case types.PhaseActive:
		return "active"
	case types.PhaseFinalizing:
		return "finalizing"
	case types.PhaseSettlement:
		return "settlement"
	}
	return "unknown"
}
