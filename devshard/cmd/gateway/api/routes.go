package api

import (
	"cmp"
	"fmt"
	"net/http"
	"slices"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
	"devshard/types"
	"devshard/user"

	json "github.com/goccy/go-json"
)

// otherRouteLabel is the single label every unmatched path folds into. See gateway-operations.md,
// "Cardinality rules".
const otherRouteLabel = "other"

// route is one registered pattern. An empty label means the route is not instrumented, which is how
// /metrics stays out of its own counters; alwaysOn exempts a route from the operator kill switch.
type route struct {
	pattern  string
	label    string
	admin    bool
	alwaysOn bool
	handler  http.HandlerFunc
}

func (s *Server) routes() []route {
	return []route{
		{pattern: "/v1/models", label: "/v1/models", handler: s.handleModels},
		{pattern: "/v1/chat/completions", label: "/v1/chat/completions", handler: s.handleChat},
		{pattern: "/v1/status", label: "/v1/status", handler: s.handleStatus},
		{pattern: "/metrics", alwaysOn: true, handler: s.handleMetrics},

		{pattern: "/devshard/{id}/v1/models", label: "/devshard/{id}/v1/models", handler: s.handleDevshardModels},
		{pattern: "/devshard/{id}/v1/chat/completions", label: "/devshard/{id}/v1/chat/completions", handler: s.handleDevshardChat},
		{pattern: "/devshard/{id}/v1/status", label: "/devshard/{id}/v1/status", handler: s.handleDevshardStatus},

		{pattern: "/devshard/{id}/v1/finalize", label: "/devshard/{id}/v1/finalize", admin: true, alwaysOn: true, handler: s.handleDevshardFinalize},

		// Recovery surface: admin and alwaysOn. See gateway-operations.md, "Per-escrow recovery surface".
		{pattern: "/devshard/{id}/v1/state", label: "/devshard/{id}/v1/state", admin: true, alwaysOn: true, handler: s.handleDevshardState},
		{pattern: "/devshard/{id}/v1/debug/state", label: "/devshard/{id}/v1/debug/state", admin: true, alwaysOn: true, handler: s.handleDevshardDebugState},
		{pattern: "/devshard/{id}/v1/debug/inferences", label: "/devshard/{id}/v1/debug/inferences", admin: true, alwaysOn: true, handler: s.handleDevshardDebugInferences},
		{pattern: "/devshard/{id}/v1/debug/pending", label: "/devshard/{id}/v1/debug/pending", admin: true, alwaysOn: true, handler: s.handleDevshardDebugPending},
		{pattern: "/devshard/{id}/v1/debug/signatures", label: "/devshard/{id}/v1/debug/signatures", admin: true, alwaysOn: true, handler: s.handleDevshardDebugSignatures},

		// Admin-only: the row has no caller to authorise against. See gateway-operations.md, "Operator".
		{pattern: "/v1/requests/{id}", label: "/v1/requests/{id}", admin: true, alwaysOn: true, handler: s.handleRequestAccounting},

		{pattern: "/v1/admin/state", label: "/v1/admin/state", admin: true, alwaysOn: true, handler: s.handleAdminState},
		{pattern: "/v1/admin/settings", label: "/v1/admin/settings", admin: true, alwaysOn: true, handler: s.handleAdminSettings},
		{pattern: "/v1/admin/devshards", label: "/v1/admin/devshards", admin: true, alwaysOn: true, handler: s.handleAdminDevshards},
		// The templated label is deliberate: the established series covers this path under the same
		// name, and a label of its own would split the panel that reads it.
		{pattern: "/v1/admin/devshards/import", label: "/v1/admin/devshards/{id}", admin: true, alwaysOn: true, handler: s.handleAdminDevshardImport},
		{pattern: "/v1/admin/devshards/{id}", label: "/v1/admin/devshards/{id}", admin: true, alwaysOn: true, handler: s.handleAdminDevshardDelete},
		{pattern: "/v1/admin/devshards/{id}/activate", label: "/v1/admin/devshards/{id}/activate", admin: true, alwaysOn: true, handler: s.handleAdminDevshardActivate},
		{pattern: "/v1/admin/devshards/{id}/deactivate", label: "/v1/admin/devshards/{id}/deactivate", admin: true, alwaysOn: true, handler: s.handleAdminDevshardDeactivate},
		{pattern: "/v1/admin/devshards/{id}/settle", label: "/v1/admin/devshards/{id}/settle", admin: true, alwaysOn: true, handler: s.handleAdminDevshardSettle},
		{pattern: "/v1/admin/devshards/{id}/participants", label: "/v1/admin/devshards/{id}/participants", admin: true, alwaysOn: true, handler: s.handleAdminDevshardParticipants},
		{pattern: "/v1/admin/escrows", label: "/v1/admin/escrows", admin: true, alwaysOn: true, handler: s.handleAdminEscrows},
		{pattern: "/v1/admin/suspicious-hosts", label: "/v1/admin/suspicious-hosts", admin: true, alwaysOn: true, handler: s.handleAdminSuspiciousHosts},
		{pattern: "/v1/admin/participants/unquarantine", label: "/v1/admin/participants/unquarantine", admin: true, alwaysOn: true, handler: s.handleAdminUnquarantine},
		{pattern: "/v1/debug/rotation", label: "/v1/debug/rotation", admin: true, alwaysOn: true, handler: s.handleDebugRotation},
		{pattern: "/v1/debug/memstats", label: "/v1/debug/memstats", admin: true, alwaysOn: true, handler: s.handleDebugMemstats},

		{pattern: "/", label: otherRouteLabel, alwaysOn: true, handler: s.handleUnmatched},
	}
}

func (s *Server) handleUnmatched(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotFound, "not found")
}

func (s *Server) buildHandler() http.Handler {
	mux := http.NewServeMux()
	for _, registered := range s.routes() {
		handler := registered.handler
		if registered.admin {
			handler = adminFailure(registered.label, s.requireAdmin(handler))
		}
		if !registered.alwaysOn {
			handler = s.disabled(handler)
		}
		var wrapped http.Handler = handler
		if registered.label != "" {
			wrapped = s.telemetry.InstrumentRoute(registered.label, wrapped)
		}
		mux.Handle(registered.pattern, wrapped)
	}
	return mux
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	s.telemetry.Handler().ServeHTTP(w, r)
}

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

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	writeJSON(w, http.StatusOK, s.modelList(s.escrows.Models()))
}

func (s *Server) handleDevshardModels(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	escrow, found := s.routableEscrow(r.PathValue("id"))
	if !found {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, s.modelList([]string{escrow.Model}))
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

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	writeJSON(w, http.StatusOK, s.status(s.routableEscrows()))
}

func (s *Server) handleDevshardStatus(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	escrow, found := s.routableEscrow(r.PathValue("id"))
	if !found {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, r.PathValue("id")))
		return
	}
	writeJSON(w, http.StatusOK, s.status([]scheduler.Escrow{escrow}))
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

func (s *Server) routableEscrows() []scheduler.Escrow {
	models := s.escrows.Models()
	escrows := make([]scheduler.Escrow, 0, len(models))
	for _, model := range models {
		escrows = append(escrows, s.escrows.Candidates(model)...)
	}
	return escrows
}

func (s *Server) routableEscrow(escrowID string) (scheduler.Escrow, bool) {
	for _, escrow := range s.routableEscrows() {
		if escrow.ID == escrowID {
			return escrow, true
		}
	}
	return scheduler.Escrow{}, false
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	s.chat(w, r, "")
}

// handleDevshardChat pins the race to one escrow, and refuses a pin the gateway no longer routes to.
// See gateway-operations.md, "Client".
func (s *Server) handleDevshardChat(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	escrowID := r.PathValue("id")
	if _, routable := s.routableEscrow(escrowID); !routable {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	s.chat(w, r, escrowID)
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request, escrowPin string) {
	configuration := s.config.Load()
	identity := s.resolveCredentials(r)
	requestID := s.requestIDs()

	body, err := readBody(w, r, chatIngestLimit)
	if err != nil {
		writeErrorFor(w, err)
		return
	}
	normalized, err := filters.NormalizeRequest(body, filters.Options{
		Admin:            identity.admin,
		DefaultMaxTokens: uint64(configuration.Limits.DefaultMaxTokens),
		MaxTokensCap:     uint64(configuration.Limits.MaxTokensCap),
		ModelTokenLimits: modelTokenLimits(configuration.Limits.ModelLimits),
	})
	if err != nil {
		s.capture.filterRejected(r, requestID, body, err)
		writeErrorFor(w, err)
		return
	}
	if err := authorizeModel(configuration.Limits, normalized.Model, identity); err != nil {
		writeErrorFor(w, err)
		return
	}
	if err := s.routableModel(normalized.Model); err != nil {
		writeErrorFor(w, err)
		return
	}
	if err := admission(s.snapshots.Snapshot(), configuration.Modes); err != nil {
		writeErrorFor(w, err)
		return
	}

	key := cacheKeyFor(r, normalized.Model, normalized.Body, normalized.Logprobs)
	if entry, hit := s.cache.get(key, s.now()); hit {
		written := serveCached(w, requestID, entry)
		logging.Info("request finished", "request", requestID, "model", normalized.Model,
			"escrow", entry.escrowID, "stream", entry.stream, "outcome", "cache_hit", "bytes", written)
		return
	}

	inputTokens := estimatePromptTokens(normalized.Body)
	if err := s.limiter.AcquireForModel(r.Context(), normalized.Model, int64(inputTokens), s.capacity.ForModel(normalized.Model)); err != nil {
		writeErrorFor(w, err)
		return
	}
	defer s.limiter.ReleaseForModel(normalized.Model, int64(inputTokens))

	if s.cache == nil {
		s.race(w, r, requestID, normalized, inputTokens, escrowPin)
		return
	}
	recorder := &cacheRecorder{ResponseWriter: w, limit: s.cache.entryLimit()}
	outcome, hiddenFailure := s.race(recorder, r, requestID, normalized, inputTokens, escrowPin)
	if entry, storable := recorder.entry(outcome.EscrowID, normalized.Stream, cmp.Or(r.Context().Err(), hiddenFailure)); storable {
		s.cache.put(key, entry, s.now())
	}
}

// routableModel refuses an unroutable model before it can take a limiter slot or an input-token
// budget, and fails closed on an empty registry. See gateway-request-lifecycle.md, "3. Authorisation
// and routability".
func (s *Server) routableModel(model string) error {
	if model == "" {
		return filters.Reject("model is required")
	}
	served := s.escrows.Models()
	if len(served) == 0 {
		return &ModelUnavailableError{Model: model}
	}
	if !s.escrows.Serves(model) {
		return &UnsupportedModelError{Model: model, Supported: served}
	}
	return nil
}

func authorizeModel(configured config.Limits, model string, identity credentials) error {
	if identity.admin {
		return nil
	}
	switch configured.AccessFor(model) {
	case config.ModelAccessOpen:
		return nil
	case config.ModelAccessAPIKey:
		if identity.apiKey {
			return nil
		}
		return &AccessDeniedError{Model: model, Message: fmt.Sprintf("model %q requires an API key", model)}
	default:
		return &AccessDeniedError{Model: model, Message: fmt.Sprintf("model %q requires an admin API key", model)}
	}
}

// race runs one request to its winner and returns the outcome together with the failure the reply's
// status could not carry. A stream commits 200 on its first byte, so a race that fails after that
// leaves an SSE error event under a success status; the caller needs the error itself to tell that
// response apart from one worth replaying.
func (s *Server) race(w http.ResponseWriter, r *http.Request, requestID string, normalized filters.Result, inputTokens uint64, escrowPin string) (engine.RaceOutcome, error) {
	client := newClientStream(w, requestID, normalized.Stream, normalized.Logprobs)
	outcome, err := s.inference.Run(r.Context(), engine.Request{
		RequestID:     requestID,
		Model:         normalized.Model,
		Escrow:        escrowPin,
		InputTokens:   inputTokens,
		Stream:        normalized.Stream,
		RequiresTools: requiresTools(normalized.Body),
		OnEscrow:      func(escrowID string) { client.Header().Set(EscrowHeader, escrowID) },

		ReduceMaxTokens: reduceMaxTokens,
		Params: user.InferenceParams{
			Model:       normalized.Model,
			Prompt:      normalized.Body,
			InputLength: uint64(len(normalized.Body)),
			MaxTokens:   outputTokenBudget(normalized),
			StartedAt:   s.now().Unix(),
			Stream:      normalized.Stream,
		},
	}, client)
	if err != nil {
		s.capture.attemptsFailed(r, requestID, normalized, err)
		if client.Started() {
			logRequestFinished(requestID, normalized, outcome, "failed_mid_stream", client, err, client.Fail(err))
			return outcome, err
		}
		logRequestFinished(requestID, normalized, outcome, "failed_before_first_byte", client, err, nil)
		w.Header().Set(RequestIDHeader, requestID)
		writeErrorFor(w, err)
		return outcome, nil
	}
	// The second of the two X-Devshard-ID writes, authoritative for a reply still holding its headers.
	// See gateway-request-lifecycle.md, "9. Streaming out".
	if outcome.EscrowID != "" {
		client.Header().Set(EscrowHeader, outcome.EscrowID)
	}
	logRequestFinished(requestID, normalized, outcome, "served", client, nil, client.Close())
	return outcome, nil
}

// logRequestFinished answers the one question a finished request can no longer be asked: how much
// reached the client and whether the terminator went with it. A delivery error is the difference
// between a reply the client read and one it is still waiting out its own timeout for.
func logRequestFinished(requestID string, normalized filters.Result, outcome engine.RaceOutcome, verdict string, stream *clientStream, raceErr, deliverErr error) {
	written, terminated := stream.delivered()
	fields := []any{
		"request", requestID,
		"model", normalized.Model,
		"escrow", outcome.EscrowID,
		"stream", normalized.Stream,
		"outcome", verdict,
		"bytes", written,
		"terminated", terminated,
	}
	if raceErr != nil {
		fields = append(fields, "error", loggedError(raceErr))
	}
	if deliverErr != nil {
		fields = append(fields, "deliver_error", loggedError(deliverErr))
	}
	if raceErr != nil || deliverErr != nil {
		logging.Warn("request finished", fields...)
		return
	}
	logging.Info("request finished", fields...)
}

// estimatePromptTokens is the limiter's and the perf buckets' input size, not a tokenizer call. See
// gateway-request-lifecycle.md, "6. Admission to capacity".
func estimatePromptTokens(body []byte) uint64 {
	if estimated := (len(body) + 3) / 4; estimated > 0 {
		return uint64(estimated)
	}
	return 1
}

// reduceMaxTokens is engine.Request's body-shaped hook: the non-streaming fallback retries a host that
// went quiet with half the output-token budget, and the escrow commits that halved budget alongside the
// body carrying it. InputLength is left on the original prompt, as the committed record's own value.
func reduceMaxTokens(params any) (any, bool) {
	dispatched, isInference := params.(user.InferenceParams)
	if !isInference {
		return nil, false
	}
	prompt, maxTokens, ok := filters.HalveMaxTokens(dispatched.Prompt, dispatched.MaxTokens, dispatched.Model)
	if !ok {
		return nil, false
	}
	dispatched.Prompt, dispatched.MaxTokens = prompt, maxTokens
	return dispatched, true
}

func outputTokenBudget(normalized filters.Result) uint64 {
	if normalized.MaxTokens > 0 {
		return normalized.MaxTokens
	}
	return normalized.MaxCompletionTokens
}

func requiresTools(body []byte) bool {
	var request struct {
		Tools      []json.RawMessage `json:"tools"`
		ToolChoice *json.RawMessage  `json:"tool_choice"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return false
	}
	if len(request.Tools) > 0 {
		return true
	}
	if request.ToolChoice == nil {
		return false
	}
	var choice string
	if err := json.Unmarshal(*request.ToolChoice, &choice); err == nil {
		return choice != "none"
	}
	return true
}
