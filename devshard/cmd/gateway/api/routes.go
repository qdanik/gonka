package api

import (
	"cmp"
	"fmt"
	"net/http"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
	"devshard/user"
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

	key := cacheKeyFor(r, normalized.Model, normalized.Body, normalized.Logprobs, normalized.ClientStream)
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
	if entry, storable := recorder.entry(outcome.EscrowID, normalized.ClientStream, cmp.Or(r.Context().Err(), hiddenFailure)); storable {
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
	startedAt := s.now()
	client := newClientStream(w, requestID, normalized.ClientStream, normalized.ClientUsage, normalized.Logprobs)
	outputTokens := outputTokenBudget(normalized)
	outcome, err := s.inference.Run(r.Context(), engine.Request{
		RequestID:     requestID,
		Model:         normalized.Model,
		Escrow:        escrowPin,
		InputTokens:   inputTokens,
		ClientStream:  normalized.ClientStream,
		RequiresTools: normalized.RequiresTools,
		OnEscrow:      func(escrowID string) { client.Header().Set(EscrowHeader, escrowID) },

		Params: user.InferenceParams{
			Model:       normalized.Model,
			Prompt:      normalized.Body,
			InputLength: uint64(len(normalized.Body)),
			MaxTokens:   outputTokens,
			StartedAt:   s.now().Unix(),
			Stream:      true,
		},
	}, client)
	if err != nil {
		s.capture.attemptsFailed(r, requestID, normalized, err)
		if client.Started() {
			logRequestFinished(requestID, normalized, outcome, "failed_mid_stream", client, s.now().Sub(startedAt), err, client.Fail(err))
			return outcome, err
		}
		logRequestFinished(requestID, normalized, outcome, "failed_before_first_byte", client, s.now().Sub(startedAt), err, nil)
		w.Header().Set(RequestIDHeader, requestID)
		writeErrorFor(w, err)
		return outcome, nil
	}
	// The second of the two X-Devshard-ID writes, authoritative for a reply still holding its headers.
	// See gateway-request-lifecycle.md, "9. Streaming out".
	if outcome.EscrowID != "" {
		client.Header().Set(EscrowHeader, outcome.EscrowID)
	}
	logRequestFinished(requestID, normalized, outcome, "served", client, s.now().Sub(startedAt), nil, client.Close())
	return outcome, nil
}
