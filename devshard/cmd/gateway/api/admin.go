package api

import (
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"devshard/cmd/gateway/config"
	"devshard/logging"
)

func (s *Server) handleAdminState(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet) {
		return
	}
	records, err := s.control.ListDevshards(r.Context())
	if writeControlFailure(w, err) {
		return
	}
	statuses, err := s.control.LoadRotationStatuses(r.Context())
	if writeControlFailure(w, err) {
		return
	}
	overrides, err := s.control.LoadOverrides(r.Context())
	if writeControlFailure(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":   s.version,
		"devshards": devshardViews(records),
		"rotation":  rotationViews(statuses),
		"overrides": overrides,
		"status":    s.status(s.routableEscrows()),
	})
}

func (s *Server) handleAdminSettings(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPut, http.MethodPost) {
		return
	}
	if r.Method == http.MethodGet {
		overrides, err := s.control.LoadOverrides(r.Context())
		if writeControlFailure(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, overrides)
		return
	}
	body, err := readBody(w, r, adminIngestLimit)
	if err != nil {
		writeErrorFor(w, err)
		return
	}
	overrides, err := config.ParseOverrides(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.operations.Reconfigure(r.Context(), overrides); err != nil {
		writeErrorFor(w, err)
		return
	}
	auditAdmin("settings replaced")
	writeJSON(w, http.StatusOK, overrides)
}

// auditAdmin records an operator action that changed state the gateway serves or settles from. It
// carries the action and its subject, never the body: an override payload can hold the admin key.
func auditAdmin(action string, fields ...any) {
	logging.Info("admin: "+action, fields...)
}

func (s *Server) handleAdminEscrows(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	var request CreateEscrowRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	if strings.TrimSpace(request.Model) == "" || request.Amount == 0 {
		writeError(w, http.StatusBadRequest, "model and a non-zero amount are required")
		return
	}
	if strings.TrimSpace(request.PrivateKeyEnv) == "" {
		writeError(w, http.StatusBadRequest, ErrPrivateKeyEnvRequired.Error())
		return
	}
	result, err := s.operations.CreateEscrow(r.Context(), request)
	if err != nil {
		writeErrorFor(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type suspiciousHostRequest struct {
	ParticipantKey string `json:"participant_key"`
}

func (s *Server) handleAdminSuspiciousHosts(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost, http.MethodDelete) {
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"hosts": s.suspicious.List()})
		return
	}
	var request suspiciousHostRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	participantKey := strings.TrimSpace(request.ParticipantKey)
	if participantKey == "" {
		writeError(w, http.StatusBadRequest, "participant_key is required")
		return
	}
	apply, action := s.suspicious.Add, "participant added to the never-trust list"
	if r.Method == http.MethodDelete {
		apply, action = s.suspicious.Remove, "participant removed from the never-trust list"
	}
	if writeControlFailure(w, apply(r.Context(), participantKey)) {
		return
	}
	auditAdmin(action, "participant", participantKey)
	writeJSON(w, http.StatusOK, map[string]any{"hosts": s.suspicious.List()})
}

func (s *Server) handleAdminUnquarantine(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	var request suspiciousHostRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	participantKey := strings.TrimSpace(request.ParticipantKey)
	if participantKey == "" {
		writeError(w, http.StatusBadRequest, "participant_key is required")
		return
	}
	if err := s.operations.Unquarantine(r.Context(), participantKey); err != nil {
		writeErrorFor(w, err)
		return
	}
	auditAdmin("participant cutoff cleared", "participant", participantKey)
	writeJSON(w, http.StatusOK, map[string]any{"participant_key": participantKey})
}

func (s *Server) handleAdminResetAccountingEpoch(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	epoch, err := strconv.ParseUint(r.PathValue("epoch"), 10, 64)
	if err != nil || epoch == 0 {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid epoch %q", r.PathValue("epoch")))
		return
	}
	cleared, err := s.operations.ResetAccountingEpoch(r.Context(), epoch)
	if err != nil {
		writeErrorFor(w, err)
		return
	}
	auditAdmin("accounting epoch reset", "epoch", epoch, "escrows", cleared)
	writeJSON(w, http.StatusOK, map[string]any{"reset": true, "epoch": epoch, "escrows": cleared})
}

func (s *Server) handleDebugRotation(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet) {
		return
	}
	statuses, err := s.control.LoadRotationStatuses(r.Context())
	if writeControlFailure(w, err) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rotation": statuses})
}

func (s *Server) handleDebugMemstats(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	writeJSON(w, http.StatusOK, map[string]any{
		"alloc":           memory.Alloc,
		"total_alloc":     memory.TotalAlloc,
		"sys":             memory.Sys,
		"heap_alloc":      memory.HeapAlloc,
		"heap_sys":        memory.HeapSys,
		"heap_idle":       memory.HeapIdle,
		"heap_inuse":      memory.HeapInuse,
		"heap_released":   memory.HeapReleased,
		"heap_objects":    memory.HeapObjects,
		"stack_inuse":     memory.StackInuse,
		"next_gc":         memory.NextGC,
		"num_gc":          memory.NumGC,
		"loaded_escrows":  len(s.routableEscrows()),
		"num_goroutine":   runtime.NumGoroutine(),
		"gc_cpu_fraction": memory.GCCPUFraction,
	})
}

func normalizeStorageDir(storagePath string) string {
	storagePath = strings.TrimSpace(storagePath)
	if storagePath == "" {
		return ""
	}
	clean := filepath.Clean(storagePath)
	if filepath.Base(clean) == "state.db" {
		return filepath.Dir(clean)
	}
	return clean
}
