package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/store"
	"devshard/types"
)

func (s *Server) handleAdminState(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet) {
		return
	}
	records, err := s.control.ListDevshards(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	statuses, err := s.control.LoadRotationStatuses(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	overrides, err := s.control.LoadOverrides(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":   s.version,
		"devshards": records,
		"rotation":  statuses,
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
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
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
	writeJSON(w, http.StatusOK, overrides)
}

func (s *Server) handleAdminDevshards(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	if r.Method == http.MethodGet {
		records, err := s.control.ListDevshards(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"devshards": records})
		return
	}
	var request AddDevshardRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	if strings.TrimSpace(request.EscrowID) == "" || strings.TrimSpace(request.Model) == "" {
		writeError(w, http.StatusBadRequest, "escrow_id and model are required")
		return
	}
	if strings.TrimSpace(request.PrivateKeyEnv) == "" {
		writeErrorFor(w, ErrPrivateKeyEnvRequired)
		return
	}
	if err := s.operations.AddDevshard(r.Context(), request); err != nil {
		writeErrorFor(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": request.EscrowID})
}

func (s *Server) handleAdminDevshardImport(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	var request ImportDevshardRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	if strings.TrimSpace(request.EscrowID) == "" || strings.TrimSpace(request.SourcePath) == "" {
		writeError(w, http.StatusBadRequest, "escrow_id and source_path are required")
		return
	}
	if strings.TrimSpace(request.PrivateKeyEnv) == "" {
		writeErrorFor(w, ErrPrivateKeyEnvRequired)
		return
	}
	if err := s.operations.ImportDevshard(r.Context(), request); err != nil {
		writeErrorFor(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": request.EscrowID})
}

func (s *Server) handleAdminDevshardDelete(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodDelete) {
		return
	}
	escrowID := r.PathValue("id")
	record, found, err := s.devshardRecord(r, escrowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	if record.Active || s.escrows.IsBusy(escrowID) {
		writeErrorFor(w, fmt.Errorf("%w: %s", escrow.ErrDevshardBusy, escrowID))
		return
	}
	if err := s.control.DeleteDevshard(r.Context(), escrowID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := removeDevshardStorage(DevshardStoragePath(s.storageDir, escrowID), s.storageDir); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID, "deleted": true})
}

func (s *Server) handleAdminDevshardActivate(w http.ResponseWriter, r *http.Request) {
	s.lifecycle(w, r, s.operations.Activate)
}

func (s *Server) handleAdminDevshardDeactivate(w http.ResponseWriter, r *http.Request) {
	s.lifecycle(w, r, s.operations.Deactivate)
}

func (s *Server) lifecycle(w http.ResponseWriter, r *http.Request, apply func(ctx context.Context, escrowID string) error) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	escrowID := r.PathValue("id")
	_, found, err := s.devshardRecord(r, escrowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	if err := apply(r.Context(), escrowID); err != nil {
		writeErrorFor(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID})
}

func (s *Server) handleAdminDevshardSettle(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	escrowID := r.PathValue("id")
	_, found, err := s.devshardRecord(r, escrowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	if s.escrows.IsBusy(escrowID) {
		writeErrorFor(w, fmt.Errorf("%w: %s", escrow.ErrDevshardBusy, escrowID))
		return
	}
	result, err := s.operations.Settle(r.Context(), escrowID)
	if err != nil {
		writeErrorFor(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleAdminDevshardParticipants(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet) {
		return
	}
	escrowID := r.PathValue("id")
	session, held := s.escrows.SettlementSession(escrowID)
	if !held {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"escrow_id":    escrowID,
		"participants": session.ParticipantKeys(),
		"slots":        session.HostParticipantKeyList(),
	})
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
	apply := s.suspicious.Add
	if r.Method == http.MethodDelete {
		apply = s.suspicious.Remove
	}
	if err := apply(r.Context(), participantKey); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
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
	writeJSON(w, http.StatusOK, map[string]any{"participant_key": participantKey})
}

func (s *Server) handleDebugRotation(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet) {
		return
	}
	statuses, err := s.control.LoadRotationStatuses(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
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

func (s *Server) handleDevshardFinalize(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	escrowID := r.PathValue("id")
	session, held := s.escrows.SettlementSession(escrowID)
	if !held {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	state := session.SnapshotState()
	if r.Method == http.MethodGet {
		if state.Phase == types.PhaseActive {
			writeError(w, http.StatusConflict, "devshard is not finalized")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"escrow_id":      escrowID,
			"phase":          phaseName(state.Phase),
			"finalize_nonce": state.FinalizeNonce,
		})
		return
	}
	if s.escrows.IsBusy(escrowID) {
		writeErrorFor(w, fmt.Errorf("%w: %s", escrow.ErrDevshardBusy, escrowID))
		return
	}
	if err := session.Finalize(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID, "finalized": true})
}

func (s *Server) devshardRecord(r *http.Request, escrowID string) (store.DevshardRecord, bool, error) {
	records, err := s.control.ListDevshards(r.Context())
	if err != nil {
		return store.DevshardRecord{}, false, err
	}
	for _, record := range records {
		if record.EscrowID == escrowID {
			return record, true, nil
		}
	}
	return store.DevshardRecord{}, false, nil
}

// badRequestUnlessOversized keeps an ingest-cap rejection at 413 while every other decode failure is
// the client's malformed body.
func badRequestUnlessOversized(err error) error {
	var oversized *http.MaxBytesError
	if errors.As(err, &oversized) {
		return err
	}
	return filters.WrapReject(err)
}

// DevshardStoragePath is where one escrow's session keeps its SQLite files, and sessions and the
// delete route must derive it the same way. See gateway-operations.md, "Operator".
func DevshardStoragePath(baseStorageDir, escrowID string) string {
	return filepath.Join(baseStorageDir, "escrow-"+escrowID)
}

// removeDevshardStorage guards os.RemoveAll on a path derived from a client-supplied escrow id. See
// gateway-operations.md, "Operator".
func removeDevshardStorage(storagePath, baseStorageDir string) error {
	if strings.TrimSpace(storagePath) == "" {
		return nil
	}
	storagePath = normalizeStorageDir(storagePath)
	baseStorageDir = filepath.Clean(baseStorageDir)
	if !strings.HasPrefix(storagePath, baseStorageDir+string(os.PathSeparator)) && storagePath != baseStorageDir {
		return fmt.Errorf("refusing to delete storage outside base dir: %s", storagePath)
	}
	if storagePath == baseStorageDir {
		return fmt.Errorf("refusing to delete base storage dir: %s", storagePath)
	}
	return os.RemoveAll(storagePath)
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
