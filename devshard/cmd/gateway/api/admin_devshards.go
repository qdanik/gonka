package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/store"
	"devshard/types"
)

func (s *Server) handleAdminDevshards(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodGet, http.MethodPost) {
		return
	}
	if r.Method == http.MethodGet {
		records, err := s.control.ListDevshards(r.Context())
		if writeControlFailure(w, err) {
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
	auditAdmin("escrow registered", "escrow", request.EscrowID, "model", request.Model)
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
	auditAdmin("escrow imported", "escrow", request.EscrowID, "model", request.Model)
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": request.EscrowID})
}

func (s *Server) handleAdminDevshardDelete(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodDelete) {
		return
	}
	escrowID := r.PathValue("id")
	record, found, err := s.devshardRecord(r, escrowID)
	if writeControlFailure(w, err) {
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
	if writeControlFailure(w, s.control.DeleteDevshard(r.Context(), escrowID)) {
		return
	}
	if writeControlFailure(w, removeDevshardStorage(DevshardStoragePath(s.storageDir, escrowID), s.storageDir)) {
		return
	}
	auditAdmin("escrow deleted with its session storage", "escrow", escrowID, "model", record.Model)
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID, "deleted": true})
}

func (s *Server) handleAdminDevshardActivate(w http.ResponseWriter, r *http.Request) {
	s.lifecycle(w, r, "escrow activated", s.operations.Activate)
}

func (s *Server) handleAdminDevshardDeactivate(w http.ResponseWriter, r *http.Request) {
	s.lifecycle(w, r, "escrow deactivated", s.operations.Deactivate)
}

func (s *Server) lifecycle(w http.ResponseWriter, r *http.Request, action string, apply func(ctx context.Context, escrowID string) error) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	escrowID := r.PathValue("id")
	_, found, err := s.devshardRecord(r, escrowID)
	if writeControlFailure(w, err) {
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
	auditAdmin(action, "escrow", escrowID)
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID})
}

func (s *Server) handleAdminDevshardSettle(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	escrowID := r.PathValue("id")
	_, found, err := s.devshardRecord(r, escrowID)
	if writeControlFailure(w, err) {
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
	if writeControlFailure(w, session.Finalize(r.Context())) {
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
