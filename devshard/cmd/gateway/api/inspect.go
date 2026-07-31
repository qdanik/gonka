package api

import (
	"fmt"
	"maps"
	"net/http"
	"slices"

	"devshard/cmd/gateway/registry"
	"devshard/types"
)

// escrowInspection is one escrow's state as an operator reads it while recovering a stuck devshard.
type escrowInspection struct {
	EscrowID      string   `json:"escrow_id"`
	Phase         string   `json:"phase"`
	Nonce         uint64   `json:"nonce"`
	LatestNonce   uint64   `json:"latest_nonce"`
	FinalizeNonce uint64   `json:"finalize_nonce,omitempty"`
	Balance       uint64   `json:"balance"`
	Fees          uint64   `json:"fees"`
	Participants  []string `json:"participants"`
	Slots         []string `json:"slots"`
	Inferences    int      `json:"inferences"`
	Pending       int      `json:"pending"`
}

type escrowInspectionDetail struct {
	escrowInspection
	Config    types.SessionConfig    `json:"config"`
	Group     []types.SlotAssignment `json:"group"`
	HostStats []hostStatsEntry       `json:"host_stats"`
}

type hostStatsEntry struct {
	Slot uint32 `json:"slot"`
	types.HostStats
}

type inferenceEntry struct {
	Nonce  uint64                `json:"nonce"`
	Status string                `json:"status"`
	Record types.InferenceRecord `json:"record"`
}

type signatureEntry struct {
	Nonce uint64   `json:"nonce"`
	Slots []uint32 `json:"slots"`
}

func (s *Server) handleDevshardState(w http.ResponseWriter, r *http.Request) {
	session, escrowID, held := s.inspectable(w, r)
	if !held {
		return
	}
	writeJSON(w, http.StatusOK, inspect(escrowID, session))
}

func (s *Server) handleDevshardDebugState(w http.ResponseWriter, r *http.Request) {
	session, escrowID, held := s.inspectable(w, r)
	if !held {
		return
	}
	state := session.SnapshotState()
	detail := escrowInspectionDetail{
		escrowInspection: inspect(escrowID, session),
		Config:           state.Config,
		Group:            state.Group,
		HostStats:        make([]hostStatsEntry, 0, len(state.HostStats)),
	}
	for _, slot := range slices.Sorted(maps.Keys(state.HostStats)) {
		stats := state.HostStats[slot]
		if stats == nil {
			continue
		}
		detail.HostStats = append(detail.HostStats, hostStatsEntry{Slot: slot, HostStats: *stats})
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleDevshardDebugInferences(w http.ResponseWriter, r *http.Request) {
	s.writeInferences(w, r, func(types.InferenceStatus) bool { return true })
}

func (s *Server) handleDevshardDebugPending(w http.ResponseWriter, r *http.Request) {
	s.writeInferences(w, r, unresolved)
}

func (s *Server) handleDevshardDebugSignatures(w http.ResponseWriter, r *http.Request) {
	session, escrowID, held := s.inspectable(w, r)
	if !held {
		return
	}
	signatures := session.Signatures()
	entries := make([]signatureEntry, 0, len(signatures))
	for _, nonce := range slices.Sorted(maps.Keys(signatures)) {
		entries = append(entries, signatureEntry{Nonce: nonce, Slots: slices.Sorted(maps.Keys(signatures[nonce]))})
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID, "signatures": entries})
}

func (s *Server) writeInferences(w http.ResponseWriter, r *http.Request, keep func(types.InferenceStatus) bool) {
	session, escrowID, held := s.inspectable(w, r)
	if !held {
		return
	}
	records := session.SnapshotState().Inferences
	entries := make([]inferenceEntry, 0, len(records))
	for _, nonce := range slices.Sorted(maps.Keys(records)) {
		record := records[nonce]
		if record == nil || !keep(record.Status) {
			continue
		}
		entries = append(entries, inferenceEntry{Nonce: nonce, Status: statusName(record.Status), Record: *record})
	}
	writeJSON(w, http.StatusOK, map[string]any{"escrow_id": escrowID, "inferences": entries})
}

// inspectable resolves a settling handle, so an escrow already retired for routing is still readable:
// that is the one an operator most needs to look at.
func (s *Server) inspectable(w http.ResponseWriter, r *http.Request) (registry.EscrowSession, string, bool) {
	if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
		return nil, "", false
	}
	escrowID := r.PathValue("id")
	session, held := s.escrows.SettlementSession(escrowID)
	if !held {
		writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return nil, "", false
	}
	return session, escrowID, true
}

func inspect(escrowID string, session registry.EscrowSession) escrowInspection {
	state := session.SnapshotState()
	pending := 0
	for _, record := range state.Inferences {
		if record != nil && unresolved(record.Status) {
			pending++
		}
	}
	return escrowInspection{
		EscrowID:      escrowID,
		Phase:         phaseName(state.Phase),
		Nonce:         session.Nonce(),
		LatestNonce:   state.LatestNonce,
		FinalizeNonce: state.FinalizeNonce,
		Balance:       state.Balance,
		Fees:          state.Fees,
		Participants:  session.ParticipantKeys(),
		Slots:         session.HostParticipantKeyList(),
		Inferences:    len(state.Inferences),
		Pending:       pending,
	}
}

// unresolved reports an inference that has not produced a result yet: the ones holding a stuck escrow
// short of settlement.
func unresolved(status types.InferenceStatus) bool {
	return status == types.StatusPending || status == types.StatusStarted || status == types.StatusChallenged
}

func statusName(status types.InferenceStatus) string {
	switch status {
	case types.StatusPending:
		return "pending"
	case types.StatusStarted:
		return "started"
	case types.StatusFinished:
		return "finished"
	case types.StatusChallenged:
		return "challenged"
	case types.StatusValidated:
		return "validated"
	case types.StatusInvalidated:
		return "invalidated"
	case types.StatusTimedOut:
		return "timed_out"
	}
	return "unknown"
}
