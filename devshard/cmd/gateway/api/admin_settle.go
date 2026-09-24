package api

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"strings"

	json "github.com/goccy/go-json"
	"golang.org/x/sync/errgroup"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/escrow"
)

const (
	defaultSettleBatchSize = 4
	maxSettleBatchSize     = 16
	settleBatchLimit       = 50
)

type settleOutcome struct {
	EscrowID string `json:"escrow_id"`
	TxHash   string `json:"tx_hash,omitempty"`
	Settler  string `json:"settler,omitempty"`
	Status   int    `json:"status,omitempty"`
	Error    string `json:"error,omitempty"`
}

type settleBatchSummary struct {
	Settled int `json:"settled"`
	Failed  int `json:"failed"`
}

type ndjsonStream struct {
	writer     http.ResponseWriter
	controller *http.ResponseController
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
		s.writeErrorFor(w, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
		return
	}
	result, err := s.settleOne(r.Context(), escrowID, isForcedSettle(r))
	if err != nil {
		s.writeErrorFor(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func isForcedSettle(r *http.Request) bool { return r.URL.Query().Get("force") == "true" }

func (s *Server) settleOne(ctx context.Context, escrowID string, forced bool) (chain.SettleEscrowResult, error) {
	var overridden int64
	switch {
	case forced:
		overridden = s.countInFlight(escrowID)
	case s.escrows.IsBusy(escrowID):
		return chain.SettleEscrowResult{}, fmt.Errorf("%w: %s", escrow.ErrDevshardBusy, escrowID)
	}
	result, err := s.operations.Settle(ctx, escrowID, forced)
	if err != nil {
		return result, err
	}
	if forced {
		auditAdmin("escrow settled under force", "escrow", escrowID, "in_flight", overridden)
	}
	return result, nil
}

func (s *Server) countInFlight(escrowID string) int64 {
	for _, escrowState := range s.escrows.Snapshot() {
		if escrowState.ID == escrowID {
			return escrowState.InFlight
		}
	}
	return 0
}

func (s *Server) handleAdminDevshardsSettleBatch(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	var request SettleDevshardsRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		s.writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	escrowIDs := distinctEscrowIDs(request.EscrowIDs)
	switch {
	case len(escrowIDs) == 0:
		writeError(w, http.StatusBadRequest, "escrow_ids is required")
		return
	case len(escrowIDs) > settleBatchLimit:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("escrow_ids holds %d escrows; at most %d settle in one call", len(escrowIDs), settleBatchLimit))
		return
	case request.BatchSize < 0 || request.BatchSize > maxSettleBatchSize:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("batch_size must be at most %d, or omitted for %d", maxSettleBatchSize, defaultSettleBatchSize))
		return
	}
	registered, err := s.registeredDevshards(r)
	if writeControlFailure(w, err) {
		return
	}
	forced := request.Force || isForcedSettle(r)
	batchSize := cmp.Or(request.BatchSize, defaultSettleBatchSize)

	stream := newNDJSONStream(w)
	outcomes := make(chan settleOutcome, len(escrowIDs))
	go func() {
		defer close(outcomes)
		var settling errgroup.Group
		settling.SetLimit(batchSize)
		for _, escrowID := range escrowIDs {
			settling.Go(func() error {
				outcomes <- s.settleEntry(r.Context(), escrowID, registered[escrowID], forced)
				return nil
			})
		}
		_ = settling.Wait()
	}()

	var summary settleBatchSummary
	for outcome := range outcomes {
		if outcome.Error == "" {
			summary.Settled++
		} else {
			summary.Failed++
		}
		stream.send(outcome)
	}
	stream.send(summary)
}

func newNDJSONStream(w http.ResponseWriter) *ndjsonStream {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.WriteHeader(http.StatusOK)
	stream := &ndjsonStream{writer: w, controller: http.NewResponseController(w)}
	_ = stream.controller.Flush()
	return stream
}

func (n *ndjsonStream) send(payload any) {
	line, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = n.writer.Write(append(line, '\n'))
	_ = n.controller.Flush()
}

func (s *Server) settleEntry(ctx context.Context, escrowID string, registered, forced bool) settleOutcome {
	if !registered {
		return settleRefused(escrowID, fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID))
	}
	result, err := s.settleOne(ctx, escrowID, forced)
	if err != nil {
		return settleRefused(escrowID, err)
	}
	return settleOutcome{EscrowID: escrowID, TxHash: result.TxHash, Settler: result.Settler}
}

func settleRefused(escrowID string, err error) settleOutcome {
	return settleOutcome{EscrowID: escrowID, Status: statusForError(err), Error: err.Error()}
}

func distinctEscrowIDs(escrowIDs []string) []string {
	seen := make(map[string]bool, len(escrowIDs))
	distinct := make([]string, 0, len(escrowIDs))
	for _, escrowID := range escrowIDs {
		escrowID = strings.TrimSpace(escrowID)
		if escrowID == "" || seen[escrowID] {
			continue
		}
		seen[escrowID] = true
		distinct = append(distinct, escrowID)
	}
	return distinct
}

func (s *Server) registeredDevshards(r *http.Request) (map[string]bool, error) {
	records, err := s.control.ListDevshards(r.Context())
	if err != nil {
		return nil, err
	}
	registered := make(map[string]bool, len(records))
	for _, record := range records {
		registered[record.EscrowID] = true
	}
	return registered, nil
}
