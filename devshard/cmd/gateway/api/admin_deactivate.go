package api

import (
	"cmp"
	"fmt"
	"net/http"

	"golang.org/x/sync/errgroup"
)

type deactivateOutcome struct {
	EscrowID string `json:"escrow_id"`
	Status   int    `json:"status,omitempty"`
	Error    string `json:"error,omitempty"`
}

type deactivateBatchSummary struct {
	Deactivated int `json:"deactivated"`
	Failed      int `json:"failed"`
}

// handleAdminDevshardsDeactivateBatch answers the way the settle batch does. See docs/operations.md, "Deactivating a list of escrows".
func (s *Server) handleAdminDevshardsDeactivateBatch(w http.ResponseWriter, r *http.Request) {
	if !allowMethods(w, r, http.MethodPost) {
		return
	}
	var request DeactivateDevshardsRequest
	if err := decodeAdminBody(w, r, &request); err != nil {
		s.writeErrorFor(w, badRequestUnlessOversized(err))
		return
	}
	escrowIDs := distinctEscrowIDs(request.EscrowIDs)
	if refusal := refuseBatch(escrowIDs, request.BatchSize); refusal != "" {
		writeError(w, http.StatusBadRequest, refusal)
		return
	}
	registered, err := s.registeredDevshards(r)
	if writeControlFailure(w, err) {
		return
	}

	stream := newNDJSONStream(w)
	outcomes := make(chan deactivateOutcome, len(escrowIDs))
	go func() {
		defer close(outcomes)
		var deactivating errgroup.Group
		deactivating.SetLimit(cmp.Or(request.BatchSize, defaultSettleBatchSize))
		for _, escrowID := range escrowIDs {
			deactivating.Go(func() error {
				outcomes <- s.deactivateEntry(r, escrowID, registered[escrowID])
				return nil
			})
		}
		_ = deactivating.Wait()
	}()

	var summary deactivateBatchSummary
	for outcome := range outcomes {
		if outcome.Error == "" {
			summary.Deactivated++
		} else {
			summary.Failed++
		}
		stream.send(outcome)
	}
	stream.send(summary)
}

func (s *Server) deactivateEntry(r *http.Request, escrowID string, registered bool) deactivateOutcome {
	err := fmt.Errorf("%w: %s", ErrUnknownDevshard, escrowID)
	if registered {
		err = s.operations.Deactivate(r.Context(), escrowID)
	}
	if err != nil {
		return deactivateOutcome{EscrowID: escrowID, Status: statusForError(err), Error: err.Error()}
	}
	auditAdmin("escrow deactivated", "escrow", escrowID)
	return deactivateOutcome{EscrowID: escrowID}
}
