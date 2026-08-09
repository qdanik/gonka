package accounting

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// A participant is addressable by its own address, which is what lets a host ask what this gateway
// saw of it. Every route is read-only, so the listener carries no authentication. See
// gateway-operations.md, "Nonce accounting".
type Handler struct {
	book         *Book
	currentEpoch CurrentEpochFunc
}

func NewHandler(book *Book, currentEpoch CurrentEpochFunc) http.Handler {
	handler := &Handler{book: book, currentEpoch: currentEpoch}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/escrows", handler.serveEscrows)
	mux.HandleFunc("GET /api/v1/epochs", handler.serveEpochs)
	mux.HandleFunc("GET /api/v1/epochs/{epoch}/participants", handler.serveParticipants)
	mux.HandleFunc("GET /api/v1/epochs/{epoch}/participants/{participant}", handler.serveParticipant)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if handler.book == nil {
			writeError(w, http.StatusServiceUnavailable, "accounting is unavailable")
			return
		}
		if trimmed := strings.TrimSuffix(r.URL.Path, "/"); trimmed != "" {
			r.URL.Path = trimmed
		}
		mux.ServeHTTP(w, r)
	})
}

func (h *Handler) serveEscrows(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, struct {
		SchemaVersion int      `json:"schema_version"`
		Escrows       []string `json:"escrows"`
	}{SchemaVersion: SchemaVersion, Escrows: h.book.EscrowIDs()})
}

func (h *Handler) serveEpochs(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, struct {
		SchemaVersion int            `json:"schema_version"`
		Epochs        []EpochSummary `json:"epochs"`
	}{SchemaVersion: SchemaVersion, Epochs: h.book.Epochs(filterOf(r, 0))})
}

func (h *Handler) serveParticipants(w http.ResponseWriter, r *http.Request) {
	epoch, err := h.epochOf(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, struct {
		SchemaVersion int                 `json:"schema_version"`
		EpochIndex    uint64              `json:"epoch_index"`
		Participants  []ParticipantRecord `json:"participants"`
	}{SchemaVersion: SchemaVersion, EpochIndex: epoch, Participants: h.book.Query(filterOf(r, epoch))})
}

func (h *Handler) serveParticipant(w http.ResponseWriter, r *http.Request) {
	epoch, err := h.epochOf(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	participant := strings.TrimSpace(r.PathValue("participant"))
	if participant == "" {
		writeError(w, http.StatusBadRequest, "invalid participant")
		return
	}
	filter := filterOf(r, epoch)
	filter.Participant = participant
	records := h.book.Query(filter)
	if len(records) == 0 {
		writeError(w, http.StatusNotFound, "participant not found")
		return
	}
	writeJSON(w, http.StatusOK, struct {
		SchemaVersion int                 `json:"schema_version"`
		EpochIndex    uint64              `json:"epoch_index"`
		Participant   string              `json:"participant"`
		Records       []ParticipantRecord `json:"records"`
	}{SchemaVersion: SchemaVersion, EpochIndex: epoch, Participant: participant, Records: records})
}

// Epoch zero is refused: the filter reads it as "every epoch", so serving it would answer a question
// about one epoch with all of them.
func (h *Handler) epochOf(r *http.Request) (uint64, error) {
	value := r.PathValue("epoch")
	if value == "current" {
		if h.currentEpoch == nil {
			return 0, errors.New("current epoch is unavailable")
		}
		return h.currentEpoch(r.Context())
	}
	epoch, err := strconv.ParseUint(value, 10, 64)
	if err != nil || epoch == 0 {
		return 0, fmt.Errorf("invalid epoch %q", value)
	}
	return epoch, nil
}

func filterOf(r *http.Request, epoch uint64) QueryFilter {
	return QueryFilter{
		EpochIndex: epoch,
		Model:      strings.TrimSpace(r.URL.Query().Get("model")),
		EscrowIDs:  escrowIDsOf(r),
	}
}

func escrowIDsOf(r *http.Request) []string {
	values := r.URL.Query()["escrow_id"]
	escrowIDs := make([]string, 0, len(values))
	for _, value := range values {
		for _, escrowID := range strings.Split(value, ",") {
			if escrowID = strings.TrimSpace(escrowID); escrowID != "" {
				escrowIDs = append(escrowIDs, escrowID)
			}
		}
	}
	return escrowIDs
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}
