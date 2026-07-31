package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
)

// UnsupportedModelError names a model no live escrow serves, listing the ones that are routable so a
// client can correct the request without a second round trip.
type UnsupportedModelError struct {
	Model     string
	Supported []string
}

func (e *UnsupportedModelError) Error() string {
	return fmt.Sprintf("unsupported model %q; supported models: %s", e.Model, strings.Join(e.Supported, ", "))
}

// ModelUnavailableError reports that no escrow is routable at all, which is a gateway that is not
// ready rather than a request that is wrong.
type ModelUnavailableError struct{ Model string }

func (e *ModelUnavailableError) Error() string {
	return fmt.Sprintf("model %q is temporarily unavailable: no devshard is currently routable", e.Model)
}

// AccessDeniedError reports a model whose access tier the presented credential does not satisfy.
type AccessDeniedError struct {
	Model   string
	Message string
}

func (e *AccessDeniedError) Error() string { return e.Message }

// BlockedError reports the chain phase that rejects a request before it is queued.
type BlockedError struct {
	Reason     chain.BlockReason
	Phase      chain.EpochPhase
	Confirming chain.ConfirmationPoCPhase
}

func (e *BlockedError) Error() string {
	return "devshard temporarily unavailable during " + e.phaseName()
}

func (e *BlockedError) phaseName() string {
	switch e.Reason {
	case chain.BlockReasonPoC:
		switch e.Phase {
		case chain.EpochPhasePoCGenerate:
			return "PoC generation"
		case chain.EpochPhasePoCGenerateWindDown:
			return "PoC generation wind down"
		case chain.EpochPhasePoCValidate:
			return "PoC validation"
		case chain.EpochPhasePoCValidateWindDown:
			return "PoC validation wind down"
		}
		return "PoC"
	case chain.BlockReasonConfirmationPoC:
		switch e.Confirming {
		case chain.ConfirmationPoCGracePeriod:
			return "confirmation PoC grace period"
		case chain.ConfirmationPoCGeneration:
			return "confirmation PoC generation"
		case chain.ConfirmationPoCValidation:
			return "confirmation PoC validation"
		}
		return "confirmation PoC"
	}
	if e.Reason != chain.BlockReasonNone {
		return strings.ReplaceAll(string(e.Reason), "_", " ")
	}
	if e.Phase != "" {
		return string(e.Phase)
	}
	return "chain admission controls"
}

var ErrUnknownDevshard = errors.New("unknown devshard")

// ErrUnknownParticipant reports a participant key no limiter state is tracked under.
var ErrUnknownParticipant = errors.New("unknown participant")

// ErrPrivateKeyEnvRequired refuses a create that names no key variable; the raw key a client could
// send instead would reach the commitment row, the logs, and every operator in between.
var ErrPrivateKeyEnvRequired = errors.New("private_key_env is required; a raw private_key is not accepted")

var ErrDevshardExists = errors.New("devshard already registered")

// statusForError maps every rejection the request path can produce onto its HTTP status. Cases the
// packages below already own are delegated to them; the ones this boundary owns are the model,
// admission and per-escrow-phase rejections nothing under it can express.
func statusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	if status := filters.ErrorStatus(err, 0); status != 0 {
		return status
	}
	var unsupported *UnsupportedModelError
	if errors.As(err, &unsupported) {
		return http.StatusBadRequest
	}
	var unavailable *ModelUnavailableError
	if errors.As(err, &unavailable) {
		return http.StatusServiceUnavailable
	}
	var denied *AccessDeniedError
	if errors.As(err, &denied) {
		return http.StatusUnauthorized
	}
	var blocked *BlockedError
	if errors.As(err, &blocked) {
		return http.StatusServiceUnavailable
	}
	var throttled *limits.RateLimitError
	if errors.As(err, &throttled) {
		return http.StatusTooManyRequests
	}
	switch {
	case errors.Is(err, scheduler.ErrNoEscrowCapacity), errors.Is(err, scheduler.ErrEscrowBusy):
		return http.StatusTooManyRequests
	case errors.Is(err, scheduler.ErrEscrowGone):
		return http.StatusConflict
	case errors.Is(err, engine.ErrStopped), errors.Is(err, registry.ErrClosed):
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrPrivateKeyEnvRequired):
		return http.StatusBadRequest
	case errors.Is(err, ErrUnknownDevshard), errors.Is(err, ErrUnknownParticipant):
		return http.StatusNotFound
	case errors.Is(err, escrow.ErrDevshardBusy), errors.Is(err, ErrDevshardExists), errors.Is(err, registry.ErrDraining):
		return http.StatusConflict
	}
	return engine.StatusForError(err)
}

type errorEnvelope struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type,omitempty"`
	Code    string `json:"code,omitempty"`
}

// writeError is the package's only error writer, so no rejection can reach a client as the
// text/plain body http.Error would give it.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorEnvelope{Error: errorDetail{Message: message}})
}

func writeErrorFor(w http.ResponseWriter, err error) {
	writeError(w, statusForError(err), err.Error())
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	body, err := json.Marshal(payload)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(body, '\n'))
}
