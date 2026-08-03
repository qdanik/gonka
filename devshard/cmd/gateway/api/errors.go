package api

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/escrow"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"

	json "github.com/goccy/go-json"
)

// noHostRetryAfter is what a client is told to wait when every participant is at capacity: a window
// frees as soon as one in-flight request completes, so the hint is short rather than a backoff ladder.
const noHostRetryAfter = time.Second

var (
	ErrUnknownDevshard = errors.New("unknown devshard")

	// ErrUnknownParticipant reports a participant key no limiter state is tracked under.
	ErrUnknownParticipant = errors.New("unknown participant")

	// ErrPrivateKeyEnvRequired refuses a create that names no key variable. See gateway-operations.md,
	// "Operator".
	ErrPrivateKeyEnvRequired = errors.New("private_key_env is required; a raw private_key is not accepted")

	ErrDevshardExists = errors.New("devshard already registered")
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
	// Our own admission refused this, so it is not the 502 an upstream failure earns: no host was asked,
	// and capacity returns on its own.
	case errors.Is(err, scheduler.ErrHostsBusy):
		return http.StatusServiceUnavailable
	case errors.Is(err, scheduler.ErrEscrowGone):
		return http.StatusConflict
	case errors.Is(err, scheduler.ErrToolsUnsupported):
		return http.StatusBadRequest
	case errors.Is(err, engine.ErrStopped), errors.Is(err, registry.ErrClosed):
		return http.StatusServiceUnavailable
	case errors.Is(err, ErrPrivateKeyEnvRequired):
		return http.StatusBadRequest
	case errors.Is(err, ErrUnknownDevshard), errors.Is(err, ErrUnknownParticipant):
		return http.StatusNotFound
	case errors.Is(err, escrow.ErrDevshardBusy), errors.Is(err, escrow.ErrSettlementInFlight),
		errors.Is(err, ErrDevshardExists), errors.Is(err, registry.ErrDraining):
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

// writeError renders a rejection as the JSON envelope, never as the text/plain http.Error gives. See
// gateway-request-lifecycle.md, "What can end a request, and with what status".
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorEnvelope{Error: errorDetail{Message: message}})
}

// writeErrorFor is the one place every rejection is written, so Retry-After is set here rather than at
// each 429's own call site. The value is rounded up: a zero would tell a client to retry immediately,
// which is the opposite of what a queue timeout means.
func writeErrorFor(w http.ResponseWriter, err error) {
	var throttled *limits.RateLimitError
	switch {
	case errors.As(err, &throttled) && throttled.RetryAfter > 0:
		seconds := int64(math.Ceil(throttled.RetryAfter.Seconds()))
		w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
	case errors.Is(err, scheduler.ErrHostsBusy):
		w.Header().Set("Retry-After", strconv.FormatInt(int64(noHostRetryAfter.Seconds()), 10))
	}
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

// maxLoggedErrorBytes bounds host-controlled text reaching a log line: a HostApplicationError with no
// message renders its whole upstream payload. See gateway-operations.md, "The request record".
const maxLoggedErrorBytes = 256

// adminFailure logs an admin response the gateway refused or failed to serve. auditAdmin records only
// the successful path, so without this a failed operator action leaves no trace at all.
func adminFailure(label string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		recorder := &failureRecorder{ResponseWriter: w, status: http.StatusOK}
		next(recorder, r)
		if recorder.status < http.StatusBadRequest {
			return
		}
		fields := []any{"route", label, "method", r.Method, "status", recorder.status, "error", recorder.reason()}
		if recorder.status >= http.StatusInternalServerError {
			logging.Error("admin request failed", fields...)
			return
		}
		logging.Warn("admin request refused", fields...)
	}
}

// failureRecorder keeps the status and the start of an error body so the failure log can carry the
// message the caller was given. Flush is forwarded so a wrapped handler keeps streaming.
type failureRecorder struct {
	http.ResponseWriter
	status int
	body   []byte
}

func (rec *failureRecorder) WriteHeader(status int) {
	rec.status = status
	rec.ResponseWriter.WriteHeader(status)
}

func (rec *failureRecorder) Write(data []byte) (int, error) {
	if rec.status >= http.StatusBadRequest && len(rec.body) < maxLoggedErrorBytes {
		rec.body = append(rec.body, data[:min(len(data), maxLoggedErrorBytes-len(rec.body))]...)
	}
	return rec.ResponseWriter.Write(data)
}

func (rec *failureRecorder) Unwrap() http.ResponseWriter { return rec.ResponseWriter }

func (rec *failureRecorder) Flush() {
	if flusher, ok := rec.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// reason prefers the envelope message; a body truncated at the cap no longer parses, so the raw text
// is the fallback rather than an empty field.
func (rec *failureRecorder) reason() string {
	var envelope errorEnvelope
	if err := json.Unmarshal(rec.body, &envelope); err == nil && envelope.Error.Message != "" {
		return envelope.Error.Message
	}
	return strings.TrimSpace(string(rec.body))
}

func loggedError(err error) string {
	text := err.Error()
	if len(text) <= maxLoggedErrorBytes {
		return text
	}
	cut := maxLoggedErrorBytes
	for cut > 0 && !utf8.RuneStart(text[cut]) {
		cut--
	}
	return text[:cut] + "…(truncated)"
}
