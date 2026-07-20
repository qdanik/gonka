package filters

import (
	"errors"
	"fmt"
	"net/http"
)

// RejectError is a rejection carrying an HTTP status plus an optional wrapped error.
type RejectError struct {
	Status  int
	Message string
	Wrapped error
}

// Error returns Message.
func (e *RejectError) Error() string { return e.Message }

// Unwrap exposes Wrapped for errors.Is/errors.As.
func (e *RejectError) Unwrap() error { return e.Wrapped }

// Reject builds a 400 RejectError from a formatted message.
func Reject(format string, args ...any) error {
	return &RejectError{Status: http.StatusBadRequest, Message: fmt.Sprintf(format, args...)}
}

// RejectWithStatus builds a RejectError with an explicit HTTP status.
func RejectWithStatus(status int, format string, args ...any) error {
	return &RejectError{Status: status, Message: fmt.Sprintf(format, args...)}
}

// WrapReject attaches a 400 status to err while keeping it in the chain. Returns nil for a nil err.
func WrapReject(err error) error {
	if err == nil {
		return nil
	}
	return &RejectError{Status: http.StatusBadRequest, Message: err.Error(), Wrapped: err}
}

// ErrorStatus returns err's RejectError status, or fallback.
func ErrorStatus(err error, fallback int) int {
	var rejectErr *RejectError
	if !errors.As(err, &rejectErr) {
		return fallback
	}
	return rejectErr.Status
}

// unsupportedParameterMessage returns the exact rejection text for an unknown/unsupported parameter.
func unsupportedParameterMessage(name string) string {
	return fmt.Sprintf("Chat completions parameter %q is currently rejected by the Gonka network. Some non-standard parameters can crash the vLLM engine on Gonka Host MLNodes, so the network rejects parameters that are not explicitly supported (see: https://github.com/gonka-ai/gonka/blob/main/docs/chat-api/README.md). If you do not need this parameter, remove it from the request; if you need it, file a request at https://github.com/gonka-ai/gonka/issues", name)
}
