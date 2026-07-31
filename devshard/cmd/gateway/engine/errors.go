package engine

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"devshard/transport"
)

var (
	ErrEmptyStream = errors.New("empty content stream")

	// ErrWinnerIncomplete: the client already saw this attempt's bytes, so no other attempt's
	// completion can be substituted for it.
	ErrWinnerIncomplete = errors.New("winner failed after streaming started")

	ErrNonStreamTimeout = errors.New("no content before the non-streaming deadline")
)

// HostApplicationError is an upstream refusal the client must see verbatim: the host answered, and
// its answer is the response.
type HostApplicationError struct {
	Code    string
	Type    string
	Message string
	Payload string
}

func (e *HostApplicationError) Error() string {
	if e == nil {
		return "host application error"
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Payload != "" {
		return e.Payload
	}
	return "host application error"
}

func (e *HostApplicationError) HTTPStatus() int {
	if e == nil {
		return http.StatusBadGateway
	}
	if code, err := strconv.Atoi(e.Code); err == nil && isClientOrServerStatus(code) {
		return code
	}
	if strings.Contains(strings.ToLower(e.Type), "badrequest") {
		return http.StatusBadRequest
	}
	return http.StatusBadGateway
}

// UpstreamStatus reports the HTTP status a transport error carries, which is the only place the
// engine can learn that a host throttled or rejected the request.
func UpstreamStatus(err error) (int, bool) {
	var upstream *transport.UpstreamStatusError
	if errors.As(err, &upstream) && upstream != nil {
		return upstream.StatusCode, true
	}
	return 0, false
}

func StatusForError(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var hostErr *HostApplicationError
	if errors.As(err, &hostErr) {
		return hostErr.HTTPStatus()
	}
	if errors.Is(err, ErrNonStreamTimeout) {
		return http.StatusGatewayTimeout
	}
	if status, ok := UpstreamStatus(err); ok && isThrottleStatus(status) {
		return http.StatusTooManyRequests
	}
	return http.StatusBadGateway
}

func isThrottleStatus(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable
}

func isClientOrServerStatus(status int) bool {
	return status >= 400 && status <= 599
}
