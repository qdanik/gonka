package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"devshard/transport"
)

func TestSentinelsSurviveWrapping(t *testing.T) {
	testCases := []struct {
		name     string
		sentinel error
	}{
		{name: "empty_stream", sentinel: ErrEmptyStream},
		{name: "winner_incomplete", sentinel: ErrWinnerIncomplete},
		{name: "non_stream_timeout", sentinel: ErrNonStreamTimeout},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			wrapped := fmt.Errorf("attempt 7: %w", testCase.sentinel)

			if !errors.Is(wrapped, testCase.sentinel) {
				t.Fatalf("errors.Is(%v) = false, want true", wrapped)
			}
			for _, other := range testCases {
				if other.name == testCase.name {
					continue
				}
				if errors.Is(wrapped, other.sentinel) {
					t.Fatalf("errors.Is(%v, %v) = true, want each sentinel to be distinct", wrapped, other.sentinel)
				}
			}
		})
	}
}

func TestHostApplicationErrorIsRecoverableWhenWrapped(t *testing.T) {
	hostErr := &HostApplicationError{Code: "400", Message: "bad request"}
	wrapped := fmt.Errorf("race failed: %w", hostErr)

	var recovered *HostApplicationError
	if !errors.As(wrapped, &recovered) {
		t.Fatalf("errors.As = false, want the host error back")
	}
	if recovered != hostErr {
		t.Fatalf("recovered = %+v, want the original error", recovered)
	}
}

func TestHostApplicationErrorHTTPStatus(t *testing.T) {
	testCases := []struct {
		name       string
		hostErr    *HostApplicationError
		wantStatus int
	}{
		{name: "code_outside_the_error_range_is_ignored", hostErr: &HostApplicationError{Code: "200", Type: "BadRequestError"}, wantStatus: http.StatusBadRequest},
		{name: "numeric_code", hostErr: &HostApplicationError{Code: "429"}, wantStatus: http.StatusTooManyRequests},
		{name: "non_numeric_code_with_bad_request_type", hostErr: &HostApplicationError{Code: "invalid_request_error", Type: "BadRequestError"}, wantStatus: http.StatusBadRequest},
		{name: "unknown_shape", hostErr: &HostApplicationError{Message: "something broke"}, wantStatus: http.StatusBadGateway},
		{name: "nil", hostErr: nil, wantStatus: http.StatusBadGateway},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.hostErr.HTTPStatus(); got != testCase.wantStatus {
				t.Fatalf("status = %d, want %d", got, testCase.wantStatus)
			}
		})
	}
}

func TestHostApplicationErrorMessage(t *testing.T) {
	testCases := []struct {
		name        string
		hostErr     *HostApplicationError
		wantMessage string
	}{
		{name: "message_wins", hostErr: &HostApplicationError{Message: "context length exceeded", Payload: `{"error":{}}`}, wantMessage: "context length exceeded"},
		{name: "payload_when_no_message", hostErr: &HostApplicationError{Payload: `{"error":{}}`}, wantMessage: `{"error":{}}`},
		{name: "fallback", hostErr: &HostApplicationError{}, wantMessage: "host application error"},
		{name: "nil", hostErr: nil, wantMessage: "host application error"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.hostErr.Error(); got != testCase.wantMessage {
				t.Fatalf("message = %q, want %q", got, testCase.wantMessage)
			}
		})
	}
}

func TestHostApplicationErrorPayloadIsForwardedVerbatim(t *testing.T) {
	const upstream = `{"error":{"message":"upstream said so","type":"BadRequestError","code":400}}`
	hostErr := &HostApplicationError{Payload: upstream, Message: "upstream said so"}

	payload := hostErr.JSONPayload()

	if string(payload) != upstream {
		t.Fatalf("payload = %s, want it byte-for-byte %s", payload, upstream)
	}
	payload[0] = 'X'
	if hostErr.Payload[0] == 'X' {
		t.Fatalf("mutating the returned payload changed the error's own copy")
	}
}

func TestHostApplicationErrorSynthesisesAPayload(t *testing.T) {
	hostErr := &HostApplicationError{Code: "400", Type: "BadRequestError", Message: "tool choice unsupported"}

	var decoded struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(hostErr.JSONPayload(), &decoded); err != nil {
		t.Fatalf("payload does not parse as JSON: %v", err)
	}
	if decoded.Error.Message != "tool choice unsupported" || decoded.Error.Type != "BadRequestError" || decoded.Error.Code != "400" {
		t.Fatalf("payload = %+v, want the error's own fields", decoded.Error)
	}
}

func TestUpstreamStatusRecovery(t *testing.T) {
	wrapped := fmt.Errorf("send: %w", &transport.UpstreamStatusError{Path: "/v1/chat/completions", StatusCode: http.StatusTooManyRequests})

	status, ok := UpstreamStatus(wrapped)
	if !ok || status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, ok = %v, want 429, true", status, ok)
	}
	if _, ok := UpstreamStatus(ErrEmptyStream); ok {
		t.Fatalf("ok = true for an error carrying no upstream status, want false")
	}
}

func TestStatusForError(t *testing.T) {
	testCases := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "success", err: nil, wantStatus: http.StatusOK},
		{name: "host_application_error", err: &HostApplicationError{Code: "400"}, wantStatus: http.StatusBadRequest},
		{name: "wrapped_host_application_error", err: fmt.Errorf("race: %w", &HostApplicationError{Code: "404"}), wantStatus: http.StatusNotFound},
		{name: "non_stream_timeout", err: fmt.Errorf("race: %w", ErrNonStreamTimeout), wantStatus: http.StatusGatewayTimeout},
		{name: "upstream_throttled", err: &transport.UpstreamStatusError{StatusCode: http.StatusTooManyRequests}, wantStatus: http.StatusTooManyRequests},
		{name: "upstream_unavailable", err: &transport.UpstreamStatusError{StatusCode: http.StatusServiceUnavailable}, wantStatus: http.StatusTooManyRequests},
		{name: "upstream_server_error", err: &transport.UpstreamStatusError{StatusCode: http.StatusInternalServerError}, wantStatus: http.StatusBadGateway},
		{name: "empty_stream", err: ErrEmptyStream, wantStatus: http.StatusBadGateway},
		{name: "winner_incomplete", err: ErrWinnerIncomplete, wantStatus: http.StatusBadGateway},
		{name: "truncated_stream", err: transport.ErrSSEStreamTruncated, wantStatus: http.StatusBadGateway},
		{name: "unknown", err: errors.New("something else"), wantStatus: http.StatusBadGateway},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := StatusForError(testCase.err); got != testCase.wantStatus {
				t.Fatalf("status = %d, want %d", got, testCase.wantStatus)
			}
		})
	}
}

func TestHostApplicationErrorWinsOverAWrappedUpstreamStatus(t *testing.T) {
	err := fmt.Errorf("race: %w", &HostApplicationError{
		Code:    "400",
		Message: "context length exceeded",
	})

	if got := StatusForError(fmt.Errorf("%w: %w", err, &transport.UpstreamStatusError{StatusCode: http.StatusTooManyRequests})); got != http.StatusBadRequest {
		t.Fatalf("status = %d, want the host's own status 400", got)
	}
}
