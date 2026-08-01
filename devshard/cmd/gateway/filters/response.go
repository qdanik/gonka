package filters

import (
	"bytes"
	"fmt"
	json "github.com/goccy/go-json"
	"net/http"
	"strings"
)

var (
	// clientStrippedFields are response-body keys, at any nesting depth, hidden from the client.
	// Paired with parameterTable's force rules by TestForcedRequestParametersHaveResponseStripCounterpart.
	clientStrippedFields = []string{
		"logprob",
		"logprobs",
		"top_logprobs",
		"token_ids",
		"prompt_token_ids",
		"prompt_logprobs",
	}

	// strippableMarkers keeps only the fields no other field contains, unquoted, so two scans cover all
	// six: "top_logprobs" contains "logprob", so finding the short one cannot miss the long one. Still
	// derived from the list -- a hand-written marker set is exactly how top_logprobs once leaked, and
	// the quote that set relied on is what hid it, since the quote sits before "top_", not "logprob".
	strippableMarkers = func() [][]byte {
		markers := make([][]byte, 0, len(clientStrippedFields))
		for _, field := range clientStrippedFields {
			contained := false
			for _, other := range clientStrippedFields {
				if other != field && strings.Contains(field, other) {
					contained = true
					break
				}
			}
			if !contained {
				markers = append(markers, []byte(field))
			}
		}
		return markers
	}()

	// nonCacheableErrorMarkers identify transient, environmental, or model-availability failures
	// excluded from caching regardless of which of message/type/code carries them.
	nonCacheableErrorMarkers = []string{
		"context canceled",
		"context cancelled",
		"client disconnected",
		"request canceled",
		"request cancelled",
		"timeout",
		"timed out",
		"rate limit",
		"overloaded",
		"temporarily unavailable",
		"service unavailable",
		"internal server error",
		"unsupported model",
		"model not found",
		"model_not_found",
		"does not exist",
		"not supported on this model",
	}
)

// stripOutcome separates an unparseable payload from a parseable one with nothing to strip.
type stripOutcome int

const (
	stripMalformed stripOutcome = iota
	stripUnchanged
	stripRewritten
)

// StripResponseBody removes clientStrippedFields from a non-streaming JSON response body, at
// any nesting depth. A malformed body passes through unchanged.
func StripResponseBody(body []byte) []byte {
	filtered, outcome := stripInternalFields(body)
	if outcome != stripRewritten {
		return body
	}
	return filtered
}

// stripInternalFields decodes, deletes and re-encodes. UseNumber keeps an integer past 2^53 exactly --
// a client's seed among them -- which a plain decode into any would round. More() keeps a body with
// trailing junk malformed, which a Decoder alone would accept by reading only its first value.
func stripInternalFields(payload []byte) ([]byte, stripOutcome) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil || decoder.More() {
		return nil, stripMalformed
	}
	if !deleteInternalFields(decoded) {
		return nil, stripUnchanged
	}
	// DisableHTMLEscape keeps the host's bytes: the default turns every < > & in generated content into
	// a six-byte escape, which is the same string to a decoder and a much larger one on the wire.
	encoded, err := json.MarshalWithOption(decoded, json.DisableHTMLEscape())
	if err != nil {
		return nil, stripMalformed
	}
	return encoded, stripRewritten
}

// deleteInternalFields removes clientStrippedFields at any depth, reporting whether anything went.
func deleteInternalFields(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for _, field := range clientStrippedFields {
			if _, held := typed[field]; held {
				delete(typed, field)
				changed = true
			}
		}
		for _, child := range typed {
			changed = deleteInternalFields(child) || changed
		}
		return changed
	case []any:
		changed := false
		for _, child := range typed {
			changed = deleteInternalFields(child) || changed
		}
		return changed
	default:
		return false
	}
}

// hasStrippableField is a cheap pre-check so untouched chunks skip the SSE split entirely.
func hasStrippableField(p []byte) bool {
	for _, marker := range strippableMarkers {
		if bytes.Contains(p, marker) {
			return true
		}
	}
	return false
}

// UpstreamError is the OpenAI-compatible error shape extracted from a response body or SSE event.
type UpstreamError struct {
	Type    string
	Code    string
	Message string
}

// IsCacheableResponse reports whether a completed upstream response may be stored: any success
// whose payload carries no failure, or a deterministic client-input error.
func IsCacheableResponse(status int, body []byte) bool {
	if len(body) == 0 || HasNonCacheableError(body) {
		return false
	}
	if status >= 200 && status < 300 {
		return true
	}
	return IsCacheableUpstreamError(status, body)
}

// HasNonCacheableError reports whether body carries a failure that must not be replayed, so a
// stored response can be re-checked on read and a poisoned entry drops itself.
func HasNonCacheableError(body []byte) bool {
	details, ok := parseUpstreamErrorDetails(body)
	return ok && !isCacheableErrorDetails(details)
}

// IsCacheableUpstreamError reports whether status/body is a deterministic client-input error
// (safe to cache) rather than a transient, environmental, or model-availability failure.
func IsCacheableUpstreamError(status int, body []byte) bool {
	if status != http.StatusBadRequest {
		return false
	}
	details, ok := parseUpstreamErrorDetails(body)
	if !ok {
		return false
	}
	return isCacheableErrorDetails(details)
}

// parseUpstreamErrorDetails extracts an OpenAI-compatible top-level error from a response body,
// whether the body is plain JSON or an SSE stream carrying the failure inside a data event.
func parseUpstreamErrorDetails(payload []byte) (UpstreamError, bool) {
	if details, ok := DecodeUpstreamError(payload); ok {
		return details, true
	}
	var found UpstreamError
	EachSSEDataPayload(payload, func(data []byte) bool {
		details, ok := DecodeUpstreamError(data)
		found = details
		return ok
	})
	return found, found != UpstreamError{}
}

// DecodeUpstreamError reads one JSON payload as an OpenAI-compatible error, accepting both the
// nested {"error":{...}} shape and the flat {"object":"error",...} one vLLM still emits.
func DecodeUpstreamError(payload []byte) (UpstreamError, bool) {
	var body struct {
		Error *struct {
			Type    string `json:"type"`
			Code    any    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Object  string `json:"object"`
		Type    string `json:"type"`
		Code    any    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return UpstreamError{}, false
	}
	if body.Error != nil {
		return UpstreamError{Type: body.Error.Type, Code: codeString(body.Error.Code), Message: body.Error.Message}, true
	}
	if body.Object == "error" && body.Message != "" {
		return UpstreamError{Type: body.Type, Code: codeString(body.Code), Message: body.Message}, true
	}
	return UpstreamError{}, false
}

// codeString renders an error code field as a string, treating JSON null as absent rather than
// the literal text "<nil>".
func codeString(code any) string {
	if code == nil {
		return ""
	}
	return fmt.Sprint(code)
}

func isCacheableErrorDetails(details UpstreamError) bool {
	msg := strings.ToLower(details.Message)
	if strings.TrimSpace(msg) == "" {
		return false
	}
	if isRetriableCapabilityError(details.Message) {
		return false
	}
	typ := strings.ToLower(details.Type)
	code := strings.ToLower(details.Code)
	for _, marker := range nonCacheableErrorMarkers {
		if strings.Contains(msg, marker) || strings.Contains(typ, marker) || strings.Contains(code, marker) {
			return false
		}
	}
	return true
}

// isRetriableCapabilityError reports host-capability failures (tool-choice support, context
// window size) that are excluded from caching because a different host may serve them fine.
func isRetriableCapabilityError(msg string) bool {
	contextLimit, _ := CapabilityLimits(msg)
	return strings.Contains(msg, ToolChoiceUnsupportedMessage) || contextLimit > 0
}
