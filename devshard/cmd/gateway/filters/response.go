package filters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// clientStrippedFields are response-body keys, at any nesting depth, hidden from the client.
// Paired with parameterTable's force rules by TestForcedRequestParametersHaveResponseStripCounterpart.
var clientStrippedFields = []string{
	"logprob",
	"logprobs",
	"top_logprobs",
	"token_ids",
	"prompt_token_ids",
	"prompt_logprobs",
}

// forcedParameterResponseField maps a forced request parameter to its response field, for the
// one case where the names differ: return_token_ids (request) makes vLLM emit token_ids.
var forcedParameterResponseField = map[string]string{
	"return_token_ids": "token_ids",
}

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

// stripInternalFields deletes clientStrippedFields from payload's decoded JSON, reporting whether
// the payload parsed and whether anything was removed.
func stripInternalFields(payload []byte) ([]byte, stripOutcome) {
	var decoded any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return nil, stripMalformed
	}
	if !deleteInternalFields(decoded) {
		return nil, stripUnchanged
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return nil, stripMalformed
	}
	return encoded, stripRewritten
}

// deleteInternalFields recursively removes clientStrippedFields keys from v at any depth,
// reporting whether it deleted anything.
func deleteInternalFields(v any) bool {
	switch typed := v.(type) {
	case map[string]any:
		changed := false
		for _, key := range clientStrippedFields {
			if _, ok := typed[key]; ok {
				delete(typed, key)
				changed = true
			}
		}
		for _, child := range typed {
			if deleteInternalFields(child) {
				changed = true
			}
		}
		return changed
	case []any:
		changed := false
		for _, child := range typed {
			if deleteInternalFields(child) {
				changed = true
			}
		}
		return changed
	default:
		return false
	}
}

// hasStrippableField is a cheap pre-check so untouched chunks skip the SSE split entirely.
// The 4 markers cover all 6 clientStrippedFields: "logprob catches logprob(s)/top_logprobs;
// the prompt_* fields need their own check since a leading "prompt_" breaks that quote match.
func hasStrippableField(p []byte) bool {
	return bytes.Contains(p, []byte(`"logprob`)) ||
		bytes.Contains(p, []byte(`"token_ids"`)) ||
		bytes.Contains(p, []byte(`"prompt_logprobs"`)) ||
		bytes.Contains(p, []byte(`"prompt_token_ids"`))
}

// upstreamErrorDetails is the OpenAI-compatible error shape extracted from a response body.
type upstreamErrorDetails struct {
	Type    string
	Code    string
	Message string
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

// parseUpstreamErrorDetails extracts an OpenAI-compatible top-level error from a plain JSON
// response body, accepting both the {"error":{...}} and legacy {"object":"error",...} shapes.
func parseUpstreamErrorDetails(payload []byte) (upstreamErrorDetails, bool) {
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
		return upstreamErrorDetails{}, false
	}
	if body.Error != nil {
		return upstreamErrorDetails{Type: body.Error.Type, Code: codeString(body.Error.Code), Message: body.Error.Message}, true
	}
	if body.Object == "error" && body.Message != "" {
		return upstreamErrorDetails{Type: body.Type, Code: codeString(body.Code), Message: body.Message}, true
	}
	return upstreamErrorDetails{}, false
}

// codeString renders an error code field as a string, treating JSON null as absent rather than
// the literal text "<nil>".
func codeString(code any) string {
	if code == nil {
		return ""
	}
	return fmt.Sprint(code)
}

// nonCacheableErrorMarkers identify transient, environmental, or model-availability failures
// excluded from caching regardless of which of message/type/code carries them.
var nonCacheableErrorMarkers = []string{
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

// isCacheableErrorDetails excludes transient, environmental, and model-availability errors;
// everything else (typically a fixed validation failure) is cacheable.
func isCacheableErrorDetails(details upstreamErrorDetails) bool {
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

// toolChoiceUnsupportedMessage is emitted when a host lacks --enable-auto-tool-choice: a host
// capability gap, not a deterministic client error, so it's excluded from caching.
const toolChoiceUnsupportedMessage = "tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"

// isRetriableCapabilityError reports host-capability failures (tool-choice support, context
// window size) that are excluded from caching because a different host may serve them fine.
func isRetriableCapabilityError(msg string) bool {
	return strings.Contains(msg, toolChoiceUnsupportedMessage) || contextLengthLimit(msg) > 0
}

// contextLengthLimit extracts the token count from a message like "maximum context length is
// 131072 tokens"; returns 0 when the marker or a following number is absent.
func contextLengthLimit(msg string) uint64 {
	const marker = "maximum context length is "
	lower := strings.ToLower(msg)
	idx := strings.Index(lower, marker)
	if idx < 0 {
		return 0
	}
	rest := msg[idx+len(marker):]
	end := strings.IndexFunc(rest, func(r rune) bool { return r < '0' || r > '9' })
	if end <= 0 {
		return 0
	}
	limit, err := strconv.ParseUint(rest[:end], 10, 64)
	if err != nil {
		return 0
	}
	return limit
}
