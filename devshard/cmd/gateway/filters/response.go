package filters

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"devshard"

	json "github.com/goccy/go-json"
)

var (
	// clientStrippedFields are response-body keys, at any nesting depth, hidden from a client that asked
	// for none of them. Paired with parameterTable's force rules by
	// TestForcedRequestParametersHaveResponseStripCounterpart.
	clientStrippedFields = []string{
		"logprob",
		"logprobs",
		"top_logprobs",
		"token_ids",
		"prompt_token_ids",
		"prompt_logprobs",
	}

	// requestableFields are the ones a client can ask for, so they survive the strip when it did.
	requestableFields = []string{
		"logprob",
		"logprobs",
		"top_logprobs",
	}

	// alwaysStrippedFields is what remains: internals no request can ask for. Derived rather than
	// written out, so a field added to the list above cannot be left out of this one and reach a client
	// that asked for nothing -- the same reason strippableMarkers is derived.
	alwaysStrippedFields = func() []string {
		fields := make([]string, 0, len(clientStrippedFields))
		for _, field := range clientStrippedFields {
			if !slices.Contains(requestableFields, field) {
				fields = append(fields, field)
			}
		}
		return fields
	}()

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

	// unicodeEscape is the only escape that can encode a letter of a field name.
	unicodeEscape = []byte(`\u`)

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

// LogprobIntent is what the client's own request asked for, read before the force rules overwrite it.
// The gateway turns logprobs on upstream for validation whatever the client sent, so without this the
// response strip cannot tell a client who asked for logprobs from one who did not, and answers both by
// removing them. See gateway-request-filtering.md, "The response side".
type LogprobIntent struct {
	Keep    bool
	KeepTop bool
}

// strippedFields is what this client must not see. A client that asked for logprobs keeps them; one
// that did not loses the whole family, including a top_logprobs a host placed outside them.
func (intent LogprobIntent) strippedFields() []string {
	if intent.Keep {
		return alwaysStrippedFields
	}
	return clientStrippedFields
}

// StripResponseBody removes the fields this client must not see from a non-streaming JSON response
// body, at any nesting depth. A malformed body passes through unchanged.
func StripResponseBody(body []byte, intent LogprobIntent) []byte {
	filtered, outcome := stripInternalFields(body, intent)
	if outcome != stripRewritten {
		return body
	}
	return filtered
}

// stripInternalFields stays on the standard library: goccy's UseNumber parses the token anyway and
// errors past float64 range, which fails this open -- a body carrying 1e999 keeps every internal field.
// See gateway-request-filtering.md, "The response side".
func stripInternalFields(payload []byte, intent LogprobIntent) ([]byte, stripOutcome) {
	decoder := stdjson.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil || decoder.More() {
		return nil, stripMalformed
	}
	changed := deleteFields(decoded, intent.strippedFields())
	if intent.Keep && !intent.KeepTop {
		changed = emptyTopLogprobs(decoded) || changed
	}
	if !changed {
		return nil, stripUnchanged
	}
	var encoded bytes.Buffer
	encoder := stdjson.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false) // the default inflates every < > & in generated content to six bytes
	if err := encoder.Encode(decoded); err != nil {
		return nil, stripMalformed
	}
	return bytes.TrimRight(encoded.Bytes(), "\n"), stripRewritten
}

// deleteFields removes the named keys at any depth, reporting whether anything went.
func deleteFields(value any, fields []string) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for _, field := range fields {
			if _, held := typed[field]; held {
				delete(typed, field)
				changed = true
			}
		}
		for _, child := range typed {
			changed = deleteFields(child, fields) || changed
		}
		return changed
	case []any:
		changed := false
		for _, child := range typed {
			changed = deleteFields(child, fields) || changed
		}
		return changed
	default:
		return false
	}
}

// hasStrippableField is a cheap pre-check so untouched chunks skip the SSE split entirely. A \u escape
// sends the payload down the full strip regardless: the scan reads raw bytes, while the client's
// decoder reads "\u006cogprobs" as logprobs, and only that escape form can spell a letter in a key.
func hasStrippableField(p []byte) bool {
	if bytes.Contains(p, unicodeEscape) {
		return true
	}
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

// emptyTopLogprobs replaces every top_logprobs array with an empty one, which is the shape OpenAI
// returns to a client that asked for logprobs without alternatives. The gateway forces the
// alternatives on upstream for validation, so leaving them would hand the client a request it never
// made; removing the key would drop a field its schema expects to be present.
func emptyTopLogprobs(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		if existing, held := typed["top_logprobs"]; held {
			if list, isList := existing.([]any); !isList || len(list) > 0 {
				typed["top_logprobs"] = []any{}
				changed = true
			}
		}
		for _, child := range typed {
			changed = emptyTopLogprobs(child) || changed
		}
		return changed
	case []any:
		changed := false
		for _, child := range typed {
			changed = emptyTopLogprobs(child) || changed
		}
		return changed
	default:
		return false
	}
}

// decodeLogprobIntent reads what the client asked for, leniently: only an explicit true counts as a
// request, so a value of the wrong shape is read as "not asked" rather than rejecting a request the
// gateway would otherwise have accepted -- the force rules overwrite both fields regardless of type.
func decodeLogprobIntent(document *Document) LogprobIntent {
	var intent LogprobIntent
	if raw, held := document.Get("logprobs"); held {
		if asked, isBool := raw.(bool); isBool {
			intent.Keep = asked
		}
	}
	if !intent.Keep {
		return intent
	}
	if raw, held := document.Get("top_logprobs"); held {
		if count, isNumber := devshard.JSONNumericUint64(raw); isNumber && count > 0 {
			intent.KeepTop = true
		}
	}
	return intent
}
