package filters

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	json "github.com/goccy/go-json"

	"devshard"
)

var (
	// clientStrippedFields are response keys, at any depth, hidden from a client that asked for none of them.
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

	// alwaysStrippedFields is derived, not written out: a hand-written second list is how top_logprobs once leaked.
	alwaysStrippedFields = func() []string {
		fields := make([]string, 0, len(clientStrippedFields))
		for _, field := range clientStrippedFields {
			if !slices.Contains(requestableFields, field) {
				fields = append(fields, field)
			}
		}
		return fields
	}()

	clientStrippedFieldSet = fieldSet(clientStrippedFields)
	alwaysStrippedFieldSet = fieldSet(alwaysStrippedFields)

	// nonCacheableErrorMarkers identify transient or availability failures excluded from caching.
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

// LogprobIntent is what the client asked for, read before the force rules overwrite it. See README.md, "Response stripping".
type LogprobIntent struct {
	Keep    bool
	KeepTop bool
}

// strippedFields is what this client must not see; one that asked for nothing loses the whole logprob family.
func (intent LogprobIntent) strippedFields() map[string]struct{} {
	if intent.Keep {
		return alwaysStrippedFieldSet
	}
	return clientStrippedFieldSet
}

func fieldSet(fields []string) map[string]struct{} {
	set := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		set[field] = struct{}{}
	}
	return set
}

// stripResponseBody removes the hidden fields at any depth; a malformed body passes through unchanged.
func stripResponseBody(body []byte, intent LogprobIntent) []byte {
	filtered, _, outcome := stripInternalFields(body, intent)
	if outcome != stripRewritten {
		return body
	}
	return filtered
}

// decodePayload stays on the standard library: goccy errors past float64 range, failing this open. See README.md, "Which JSON decoder, and why".
func decodePayload(payload []byte) (decoded any, normalized, ok bool) {
	if value, parsed := decodeJSONValue(payload); parsed {
		return value, false, true
	}
	// A backend writes NaN/Infinity as barewords; without this nothing can inspect or forward the body.
	rewritten, replaced := ReplaceNonFiniteNumbers(payload)
	if !replaced {
		return nil, false, false
	}
	value, parsed := decodeJSONValue(rewritten)
	return value, true, parsed
}

func decodeJSONValue(payload []byte) (any, bool) {
	decoder := stdjson.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil || decoder.More() {
		return nil, false
	}
	return decoded, true
}

// stripDecodedFields deletes what this client must not see from a payload already decoded.
func stripDecodedFields(decoded any, intent LogprobIntent) bool {
	changed := deleteFields(decoded, intent.strippedFields())
	if intent.Keep && !intent.KeepTop {
		changed = emptyTopLogprobs(decoded) || changed
	}
	return changed
}

// stripInternalFields hands back what it decoded so a caller can ask the payload a further question without decoding it a second time.
func stripInternalFields(payload []byte, intent LogprobIntent) ([]byte, any, stripOutcome) {
	decoded, normalized, ok := decodePayload(payload)
	if !ok {
		return nil, nil, stripMalformed
	}
	if !stripDecodedFields(decoded, intent) && !normalized {
		return nil, decoded, stripUnchanged
	}
	encoded, err := encodeCompact(decoded)
	if err != nil {
		return nil, decoded, stripMalformed
	}
	return encoded, decoded, stripRewritten
}

// One pass over the decoded keys, rather than one lookup per stripped name: a chunk's objects are smaller than the list.
func deleteFields(value any, fields map[string]struct{}) bool {
	switch typed := value.(type) {
	case map[string]any:
		changed := false
		for key, child := range typed {
			if _, stripped := fields[key]; stripped {
				delete(typed, key)
				changed = true
				continue
			}
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

// UpstreamError is the OpenAI-compatible error shape extracted from a response body or SSE event.
type UpstreamError struct {
	Type    string
	Code    string
	Message string
}

// IsCacheableResponse reports whether a completed upstream response may be stored.
func IsCacheableResponse(status int, body []byte) bool {
	if len(body) == 0 || HasNonCacheableError(body) {
		return false
	}
	if status >= 200 && status < 300 {
		return true
	}
	return IsCacheableUpstreamError(status, body)
}

// HasNonCacheableError reports a failure that must not be replayed, so a poisoned entry drops itself on read.
func HasNonCacheableError(body []byte) bool {
	details, ok := parseUpstreamErrorDetails(body)
	return ok && !isCacheableErrorDetails(details)
}

// IsCacheableUpstreamError reports a deterministic client-input error, safe to cache.
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

// parseUpstreamErrorDetails extracts a top-level error from plain JSON or from an SSE data event.
func parseUpstreamErrorDetails(payload []byte) (UpstreamError, bool) {
	if details, ok := DecodeUpstreamError(payload); ok {
		return details, true
	}
	var found UpstreamError
	// An empty {"error":{}} carries nothing, so stopping on it would leave a real error in a later event unseen.
	EachSSEDataPayload(payload, func(data []byte) bool {
		details, ok := DecodeUpstreamError(data)
		if !ok || details == (UpstreamError{}) {
			return false
		}
		found = details
		return true
	})
	return found, found != UpstreamError{}
}

// DecodeUpstreamError accepts both the nested {"error":{...}} shape and the flat {"object":"error",...} one vLLM emits.
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

// codeString treats a JSON null code as absent rather than the literal text "<nil>".
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

// isRetriableCapabilityError excludes host-capability failures from caching: a different host may serve them fine.
func isRetriableCapabilityError(msg string) bool {
	contextLimit, _ := CapabilityLimits(msg)
	return strings.Contains(msg, ToolChoiceUnsupportedMessage) || contextLimit > 0
}

// emptyTopLogprobs leaves the empty array OpenAI returns when alternatives were not asked for. See README.md, "Response stripping".
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

// decodeLogprobIntent is lenient: only an explicit true counts, so a wrong-shaped value reads as "not asked".
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

// nonFiniteLiterals are the barewords a backend writes for a probability of zero; none is valid JSON.
var nonFiniteLiterals = [][]byte{[]byte("-Infinity"), []byte("Infinity"), []byte("NaN")}

// ReplaceNonFiniteNumbers rewrites the barewords above to null outside strings; ok=false when the body carries none, allocating nothing.
func ReplaceNonFiniteNumbers(body []byte) ([]byte, bool) {
	carries := false
	for _, literal := range nonFiniteLiterals {
		if bytes.Contains(body, literal) {
			carries = true
			break
		}
	}
	if !carries {
		return nil, false
	}
	out := make([]byte, 0, len(body))
	replaced := false
	for index := 0; index < len(body); {
		if body[index] == '"' {
			// One rule for where a string literal ends, shared with the structural scan.
			literalEnd := index + 1 + stringLiteralEnd(body[index+1:])
			out = append(out, body[index:literalEnd]...)
			index = literalEnd
			continue
		}
		if literal := matchNonFinite(body[index:]); literal > 0 {
			out = append(out, []byte("null")...)
			index += literal
			replaced = true
			continue
		}
		out = append(out, body[index])
		index++
	}
	return out, replaced
}

// matchNonFinite reports the length of a bareword at the start of tail; longest first, so -Infinity is not read as Infinity.
func matchNonFinite(tail []byte) int {
	for _, literal := range nonFiniteLiterals {
		if bytes.HasPrefix(tail, literal) {
			return len(literal)
		}
	}
	return 0
}
