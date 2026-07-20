package filters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func readSSEFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "sse", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return data
}

// splitCompleteEvents splits an SSE stream into its "\n\n"-terminated events, dropping the
// trailing empty piece SplitAfter produces when the input ends on the separator.
func splitCompleteEvents(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	var events [][]byte
	for _, event := range bytes.SplitAfter(stream, []byte("\n\n")) {
		if len(event) == 0 {
			continue
		}
		events = append(events, event)
	}
	return events
}

// --- clientStrippedFields ---

func TestClientStrippedFieldsExactList(t *testing.T) {
	want := []string{
		"logprob",
		"logprobs",
		"top_logprobs",
		"token_ids",
		"prompt_token_ids",
		"prompt_logprobs",
	}
	if !reflect.DeepEqual(clientStrippedFields, want) {
		t.Errorf("clientStrippedFields = %#v, want %#v", clientStrippedFields, want)
	}
}

// --- pairing test: the design's point ---

// TestForcedRequestParametersHaveResponseStripCounterpart is the pairing test: every forced
// request field must have a matching clientStrippedFields entry, so the two can't drift apart.
func TestForcedRequestParametersHaveResponseStripCounterpart(t *testing.T) {
	stripped := make(map[string]bool, len(clientStrippedFields))
	for _, field := range clientStrippedFields {
		stripped[field] = true
	}
	if len(forcedParameterNames) == 0 {
		t.Fatal("forcedParameterNames is empty; the pairing test would pass vacuously")
	}
	for _, name := range forcedParameterNames {
		responseField := name
		if mapped, ok := forcedParameterResponseField[name]; ok {
			responseField = mapped
		}
		if !stripped[responseField] {
			t.Errorf("forced request parameter %q (response field %q) has no matching entry in clientStrippedFields", name, responseField)
		}
	}
}

// --- RewriteStreamChunk: fixtures with nothing to strip pass through byte-identical ---

func TestRewriteStreamChunk_PureContentPassthrough(t *testing.T) {
	tests := []string{
		"content_stream.sse",
		"tool_calls_stream.sse",
		"newlineless_final_content.sse",
		"newlineless_final_error.sse",
	}
	for _, fixture := range tests {
		t.Run(fixture, func(t *testing.T) {
			input := readSSEFixture(t, fixture)
			got := RewriteStreamChunk(input)
			if !bytes.Equal(got, input) {
				t.Errorf("RewriteStreamChunk() = %q, want unchanged %q", got, input)
			}
		})
	}
}

func TestRewriteStreamChunk_EmptyAndNilInput(t *testing.T) {
	for _, name := range []string{"nil", "empty"} {
		t.Run(name, func(t *testing.T) {
			var input []byte
			if name == "empty" {
				input = []byte{}
			}
			got := RewriteStreamChunk(input)
			if len(got) != 0 {
				t.Errorf("RewriteStreamChunk() = %q, want empty", got)
			}
		})
	}
}

func TestRewriteStreamChunk_DoneMarkerPreservedExactly(t *testing.T) {
	input := readSSEFixture(t, "logprobs_stream.sse")
	got := RewriteStreamChunk(input)
	if !bytes.HasSuffix(got, []byte("data: [DONE]\n\n")) {
		t.Errorf("RewriteStreamChunk() does not end with an unchanged [DONE] event: %q", got)
	}
}

// --- RewriteStreamChunk: field stripping is the point of the function ---

func TestRewriteStreamChunk_StripsLogprobsFamily(t *testing.T) {
	input := readSSEFixture(t, "logprobs_stream.sse")
	got := RewriteStreamChunk(input)
	for _, field := range []string{`"logprobs"`, `"top_logprobs"`, `"logprob"`} {
		if bytes.Contains(got, []byte(field)) {
			t.Errorf("RewriteStreamChunk() output still contains %s: %q", field, got)
		}
	}
	if !bytes.Contains(got, []byte(`"content":"ok"`)) {
		t.Error("RewriteStreamChunk() dropped sibling content field it must preserve")
	}
	if !bytes.Contains(got, []byte(`"finish_reason":"stop"`)) {
		t.Error("RewriteStreamChunk() dropped sibling finish_reason field it must preserve")
	}
}

func TestRewriteStreamChunk_StripsTokenIdFamily(t *testing.T) {
	input := readSSEFixture(t, "token_ids_stream.sse")
	got := RewriteStreamChunk(input)
	for _, field := range []string{`"token_ids"`, `"prompt_token_ids"`, `"prompt_logprobs"`} {
		if bytes.Contains(got, []byte(field)) {
			t.Errorf("RewriteStreamChunk() output still contains %s: %q", field, got)
		}
	}
	if !bytes.Contains(got, []byte(`"content":"ok"`)) {
		t.Error("RewriteStreamChunk() dropped sibling content field it must preserve")
	}
}

// --- RewriteStreamChunk: malformed / non-data lines pass through unchanged ---

func TestRewriteStreamChunk_MixedStreamOnlyRewritesParseableEvents(t *testing.T) {
	input := readSSEFixture(t, "malformed_data_line.sse")
	got := RewriteStreamChunk(input)
	// The well-formed first event's logprobs must be gone.
	if bytes.Contains(got, []byte(`"token":"ok","logprob"`)) {
		t.Error("well-formed event's logprobs was not stripped")
	}
	// The malformed second event is unparseable JSON and must survive byte-for-byte,
	// including its own raw "logprobs"/"logprob" text.
	malformedEvent := `data: {"id":"chatcmpl-9","broken":true,"logprobs":{"content":[{"token":"partial","logprob":-0.02}]` + "\n\n"
	if !bytes.Contains(got, []byte(malformedEvent)) {
		t.Errorf("malformed event was altered; want it verbatim in output %q", got)
	}
	if !bytes.HasSuffix(got, []byte("data: [DONE]\n\n")) {
		t.Error("[DONE] marker not preserved after a malformed event")
	}
}

func TestRewriteStreamChunk_CommentAndBlankLinesPassThrough(t *testing.T) {
	input := readSSEFixture(t, "comment_and_blank_lines.sse")
	got := RewriteStreamChunk(input)
	if !bytes.HasPrefix(got, []byte(": keep-alive\n\n")) {
		t.Errorf("comment line not preserved verbatim at head of output: %q", got)
	}
	if bytes.Contains(got, []byte(`"logprobs"`)) {
		t.Error("the data event's logprobs was not stripped")
	}
	if !bytes.Contains(got, []byte(`"content":"hi"`)) {
		t.Error("dropped sibling content field it must preserve")
	}
}

// --- RewriteStreamChunk: chunk framing contract ---

func TestRewriteStreamChunk_ChunkByChunkMatchesWholeStream(t *testing.T) {
	fixtures := []string{
		"content_stream.sse",
		"tool_calls_stream.sse",
		"logprobs_stream.sse",
		"token_ids_stream.sse",
		"malformed_data_line.sse",
		"comment_and_blank_lines.sse",
	}
	for _, name := range fixtures {
		t.Run(name, func(t *testing.T) {
			input := readSSEFixture(t, name)
			whole := RewriteStreamChunk(input)
			var chunked bytes.Buffer
			for _, event := range splitCompleteEvents(t, input) {
				chunked.Write(RewriteStreamChunk(event))
			}
			if !bytes.Equal(whole, chunked.Bytes()) {
				t.Errorf("chunk-by-chunk result differs from whole-stream result\n whole:   %q\n chunked: %q", whole, chunked.Bytes())
			}
		})
	}
}

// TestRewriteStreamChunk_NonEventAlignedSplitDoesNotStripAcrossBoundary pins the documented
// contract: RewriteStreamChunk holds no state across calls, so a mid-JSON split leaves both
// halves unmodified (neither parses alone) rather than corrupted or silently stripped.
func TestRewriteStreamChunk_NonEventAlignedSplitDoesNotStripAcrossBoundary(t *testing.T) {
	input := readSSEFixture(t, "logprobs_stream.sse")
	events := splitCompleteEvents(t, input)
	target := events[1] // the logprobs-bearing event
	splitAt := bytes.Index(target, []byte("top_logprobs")) + len("top_logprobs")
	if splitAt <= len("top_logprobs") || splitAt >= len(target) {
		t.Fatalf("fixture no longer contains a mid-event top_logprobs split point")
	}
	firstHalf, secondHalf := target[:splitAt], target[splitAt:]

	firstOut := RewriteStreamChunk(firstHalf)
	secondOut := RewriteStreamChunk(secondHalf)

	reassembled := append(append([]byte(nil), firstOut...), secondOut...)
	if !bytes.Equal(reassembled, target) {
		t.Errorf("non-aligned split must reassemble to the original event unmodified\n got:  %q\n want: %q", reassembled, target)
	}
	if !bytes.Contains(reassembled, []byte(`"logprob"`)) {
		t.Error("expected the unstripped logprob text to survive a non-aligned split (documented limitation)")
	}
}

// --- StripResponseBody ---

func TestStripResponseBody_RemovesAllInternalFieldsAtAnyDepth(t *testing.T) {
	body := []byte(`{
		"id": "chatcmpl-full",
		"object": "chat.completion",
		"choices": [
			{
				"index": 0,
				"message": {"role": "assistant", "content": "hi"},
				"logprobs": {"content": [{"token": "hi", "logprob": -0.1, "top_logprobs": [{"token": "hi", "logprob": -0.1}]}]},
				"token_ids": [1, 2, 3],
				"finish_reason": "stop"
			}
		],
		"prompt_token_ids": [9, 8, 7],
		"prompt_logprobs": null
	}`)
	got := StripResponseBody(body)
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("StripResponseBody() produced invalid JSON: %v (%q)", err, got)
	}
	for _, field := range []string{"logprob", "logprobs", "top_logprobs", "token_ids", "prompt_token_ids", "prompt_logprobs"} {
		if bytes.Contains(got, fmt.Appendf(nil, "%q", field)) {
			t.Errorf("StripResponseBody() output still contains %q: %s", field, got)
		}
	}
	choices, ok := decoded["choices"].([]any)
	if !ok || len(choices) != 1 {
		t.Fatalf("choices missing or malformed after strip: %#v", decoded["choices"])
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		t.Fatalf("choices[0] is not an object: %#v", choices[0])
	}
	if choice["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %#v, want %q", choice["finish_reason"], "stop")
	}
	message, ok := choice["message"].(map[string]any)
	if !ok || message["content"] != "hi" {
		t.Errorf("message.content not preserved: %#v", choice["message"])
	}
}

func TestStripResponseBody_NoChangeReturnsEquivalentBytes(t *testing.T) {
	body := []byte(`{"id":"chatcmpl-plain","choices":[{"message":{"content":"hi"}}]}`)
	got := StripResponseBody(body)
	if !bytes.Equal(got, body) {
		t.Errorf("StripResponseBody() = %q, want unchanged %q", got, body)
	}
}

func TestStripResponseBody_MalformedBodyPassesThroughUnchanged(t *testing.T) {
	body := []byte(`this is not json`)
	got := StripResponseBody(body)
	if !bytes.Equal(got, body) {
		t.Errorf("StripResponseBody() = %q, want unchanged %q", got, body)
	}
}

func TestStripResponseBody_EmptyBodyPassesThroughUnchanged(t *testing.T) {
	got := StripResponseBody([]byte{})
	if len(got) != 0 {
		t.Errorf("StripResponseBody() = %q, want empty", got)
	}
}

func TestStripResponseBody_NullValuedFieldAlsoStripped(t *testing.T) {
	body := []byte(`{"id":"x","prompt_logprobs":null,"choices":[]}`)
	got := StripResponseBody(body)
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if _, exists := decoded["prompt_logprobs"]; exists {
		t.Errorf("prompt_logprobs key still present after strip: %s", got)
	}
}

// --- IsCacheableUpstreamError ---

func TestIsCacheableUpstreamError(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"deterministic validation error is cacheable", 400, `{"error":{"type":"invalid_request_error","message":"temperature must be between 0 and 2"}}`, true},
		{"legacy object-error shape is cacheable", 400, `{"object":"error","type":"invalid_request_error","message":"unknown parameter foo"}`, true},
		{"empty message is not cacheable", 400, `{"error":{"message":""}}`, false},
		{"body without a recognizable error shape is not cacheable", 400, `{"choices":[]}`, false},
		{"malformed body is not cacheable", 400, `not json`, false},
		{"null code does not crash and is cacheable", 400, `{"error":{"code":null,"message":"fine"}}`, true},
		{"status 200 is never treated as a cacheable error", 200, `{"error":{"message":"anything"}}`, false},
		{"status 500 is never cacheable regardless of message", 500, `{"error":{"message":"a fixed validation failure"}}`, false},
		{"status 429 is never cacheable", 429, `{"error":{"message":"a fixed validation failure"}}`, false},
		{"status 401 is never cacheable", 401, `{"error":{"message":"a fixed validation failure"}}`, false},
		{"tool choice capability error is not cacheable", 400, `{"error":{"message":"tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"}}`, false},
		{"context length capability error is not cacheable", 400, `{"error":{"message":"This model's maximum context length is 120000 tokens"}}`, false},
		{"context length marker with no digits is cacheable", 400, `{"error":{"message":"maximum context length is unknown here"}}`, true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			got := IsCacheableUpstreamError(testCase.status, []byte(testCase.body))
			if got != testCase.want {
				t.Errorf("IsCacheableUpstreamError(%d, %s) = %v, want %v", testCase.status, testCase.body, got, testCase.want)
			}
		})
	}
}

// TestIsCacheableUpstreamError_EveryMarkerExcludes is table-driven over every entry in
// nonCacheableErrorMarkers, proving each is individually caught in message, type, and code.
func TestIsCacheableUpstreamError_EveryMarkerExcludes(t *testing.T) {
	for _, marker := range nonCacheableErrorMarkers {
		t.Run("marker in message: "+marker, func(t *testing.T) {
			body := fmt.Sprintf(`{"error":{"message":%q}}`, "request failed: "+marker+" occurred")
			if got := IsCacheableUpstreamError(400, []byte(body)); got {
				t.Errorf("IsCacheableUpstreamError() = true for message marker %q, want false", marker)
			}
		})
		t.Run("marker in type, case-insensitive: "+marker, func(t *testing.T) {
			body := fmt.Sprintf(`{"error":{"type":%q,"message":"generic failure"}}`, strings.ToUpper(marker))
			if got := IsCacheableUpstreamError(400, []byte(body)); got {
				t.Errorf("IsCacheableUpstreamError() = true for type marker %q, want false", marker)
			}
		})
		t.Run("marker in code: "+marker, func(t *testing.T) {
			body := fmt.Sprintf(`{"error":{"code":%q,"message":"generic failure"}}`, marker)
			if got := IsCacheableUpstreamError(400, []byte(body)); got {
				t.Errorf("IsCacheableUpstreamError() = true for code marker %q, want false", marker)
			}
		})
	}
}
