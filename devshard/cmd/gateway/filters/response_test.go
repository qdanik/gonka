package filters

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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

// rewriteWholeStream drives one StreamRewriter over the whole stream and returns everything it
// emits, so the fixtures assert against the rewriter production streams through.
func rewriteWholeStream(t *testing.T, stream []byte) []byte {
	t.Helper()
	rewriter := NewStreamRewriter(LogprobIntent{}, true)
	emitted, err := rewriter.Write(stream)
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}
	final, err := rewriter.Close()
	if err != nil {
		t.Fatalf("Close() = %v", err)
	}
	return append(emitted, final...)
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

// forcedParameterNames derives the forced set from parameterTable itself by running every rule
// against an empty document: only a force rule writes its own parameter with nothing to act on.
func forcedParameterNames(t *testing.T) []string {
	t.Helper()
	var forced []string
	for _, parameter := range parameterTable {
		for _, rule := range parameter.Rules {
			document, err := ParseDocument([]byte(`{}`))
			if err != nil {
				t.Fatalf("ParseDocument: %v", err)
			}
			if rule.Apply(RuleContext{Document: document, Param: parameter.Name}) != nil {
				continue
			}
			if _, written := document.Get(parameter.Name); written {
				forced = append(forced, parameter.Name)
				break
			}
		}
	}
	return forced
}

// forcedParameterResponseField maps a forced request parameter to its response field, for the one
// case where the names differ: return_token_ids (request) makes vLLM emit token_ids.
var forcedParameterResponseField = map[string]string{
	"return_token_ids": "token_ids",
}

// TestForcedRequestParametersHaveResponseStripCounterpart is the pairing test: every field
// parameterTable forces on must have a matching clientStrippedFields entry, so a force rule added
// without a strip counterpart leaks internal fields to the client.
func TestForcedRequestParametersHaveResponseStripCounterpart(t *testing.T) {
	stripped := make(map[string]bool, len(clientStrippedFields))
	for _, field := range clientStrippedFields {
		stripped[field] = true
	}
	forced := forcedParameterNames(t)
	if len(forced) == 0 {
		t.Fatal("parameterTable declares no forced parameter; the pairing test would pass vacuously")
	}
	for _, name := range forced {
		responseField := name
		if mapped, ok := forcedParameterResponseField[name]; ok {
			responseField = mapped
		}
		if !stripped[responseField] {
			t.Errorf("forced request parameter %q (response field %q) has no matching entry in clientStrippedFields", name, responseField)
		}
	}
}

func TestStreamRewriterFixture_PureContentPassthrough(t *testing.T) {
	tests := []string{
		"content_stream.sse",
		"tool_calls_stream.sse",
		"newlineless_final_content.sse",
		"newlineless_final_error.sse",
	}
	for _, fixture := range tests {
		t.Run(fixture, func(t *testing.T) {
			input := readSSEFixture(t, fixture)
			got := rewriteWholeStream(t, input)
			if !bytes.Equal(got, input) {
				t.Errorf("rewritten stream = %q, want unchanged %q", got, input)
			}
		})
	}
}

func TestStreamRewriterFixture_EmptyAndNilInput(t *testing.T) {
	for _, name := range []string{"nil", "empty"} {
		t.Run(name, func(t *testing.T) {
			var input []byte
			if name == "empty" {
				input = []byte{}
			}
			got := rewriteWholeStream(t, input)
			if len(got) != 0 {
				t.Errorf("rewritten stream = %q, want empty", got)
			}
		})
	}
}

func TestStreamRewriterFixture_DoneMarkerPreservedExactly(t *testing.T) {
	input := readSSEFixture(t, "logprobs_stream.sse")
	got := rewriteWholeStream(t, input)
	if !bytes.HasSuffix(got, []byte("data: [DONE]\n\n")) {
		t.Errorf("rewritten stream does not end with an unchanged [DONE] event: %q", got)
	}
}

func TestStreamRewriterFixture_StripsLogprobsFamily(t *testing.T) {
	input := readSSEFixture(t, "logprobs_stream.sse")
	got := rewriteWholeStream(t, input)
	for _, field := range []string{`"logprobs"`, `"top_logprobs"`, `"logprob"`} {
		if bytes.Contains(got, []byte(field)) {
			t.Errorf("rewritten stream output still contains %s: %q", field, got)
		}
	}
	if !bytes.Contains(got, []byte(`"content":"ok"`)) {
		t.Error("rewritten stream dropped sibling content field it must preserve")
	}
	if !bytes.Contains(got, []byte(`"finish_reason":"stop"`)) {
		t.Error("rewritten stream dropped sibling finish_reason field it must preserve")
	}
}

func TestStreamRewriterFixture_StripsTokenIdFamily(t *testing.T) {
	input := readSSEFixture(t, "token_ids_stream.sse")
	got := rewriteWholeStream(t, input)
	for _, field := range []string{`"token_ids"`, `"prompt_token_ids"`, `"prompt_logprobs"`} {
		if bytes.Contains(got, []byte(field)) {
			t.Errorf("rewritten stream output still contains %s: %q", field, got)
		}
	}
	if !bytes.Contains(got, []byte(`"content":"ok"`)) {
		t.Error("rewritten stream dropped sibling content field it must preserve")
	}
}

func TestStreamRewriterFixture_MalformedEventIsDroppedNotForwarded(t *testing.T) {
	input := readSSEFixture(t, "malformed_data_line.sse")
	got := rewriteWholeStream(t, input)
	if bytes.Contains(got, []byte(`"token":"ok","logprob"`)) {
		t.Error("well-formed event's logprobs was not stripped")
	}
	if bytes.Contains(got, []byte("logprob")) {
		t.Errorf("malformed event leaked internal fields instead of being dropped: %q", got)
	}
	if bytes.Contains(got, []byte(`"broken":true`)) {
		t.Errorf("malformed event was forwarded: %q", got)
	}
	if !bytes.HasSuffix(got, []byte("data: [DONE]\n\n")) {
		t.Error("[DONE] marker not preserved after a malformed event")
	}
}

func TestStreamRewriterFixture_CommentAndBlankLinesPassThrough(t *testing.T) {
	input := readSSEFixture(t, "comment_and_blank_lines.sse")
	got := rewriteWholeStream(t, input)
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

func TestStreamRewriterFixture_ChunkByChunkMatchesWholeStream(t *testing.T) {
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
			whole := rewriteWholeStream(t, input)
			rewriter := NewStreamRewriter(LogprobIntent{}, true)
			var chunked bytes.Buffer
			for _, event := range splitCompleteEvents(t, input) {
				emitted, err := rewriter.Write(event)
				if err != nil {
					t.Fatalf("Write() = %v", err)
				}
				chunked.Write(emitted)
			}
			final, err := rewriter.Close()
			if err != nil {
				t.Fatalf("Close() = %v", err)
			}
			chunked.Write(final)
			if !bytes.Equal(whole, chunked.Bytes()) {
				t.Errorf("chunk-by-chunk result differs from whole-stream result\n whole:   %q\n chunked: %q", whole, chunked.Bytes())
			}
		})
	}
}

// A stream ending mid-event must drop the fragment rather than emit the internal fields it still
// carries, and must report the truncation so the response fails instead of completing short.
func TestStreamRewriterFixture_TruncatedEventIsDroppedAndReported(t *testing.T) {
	input := readSSEFixture(t, "logprobs_stream.sse")
	events := splitCompleteEvents(t, input)
	target := events[1]
	splitAt := bytes.Index(target, []byte("top_logprobs")) + len("top_logprobs")
	if splitAt <= len("top_logprobs") || splitAt >= len(target) {
		t.Fatalf("fixture no longer contains a mid-event top_logprobs split point")
	}

	rewriter := NewStreamRewriter(LogprobIntent{}, true)
	emitted, err := rewriter.Write(target[:splitAt])
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}
	if len(emitted) != 0 {
		t.Errorf("truncated data event must be held, not emitted, got %q", emitted)
	}

	final, err := rewriter.Close()
	if !errors.Is(err, ErrStreamTruncatedEvent) {
		t.Errorf("Close() error = %v, want ErrStreamTruncatedEvent", err)
	}
	if len(final) != 0 {
		t.Errorf("truncated data event must be dropped, got %q", final)
	}
}

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
	got := stripResponseBody(body, LogprobIntent{})
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("stripResponseBody(, LogprobIntent{}) produced invalid JSON: %v (%q)", err, got)
	}
	for _, field := range []string{"logprob", "logprobs", "top_logprobs", "token_ids", "prompt_token_ids", "prompt_logprobs"} {
		if bytes.Contains(got, fmt.Appendf(nil, "%q", field)) {
			t.Errorf("stripResponseBody(, LogprobIntent{}) output still contains %q: %s", field, got)
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
	got := stripResponseBody(body, LogprobIntent{})
	if !bytes.Equal(got, body) {
		t.Errorf("stripResponseBody(, LogprobIntent{}) = %q, want unchanged %q", got, body)
	}
}

func TestStripResponseBody_MalformedBodyPassesThroughUnchanged(t *testing.T) {
	body := []byte(`this is not json`)
	got := stripResponseBody(body, LogprobIntent{})
	if !bytes.Equal(got, body) {
		t.Errorf("stripResponseBody(, LogprobIntent{}) = %q, want unchanged %q", got, body)
	}
}

func TestStripResponseBody_EmptyBodyPassesThroughUnchanged(t *testing.T) {
	got := stripResponseBody([]byte{}, LogprobIntent{})
	if len(got) != 0 {
		t.Errorf("stripResponseBody(, LogprobIntent{}) = %q, want empty", got)
	}
}

func TestStripResponseBody_NullValuedFieldAlsoStripped(t *testing.T) {
	body := []byte(`{"id":"x","prompt_logprobs":null,"choices":[]}`)
	got := stripResponseBody(body, LogprobIntent{})
	var decoded map[string]any
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if _, exists := decoded["prompt_logprobs"]; exists {
		t.Errorf("prompt_logprobs key still present after strip: %s", got)
	}
}

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

// What the host said about its own failure, in the order the rule reads it: the status it would have
// answered with, then the class it named, then the words of the message.
func TestAnErrorIsJudgedByWhatTheHostNamed(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "a numeric code of 400 is about the request", body: `{"object":"error","code":400,"message":"context too long","type":"BadRequestError"}`, want: true},
		{name: "422 likewise", body: `{"error":{"code":422,"message":"unprocessable"}}`, want: true},
		{name: "404 names a model another host may serve", body: `{"error":{"code":404,"message":"the model does not exist"}}`},
		{name: "429 is about the moment", body: `{"error":{"code":429,"message":"slow down"}}`},
		{name: "500 is about the moment", body: `{"error":{"code":500,"message":"a fixed validation failure"}}`},
		{name: "a status wins over an innocent message", body: `{"error":{"code":503,"message":"everything is fine"}}`},
		{name: "a class the host named, with no status", body: `{"object":"error","code":null,"message":"boom","type":"server_error"}`},
		{name: "a request class, with no status", body: `{"error":{"type":"BadRequestError","message":"unknown parameter foo"}}`, want: true},
		{name: "a class spelled in the code", body: `{"error":{"code":"rate_limit_exceeded","message":"generic failure"}}`},
		{name: "nothing but a message, which is all the host gave", body: `{"error":{"message":"upstream request timed out"}}`},
		{name: "nothing but a message that names no failure", body: `{"error":{"message":"temperature must be between 0 and 2"}}`, want: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := IsCacheableUpstreamError(400, []byte(testCase.body)); got != testCase.want {
				t.Errorf("IsCacheableUpstreamError(400, %s) = %v, want %v", testCase.body, got, testCase.want)
			}
		})
	}
}

func TestIsCacheableUpstreamError_EveryMarkerExcludes(t *testing.T) {
	for _, marker := range momentaryFailureMessages {
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

func TestIsCacheableResponseCoversSuccessesAndSSEEmbeddedFailures(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
		want   bool
	}{
		{"a plain success is cacheable", 200, `{"choices":[{"message":{"content":"hi"},"finish_reason":"stop"}]}`, true},
		{"a 204 is cacheable", 204, `{"choices":[]}`, true},
		{"an empty body is never cacheable", 200, ``, false},
		{"a completed sse stream is cacheable", 200, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", true},
		{"a success carrying a transient error inside an sse event is not cacheable", 200, "data: {\"choices\":[]}\n\ndata: {\"error\":{\"message\":\"upstream timeout\"}}\n\n", false},
		{"a success carrying a deterministic error inside an sse event is still not a success", 200, "data: {\"error\":{\"message\":\"temperature must be between 0 and 2\"}}\n\n", true},
		{"a 400 carrying a deterministic error inside an sse event is cacheable", 400, "data: {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"unknown parameter foo\"}}\n\n", true},
		{"a 400 carrying a transient error inside an sse event is not cacheable", 400, "data: {\"error\":{\"message\":\"service unavailable\"}}\n\n", false},
		{"a 500 is not cacheable", 500, `{"choices":[]}`, false},
		{"crlf sse framing is scanned too", 200, "data: {\"error\":{\"message\":\"rate limit exceeded\"}}\r\n\r\n", false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := IsCacheableResponse(testCase.status, []byte(testCase.body)); got != testCase.want {
				t.Errorf("IsCacheableResponse(%d, %q) = %v, want %v", testCase.status, testCase.body, got, testCase.want)
			}
		})
	}
}

func TestHasNonCacheableErrorFindsFailuresRegardlessOfFraming(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{"a clean completion carries none", `{"choices":[{"message":{"content":"hi"}}]}`, false},
		{"a plain transient error", `{"error":{"message":"upstream request timeout"}}`, true},
		{"a plain deterministic error is replayable", `{"error":{"message":"temperature must be between 0 and 2"}}`, false},
		{"an sse-embedded transient error", "data: {\"choices\":[]}\n\ndata: {\"error\":{\"message\":\"overloaded\"}}\n\n", true},
		{"an sse stream with no error", "data: {\"choices\":[]}\n\ndata: [DONE]\n\n", false},
		{"a malformed body carries none", `not json`, false},
		{"a failure a host hid behind wrongly typed choices, framed", "data: {\"error\":{\"message\":\"service unavailable\"},\"choices\":\"none\"}\n\n", true},
		{"the first failure is the one kept", "data: {\"error\":{\"message\":\"temperature must be between 0 and 2\"}}\n\ndata: {\"error\":{\"message\":\"overloaded\"}}\n\n", false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := HasNonCacheableError([]byte(testCase.body)); got != testCase.want {
				t.Errorf("HasNonCacheableError(%q) = %v, want %v", testCase.body, got, testCase.want)
			}
		})
	}
}

// The strip must not rewrite numbers it was not asked to touch. Decoding into any turns every number
// into a float64, and seed is the one field a client uses to make a completion reproducible: handing
// back a different seed than the host reported breaks exactly the guarantee seed exists for.
func TestStripKeepsIntegersTooLargeForFloat64(t *testing.T) {
	body := []byte(`{"id":"c","seed":9007199254740993,"logprobs":{"content":[]},"choices":[]}`)

	stripped := string(stripResponseBody(body, LogprobIntent{}))

	if !strings.Contains(stripped, `"seed":9007199254740993`) {
		t.Fatalf("seed was rewritten: %s", stripped)
	}
	if strings.Contains(stripped, "logprobs") {
		t.Fatalf("the strip did not run at all, so the seed was never at risk: %s", stripped)
	}
}

// A body with a second value after the first is malformed and must pass through untouched, the way
// json.Unmarshal treated it. A Decoder alone would read the first value and drop the rest.
func TestStripLeavesABodyWithTrailingJunkAlone(t *testing.T) {
	body := []byte(`{"logprobs":{"content":[]}} {"and":"more"}`)

	if got := string(stripResponseBody(body, LogprobIntent{})); got != string(body) {
		t.Fatalf("a malformed body was rewritten:\n got %s\nwant %s", got, body)
	}
}

// A host controls the response body, so nesting is an input it chooses. A recursive strip with no depth
// bound answers that with a stack overflow, which runtime.throw makes uncatchable: the process dies and
// takes every in-flight race and pending settlement with it. The decoder's own limit is what prevents
// it, and this pins that the limit is still there.
func TestADeeplyNestedResponseCannotCrashTheProcess(t *testing.T) {
	for _, depth := range []int{1_000, 1_000_000, 3_000_000} {
		body := []byte(`{"logprobs":1,"a":` + strings.Repeat("[", depth) + strings.Repeat("]", depth) + `}`)

		if got := stripResponseBody(body, LogprobIntent{}); len(got) == 0 {
			t.Fatalf("depth %d produced nothing", depth)
		}
	}
}

// Past the decoder's depth limit the body is unparseable, so it goes to the client whole -- internal
// fields included. The gateway that this replaces behaves the same way, and the alternative is dropping
// a reply a client is waiting for, but a host wanting its logprobs seen can nest its way there.
func TestPastTheDepthLimitTheStripIsBypassed(t *testing.T) {
	shallow := []byte(`{"logprobs":1,"a":[[]]}`)
	if strings.Contains(string(stripResponseBody(shallow, LogprobIntent{})), "logprobs") {
		t.Fatal("a shallow body kept its internal field")
	}

	deep := []byte(`{"logprobs":1,"a":` + strings.Repeat("[", 100_000) + strings.Repeat("]", 100_000) + `}`)

	if !strings.Contains(string(stripResponseBody(deep, LogprobIntent{})), "logprobs") {
		t.Fatal("the deep body was stripped after all -- then this test, and the bypass it documents, are stale")
	}
}

// A number past float64 range is well-formed JSON a client parses fine. Treating it as unparseable
// forwards the whole body, internal fields and all -- the strip failing open on input the host chooses.
func TestAnOutOfRangeNumberDoesNotDisableTheStrip(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"hi"},"logprobs":{"a":1}}],"seed":1e999}`)

	stripped := string(stripResponseBody(body, LogprobIntent{}))

	if strings.Contains(stripped, "logprobs") {
		t.Fatalf("one out-of-range number turned the strip off: %s", stripped)
	}
	if !strings.Contains(stripped, "1e999") {
		t.Fatalf("the number was rewritten rather than passed through: %s", stripped)
	}
}

// The host's own bytes reach the client: the encoder's default would inflate every < > & in generated
// content to a six-byte escape, which is the same string to a decoder and a much larger one on the wire.
func TestGeneratedMarkupIsNotEscaped(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"<div> a & b"}}],"logprobs":{}}`)

	if got := string(stripResponseBody(body, LogprobIntent{})); !strings.Contains(got, "<div> a & b") {
		t.Fatalf("markup was escaped on the way out: %s", got)
	}
}

// The gateway forces logprobs on upstream for validation whatever the client sent, so the response
// strip is the only place that can tell a client who asked for them from one who did not. Stripping
// both alike takes from a paying client exactly what it asked for.
func TestTheStripFollowsWhatTheClientAskedFor(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"hi"},"logprobs":{"content":[{"token":"hi","logprob":-0.5,"top_logprobs":[{"token":"hello","logprob":-1.5}]}]}}],"prompt_logprobs":[1],"token_ids":[7]}`)

	testCases := []struct {
		name          string
		intent        LogprobIntent
		wantLogprobs  bool
		wantTopFilled bool
	}{
		{name: "asked_for_neither", intent: LogprobIntent{}},
		{name: "asked_for_logprobs_only", intent: LogprobIntent{Keep: true}, wantLogprobs: true},
		{name: "asked_for_alternatives_too", intent: LogprobIntent{Keep: true, KeepTop: true}, wantLogprobs: true, wantTopFilled: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			stripped := string(stripResponseBody(body, testCase.intent))

			if held := strings.Contains(stripped, `"logprob"`); held != testCase.wantLogprobs {
				t.Fatalf("logprob present = %v, want %v: %s", held, testCase.wantLogprobs, stripped)
			}
			if filled := strings.Contains(stripped, `"hello"`); filled != testCase.wantTopFilled {
				t.Fatalf("alternatives present = %v, want %v: %s", filled, testCase.wantTopFilled, stripped)
			}
			if testCase.wantLogprobs && !testCase.wantTopFilled && !strings.Contains(stripped, `"top_logprobs":[]`) {
				t.Fatalf("top_logprobs must stay present and empty, which is the shape a client without alternatives expects: %s", stripped)
			}
			// Internals are never anyone's to ask for.
			for _, internal := range []string{"prompt_logprobs", "token_ids"} {
				if strings.Contains(stripped, internal) {
					t.Fatalf("%s reached the client: %s", internal, stripped)
				}
			}
		})
	}
}

// The force rules overwrite logprobs at StagePostLimits, so reading the intent afterwards would
// record what the gateway wants for validation rather than what the client sent.
func TestTheLogprobIntentIsReadBeforeTheForceRules(t *testing.T) {
	testCases := []struct {
		name       string
		body       string
		wantIntent LogprobIntent
	}{
		{name: "asked_for_neither", body: `{"model":"qwen","messages":[{"role":"user","content":"hi"}]}`},
		{name: "asked_for_logprobs", body: `{"model":"qwen","messages":[{"role":"user","content":"hi"}],"logprobs":true}`, wantIntent: LogprobIntent{Keep: true}},
		{name: "asked_for_alternatives", body: `{"model":"qwen","messages":[{"role":"user","content":"hi"}],"logprobs":true,"top_logprobs":3}`, wantIntent: LogprobIntent{Keep: true, KeepTop: true}},
		{name: "alternatives_without_logprobs", body: `{"model":"qwen","messages":[{"role":"user","content":"hi"}],"top_logprobs":3}`},
		{name: "logprobs_of_the_wrong_type", body: `{"model":"qwen","messages":[{"role":"user","content":"hi"}],"logprobs":"yes"}`},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, err := NormalizeRequest([]byte(testCase.body), Options{RoutedModel: "qwen"})
			if err != nil {
				t.Fatalf("NormalizeRequest: %v", err)
			}
			if result.Logprobs != testCase.wantIntent {
				t.Fatalf("intent = %+v, want %+v", result.Logprobs, testCase.wantIntent)
			}
			if !strings.Contains(string(result.Body), `"logprobs":true`) {
				t.Fatalf("the request that goes upstream must carry forced logprobs whatever the client asked: %s", result.Body)
			}
		})
	}
}

// The two field sets are one list split in two, and the split is what decides whether a field can
// ever reach a client. A field in neither set is stripped from nobody; a requestable field missing
// from the full list is stripped from everybody.
func TestTheStripSetsPartitionTheFullList(t *testing.T) {
	for _, field := range requestableFields {
		if !slices.Contains(clientStrippedFields, field) {
			t.Fatalf("%q is requestable but not in the strip list, so a client that asked for nothing still sees it", field)
		}
	}
	for _, field := range clientStrippedFields {
		requestable := slices.Contains(requestableFields, field)
		always := slices.Contains(alwaysStrippedFields, field)
		if requestable == always {
			t.Fatalf("%q is in %s, want exactly one of requestable and always-stripped",
				field, map[bool]string{true: "both sets", false: "neither set"}[requestable])
		}
	}
}

// Every field in the list must actually leave a response that carries it; the strip decides on the
// decoded payload, so this is the test that a field added to the list is reachable by the delete.
func TestEveryStrippedFieldIsRemoved(t *testing.T) {
	for _, field := range clientStrippedFields {
		body := []byte(`{"choices":[{"index":0,"` + field + `":{"content":[]}}]}`)

		stripped := stripResponseBody(body, LogprobIntent{})

		if bytes.Contains(stripped, []byte(field)) {
			t.Fatalf("field %q survived the strip: %s", field, stripped)
		}
	}
}

// A backend writes NaN and Infinity as barewords for a probability of zero, and neither is JSON. Left
// alone the body is inspected by nobody: the buffered path forwards it with every internal field in
// it, and the streaming path drops the event, taking the client's answer with it.
func TestABodyCarryingNonFiniteNumbersIsStillStrippedAndDelivered(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"hi"},"logprobs":{"content":[{"logprob":-Infinity}]}}],"token_ids":[7],"prompt_logprobs":[1]}`)

	stripped := stripResponseBody(body, LogprobIntent{})

	for _, internal := range []string{"token_ids", "prompt_logprobs", "logprob"} {
		if bytes.Contains(stripped, []byte(internal)) {
			t.Fatalf("%s reached the client: %s", internal, stripped)
		}
	}
	if !bytes.Contains(stripped, []byte(`"content":"hi"`)) {
		t.Fatalf("the client's answer was lost: %s", stripped)
	}
	var decoded any
	if err := json.Unmarshal(stripped, &decoded); err != nil {
		t.Fatalf("the delivered body is not JSON: %v (%s)", err, stripped)
	}
}

// A bareword inside a string is content, not a number. The body must also fail to parse on its own,
// or normalisation never runs and the test proves nothing about what it leaves alone.
func TestNonFiniteWordsInsideStringsAreLeftAlone(t *testing.T) {
	body := []byte(`{"choices":[{"message":{"content":"the value is NaN, or -Infinity"},"logprobs":{"content":[{"logprob":NaN}]}}],"token_ids":[7]}`)
	var probe any
	if json.Unmarshal(body, &probe) == nil {
		t.Fatal("the body parses on its own, so normalisation never runs and this test is vacuous")
	}

	stripped := stripResponseBody(body, LogprobIntent{})

	if !bytes.Contains(stripped, []byte(`the value is NaN, or -Infinity`)) {
		t.Fatalf("content was rewritten: %s", stripped)
	}
	if bytes.Contains(stripped, []byte("token_ids")) {
		t.Fatalf("an internal field survived: %s", stripped)
	}
}

// A real error in a later event must still be found. An empty {"error":{}} decodes without carrying
// anything, and stopping on it would leave the stream readable as cacheable.
func TestAnEmptyErrorEventDoesNotStopTheScan(t *testing.T) {
	payload := []byte("data: {\"error\":{}}\n\n" + "data: {\"object\":\"error\",\"message\":\"service unavailable\"}\n\n")

	details, found := parseUpstreamErrorDetails(payload)

	if !found {
		t.Fatal("the scan stopped at the empty error and never saw the real one")
	}
	if details.Message != "service unavailable" {
		t.Fatalf("message = %q, want the later event's", details.Message)
	}
}

// The gateway appends its own [DONE] when a host sends none, so the terminator cannot say whether the
// model finished. A reply that stopped mid-answer is served and must never be replayed from the cache.
func TestAnAnswerThatNeverFinishedIsNotCacheable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "a choice that never finished",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"reasoning\":\"still working\"}}]}\n\ndata: [DONE]\n\n",
		},
		{
			name: "a choice finished by a later event",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\n" +
				"data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n",
			want: true,
		},
		{
			name: "a terminal reason a later event does not name again",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
				"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"trailing\"}}]}\n\ndata: [DONE]\n\n",
			want: true,
		},
		{
			name: "a null finish_reason is not terminal",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n",
		},
		{
			name: "an empty finish_reason is not terminal",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"\"}]}\n\ndata: [DONE]\n\n",
		},
		{
			name: "a second choice that never finished",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"a\"},\"finish_reason\":\"stop\"},{\"index\":1,\"delta\":{\"content\":\"b\"}}]}\n\n",
		},
		{
			name: "an unindexed second choice is not vouched for by its sibling",
			body: "data: {\"choices\":[{\"delta\":{\"content\":\"a\"},\"finish_reason\":\"stop\"},{\"delta\":{\"content\":\"b\"}}]}\n\n",
		},
		{
			name: "the usage-only event a stream ends with starts no choice",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n" +
				"data: {\"choices\":[],\"usage\":{\"completion_tokens\":7}}\n\ndata: [DONE]\n\n",
			want: true,
		},
		{
			name: "a completion terminated by stop_reason alone, as the chunk conversion reads it",
			body: `{"object":"chat.completion","choices":[{"index":0,"message":{"content":"ok"},"finish_reason":null,"stop_reason":128009}]}`,
			want: true,
		},
		{
			name: "a length cut is an ending like any other",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n",
			want: true,
		},
		{
			name: "a choice a host spelled with a capital, still unfinished",
			body: `{"object":"chat.completion","Choices":[{"index":0,"message":{"content":"half"},"finish_reason":null}]}`,
		},
		{
			name: "a folded completion that never finished",
			body: `{"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":null}]}`,
		},
		{
			name: "a folded completion that finished",
			body: `{"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}]}`,
			want: true,
		},
		{
			name: "an unframed completion a host sent instead of a stream",
			body: `{"object":"chat.completion","choices":[{"index":0,"message":{"content":"hi"}}]}`,
		},
		{
			name: "a choice split across two data lines is one object, as a client reads it",
			body: "data: {\"choices\":[{\"index\":0,\ndata: \"delta\":{\"content\":\"half\"}}]}\n\n",
		},
		{
			name: "a finished choice split across two data lines",
			body: "data: {\"choices\":[{\"index\":0,\ndata: \"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			want: true,
		},
		{
			name: "a transient failure a host hid behind a choices field of the wrong type",
			body: `{"error":{"message":"service unavailable"},"choices":"none"}`,
		},
		{
			name: "an error object carrying nothing is not replayable either",
			body: `{"error":{}}`,
		},
		{
			name: "an event cut off mid-object",
			body: "data: {\"choices\":[{\"index\":0,\"delta\":{\"cont\n\n",
		},
		{
			name: "an index spelled as a string falls back to where the choice stands",
			body: `{"choices":[{"index":"0","message":{"content":"a"},"finish_reason":"stop"},{"index":"1","message":{"content":"b"}}]}`,
		},
		{
			name: "an index past what an int64 holds falls back the same way",
			body: `{"choices":[{"index":1e30,"message":{"content":"a"},"finish_reason":"stop"},{"index":1e30,"message":{"content":"b"}}]}`,
		},
		{
			name: "an empty error event is one event among many, and stops nothing",
			body: "data: {\"error\":{}}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
			want: true,
		},
		{
			name: "a body that is neither JSON nor events",
			body: `<html>upstream is down</html>`,
		},
		{
			name: "an unframed completion cut off mid-object",
			body: `{"id":"c","object":"chat.completion","choices":[{"index":0,"message":{"content":"half`,
		},
		{
			name: "a reply that started no choice at all",
			body: `{"id":"resp","choices":[]}`,
			want: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := IsCacheableResponse(200, []byte(testCase.body)); got != testCase.want {
				t.Errorf("IsCacheableResponse(200, %q) = %v, want %v", testCase.body, got, testCase.want)
			}
		})
	}
}

// The rule is only worth having if a real answer still earns its entry, so every recorded host stream is
// classified here and a fixture added to testdata/sse fails until someone says which side it belongs on.
func TestEveryRecordedStreamIsClassified(t *testing.T) {
	t.Parallel()
	storable := map[string]bool{
		"comment_and_blank_lines.sse":   false, // its one choice carries finish_reason null
		"completion_wrapped_stream.sse": true,
		"content_stream.sse":            true,
		"kimi_thinking_stream.sse":      true,
		"logprobs_stream.sse":           true,
		"malformed_data_line.sse":       false, // an event nobody can read
		"newlineless_final_content.sse": false, // ends on content, never on a reason
		"newlineless_final_error.sse":   false, // type server_error: the host named a failure of the moment
		"token_ids_stream.sse":          true,
		"tool_calls_stream.sse":         true,
	}
	entries, err := os.ReadDir(filepath.Join("testdata", "sse"))
	if err != nil {
		t.Fatalf("reading fixtures: %v", err)
	}
	entries = slices.DeleteFunc(entries, func(entry os.DirEntry) bool { return entry.IsDir() })
	if len(entries) != len(storable) {
		t.Fatalf("%d fixtures against %d classified: the list and the directory must say the same", len(entries), len(storable))
	}
	for _, entry := range entries {
		want, classified := storable[entry.Name()]
		if !classified {
			t.Fatalf("%s is unclassified: say whether the cache must store it", entry.Name())
		}
		t.Run(entry.Name(), func(t *testing.T) {
			if got := IsCacheableResponse(200, readSSEFixture(t, entry.Name())); got != want {
				t.Fatalf("IsCacheableResponse = %v, want %v", got, want)
			}
		})
	}
}

// The refusal is not just a boolean: routes.go branches on the name, and operations.md documents it.
func TestCacheRefusalNamesWhyItRefused(t *testing.T) {
	t.Parallel()
	finished := `{"choices":[{"index":0,"message":{"content":"hi"},"finish_reason":"stop"}]}`
	tests := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{name: "a finished success", status: 200, body: finished, want: CacheStorable},
		{name: "no body at all", status: 200, body: "", want: CacheRefusedEmptyBody},
		{name: "a transient failure", status: 200, body: `{"error":{"message":"service unavailable"}}`, want: CacheRefusedFailure},
		{name: "an event nobody can read", status: 200, body: "data: {\"choices\":[{\"index\":0,\n\n", want: CacheRefusedUnreadable},
		{name: "a body nothing can read", status: 200, body: "<html>upstream is down</html>", want: CacheRefusedUnreadable},
		{name: "choices no client could render", status: 200, body: `{"choices":"none"}`, want: CacheRefusedUnreadable},
		{name: "the same, inside an event", status: 200, body: "data: {\"choices\":\"none\"}\n\n", want: CacheRefusedUnreadable},
		{name: "an answer that stopped mid-answer", status: 200, body: `{"choices":[{"index":0,"message":{"content":"half"}}]}`, want: CacheRefusedUnfinished},
		{name: "a status no replay may carry", status: 500, body: finished, want: CacheRefusedStatus},
		{name: "a status refused before the answer is judged", status: 500, body: `{"choices":[{"index":0,"message":{"content":"half"}}]}`, want: CacheRefusedStatus},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := CacheRefusal(testCase.status, []byte(testCase.body)); got != testCase.want {
				t.Errorf("CacheRefusal(%d, %q) = %q, want %q", testCase.status, testCase.body, got, testCase.want)
			}
		})
	}
}

// A host naming more choices than the fold would ever merge is not one to replay, however it ends them.
func TestAFloodOfChoicesIsNotStored(t *testing.T) {
	t.Parallel()
	for _, choices := range []int{maxIndexedElements, maxIndexedElements + 1} {
		t.Run(fmt.Sprint(choices), func(t *testing.T) {
			var body strings.Builder
			body.WriteString(`{"choices":[`)
			for index := range choices {
				if index > 0 {
					body.WriteString(",")
				}
				fmt.Fprintf(&body, `{"index":%d,"message":{"content":"x"},"finish_reason":"stop"}`, index)
			}
			body.WriteString(`]}`)

			want := choices <= maxIndexedElements
			if got := IsCacheableResponse(200, []byte(body.String())); got != want {
				t.Fatalf("IsCacheableResponse over %d choices = %v, want %v", choices, got, want)
			}
		})
	}
}

// The strip rewrites a host's NaN/Infinity barewords into null on the way out, so what the cache is handed
// is decodable. If that ever stops holding, the terminal chunk becomes unreadable and nothing is cached.
func TestAStreamCarryingBarewordsStaysCacheableAfterTheStrip(t *testing.T) {
	t.Parallel()
	event := []byte(`data: {"choices":[{"index":0,"delta":{"content":"ok"},` +
		`"logprobs":{"content":[{"token":"ok","logprob":-Infinity}]},"finish_reason":"stop"}]}` + "\n\n")

	rewritten, err := NewStreamRewriter(LogprobIntent{Keep: true, KeepTop: true}, true).Write(event)
	if err != nil {
		t.Fatalf("Write(): %v", err)
	}

	if bytes.Contains(rewritten, []byte("Infinity")) {
		t.Fatalf("a bareword no decoder reads reached the cache: %s", rewritten)
	}
	if got := CacheRefusal(200, rewritten); got != CacheStorable {
		t.Fatalf("CacheRefusal = %q, want the reply stored", got)
	}
}
