package engine

import (
	"bytes"
	"context"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"devshard/cmd/gateway/filters"
	"devshard/transport"
)

const (
	kimiModel = "moonshotai/Kimi-K2.6"
	qwenModel = "Qwen/Qwen3-235B-A22B-Instruct-2507"
)

func TestContentSource(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		body           string
		thinkingBudget bool
		wantSource     string
	}{
		{name: "empty", body: ""},
		{name: "done_only", body: "data: [DONE]\n\n"},
		{name: "role_chunk_only", body: `data: {"id":"a","choices":[{"delta":{"role":"assistant"},"index":0,"finish_reason":null}]}` + "\n\n"},
		{name: "finish_only", body: `data: {"id":"a","choices":[{"delta":{},"index":0,"finish_reason":"stop"}]}` + "\n\n"},
		{
			name:           "empty_stop_with_completion_tokens_on_a_thinking_budget_route",
			body:           `data: {"id":"a","choices":[{"message":{"content":""},"index":0,"finish_reason":"stop"}],"usage":{"completion_tokens":1}}` + "\n\n",
			thinkingBudget: true,
			wantSource:     "message.empty_stop_completion_tokens",
		},
		{
			name: "empty_stop_with_completion_tokens_off_a_thinking_budget_route",
			body: `data: {"id":"a","choices":[{"message":{"content":""},"index":0,"finish_reason":"stop"}],"usage":{"completion_tokens":1}}` + "\n\n",
		},
		{
			name:           "empty_stop_without_completion_tokens",
			body:           `data: {"id":"a","choices":[{"message":{"content":""},"index":0,"finish_reason":"stop"}],"usage":{"completion_tokens":0}}` + "\n\n",
			thinkingBudget: true,
		},
		{
			name:       "delta_content",
			body:       `data: {"id":"a","choices":[{"delta":{"content":"Hello"},"index":0}]}` + "\n\n",
			wantSource: "delta.content",
		},
		{
			name:       "delta_reasoning_content",
			body:       `data: {"id":"a","choices":[{"delta":{"reasoning_content":"hmm"},"index":0}]}` + "\n\n",
			wantSource: "delta.reasoning_content",
		},
		{
			name:       "delta_reasoning",
			body:       `data: {"id":"a","choices":[{"delta":{"reasoning":"hmm"},"index":0}]}` + "\n\n",
			wantSource: "delta.reasoning",
		},
		{
			name:       "delta_tool_calls",
			body:       `data: {"id":"a","choices":[{"delta":{"tool_calls":[{"id":"x"}]},"index":0}]}` + "\n\n",
			wantSource: "delta.tool_calls",
		},
		{
			name: "delta_tool_calls_empty_array",
			body: `data: {"id":"a","choices":[{"delta":{"tool_calls":[]},"index":0}]}` + "\n\n",
		},
		{
			name:       "delta_content_precedence_over_reasoning",
			body:       `data: {"choices":[{"delta":{"content":"hi","reasoning_content":"think"}}]}` + "\n\n",
			wantSource: "delta.content",
		},
		{
			name:       "message_content_convertible",
			body:       `data: {"choices":[{"message":{"content":"stub"}}],"usage":{}}` + "\n\n",
			wantSource: "message.content",
		},
		{
			name:       "message_reasoning_convertible",
			body:       `data: {"choices":[{"message":{"reasoning":"stub"}}]}` + "\n\n",
			wantSource: "message.reasoning",
		},
		{
			name:       "message_reasoning_content_convertible",
			body:       `data: {"choices":[{"message":{"reasoning_content":"stub"}}]}` + "\n\n",
			wantSource: "message.reasoning_content",
		},
		{
			name:       "message_tool_calls_convertible",
			body:       `data: {"choices":[{"message":{"tool_calls":[{"id":"x"}]}}]}` + "\n\n",
			wantSource: "message.tool_calls",
		},
		{
			name: "message_tool_calls_empty_array",
			body: `data: {"choices":[{"message":{"tool_calls":[]}}]}` + "\n\n",
		},
		{
			name: "completion_text_field_rejected",
			body: `data: {"choices":[{"text":"abc"}]}` + "\n\n",
		},
		{
			name:       "role_then_content_in_one_write",
			body:       `data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n" + `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
			wantSource: "delta.content",
		},
		{name: "malformed_json", body: "data: {not_json}\n\n"},
		{
			name: "openai_error_not_content",
			body: `data: {"error":{"code":400,"message":"bad request","type":"BadRequestError"}}` + "\n\n",
		},
		{name: "non_data_lines_ignored", body: "event: ping\nid: 42\n\n"},
		{name: "delta_empty_string", body: `data: {"choices":[{"delta":{"content":""}}]}` + "\n\n"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			source, ok := contentSource([]byte(testCase.body), testCase.thinkingBudget)
			if source != testCase.wantSource {
				t.Errorf("contentSource = %q, want %q", source, testCase.wantSource)
			}
			if ok != (testCase.wantSource != "") {
				t.Errorf("contentSource ok = %v, want %v", ok, testCase.wantSource != "")
			}
		})
	}
}

func TestErrorPayload(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		body        string
		wantSource  string
		wantCode    string
		wantType    string
		wantMessage string
	}{
		{
			name:        "openai_error_with_type",
			body:        `data: {"error":{"code":400,"message":"bad request","type":"BadRequestError"}}` + "\n\n",
			wantSource:  "error.BadRequestError",
			wantCode:    "400",
			wantType:    "BadRequestError",
			wantMessage: "bad request",
		},
		{
			name:        "flat_openai_error_with_type",
			body:        `data: {"code":400,"object":"error","message":"context too long","type":"BadRequestError"}` + "\n\n",
			wantSource:  "error.BadRequestError",
			wantCode:    "400",
			wantType:    "BadRequestError",
			wantMessage: "context too long",
		},
		{
			name:        "openai_error_without_type",
			body:        `data: {"error":{"code":500,"message":"backend failed"}}` + "\n\n",
			wantSource:  "error",
			wantCode:    "500",
			wantMessage: "backend failed",
		},
		{
			name:        "null_code_is_empty_not_nil_literal",
			body:        `data: {"error":{"code":null,"message":"boom","type":"server_error"}}` + "\n\n",
			wantSource:  "error.server_error",
			wantType:    "server_error",
			wantMessage: "boom",
		},
		{
			name:        "flat_error_null_code",
			body:        `data: {"object":"error","code":null,"message":"boom","type":"server_error"}` + "\n\n",
			wantSource:  "error.server_error",
			wantType:    "server_error",
			wantMessage: "boom",
		},
		{
			name:        "string_code",
			body:        `data: {"error":{"message":"boom","type":"server_error","code":"internal"}}` + "\n\n",
			wantSource:  "error.server_error",
			wantCode:    "internal",
			wantType:    "server_error",
			wantMessage: "boom",
		},
		{name: "done_only", body: "data: [DONE]\n\n"},
		{name: "content_not_error", body: `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n"},
		{name: "flat_shape_without_message", body: `data: {"object":"error","type":"BadRequestError"}` + "\n\n"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			failure, ok := errorPayload([]byte(testCase.body))
			if ok != (testCase.wantSource != "") {
				t.Fatalf("errorPayload ok = %v, want %v", ok, testCase.wantSource != "")
			}
			if failure.Source != testCase.wantSource {
				t.Errorf("Source = %q, want %q", failure.Source, testCase.wantSource)
			}
			if failure.Code != testCase.wantCode {
				t.Errorf("Code = %q, want %q", failure.Code, testCase.wantCode)
			}
			if failure.Type != testCase.wantType {
				t.Errorf("Type = %q, want %q", failure.Type, testCase.wantType)
			}
			if failure.Message != testCase.wantMessage {
				t.Errorf("Message = %q, want %q", failure.Message, testCase.wantMessage)
			}
		})
	}
}

func TestErrorPayloadCopiesTheEventBytes(t *testing.T) {
	t.Parallel()
	body := []byte(`data: {"error":{"message":"boom","type":"server_error"}}` + "\n\n")

	failure, ok := errorPayload(body)

	if !ok {
		t.Fatal("errorPayload found nothing")
	}
	want := `{"error":{"message":"boom","type":"server_error"}}`
	if failure.Payload != want {
		t.Fatalf("Payload = %q, want %q", failure.Payload, want)
	}
	for index := range body {
		body[index] = 'x'
	}
	if failure.Payload != want {
		t.Errorf("Payload aliases the caller's buffer: %q", failure.Payload)
	}
}

func TestUsageCompletionTokens(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		body       string
		wantTokens int64
	}{
		{
			name:       "usage_with_completion_tokens",
			body:       `data: {"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":100,"total_tokens":112}}` + "\n\n",
			wantTokens: 100,
		},
		{name: "usage_zero_completion_tokens", body: `data: {"usage":{"prompt_tokens":12,"completion_tokens":0,"total_tokens":12}}` + "\n\n"},
		{name: "usage_absent", body: `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n"},
		{name: "done_marker_only", body: "data: [DONE]\n\n"},
		{name: "empty_input", body: ""},
		{
			name:       "malformed_json_skipped",
			body:       "data: {not json}\n\ndata: {\"usage\":{\"completion_tokens\":42}}\n\n",
			wantTokens: 42,
		},
		{name: "usage_null", body: `data: {"choices":[],"usage":null}` + "\n\n"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			tokens, ok := usageCompletionTokens([]byte(testCase.body))
			if tokens != testCase.wantTokens {
				t.Errorf("tokens = %d, want %d", tokens, testCase.wantTokens)
			}
			if ok != (testCase.wantTokens > 0) {
				t.Errorf("ok = %v, want %v", ok, testCase.wantTokens > 0)
			}
		})
	}
}

func TestRetriableCapability(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name          string
		message       string
		wantRetriable bool
		wantLimit     uint64
	}{
		{name: "tool_choice_unsupported", message: filters.ToolChoiceUnsupportedMessage, wantRetriable: true},
		{
			name:          "context_length_with_trailing_text",
			message:       "This model's maximum context length is 131072 tokens. However, you requested 150000 tokens.",
			wantRetriable: true,
			wantLimit:     131072,
		},
		{
			name:          "context_length_at_end_of_message",
			message:       "This model's maximum context length is 131072",
			wantRetriable: true,
			wantLimit:     131072,
		},
		{
			name:          "marker_matched_case_insensitively",
			message:       "Maximum Context Length Is 4096 tokens",
			wantRetriable: true,
			wantLimit:     4096,
		},
		{name: "plain_model_error", message: "plain model error"},
		{name: "marker_without_digits", message: "maximum context length is unknown"},
		{name: "empty", message: ""},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := ParseCapabilityError(testCase.message).Retriable(); got != testCase.wantRetriable {
				t.Errorf("Retriable() = %v, want %v", got, testCase.wantRetriable)
			}
			if got := ParseCapabilityError(testCase.message).ContextLimit; got != testCase.wantLimit {
				t.Errorf("ContextLimit = %d, want %d", got, testCase.wantLimit)
			}
		})
	}
}

// replayStream drives one attempt's real classifier and accumulator over chunks exactly as
// attemptWriter and runAttempt do, and returns the state the terminal was decided from.
func replayStream(t *testing.T, model string, receipted bool, chunks ...[]byte) *attemptState {
	t.Helper()
	classifier := newTestClassifier(model, nil)
	defer classifier.Release()

	state := &attemptState{}
	if receipted {
		state.receiptTime = testEpoch
	}
	for _, chunk := range chunks {
		state.record(classifier.Classify(chunk))
	}
	state.classify(context.Background(), AttemptSpec{Model: model, Classifier: classifier}, nil)
	return state
}

func fixedChunks(body []byte, size int) [][]byte {
	chunks := make([][]byte, 0, len(body)/size+1)
	for start := 0; start < len(body); start += size {
		chunks = append(chunks, body[start:min(start+size, len(body))])
	}
	return chunks
}

// An error event is a chunk but never content: crowning on one would let a host answering instantly
// with an error beat a slower host that goes on to produce tokens. A capability refusal is neither,
// because another host can still serve the request it names.
func TestAccumulatorCountsErrorEventsWithoutCrowningThem(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name            string
		body            string
		wantSource      string
		wantChunks      int64
		wantErrorSource string
		wantTerminal    Terminal
	}{
		{
			name:            "server_error_counts_but_does_not_crown",
			body:            `data: {"error":{"code":500,"message":"backend failed","type":"server_error"}}` + "\n\n",
			wantChunks:      1,
			wantErrorSource: "error.server_error",
			wantTerminal:    TerminalErrorStream,
		},
		{
			name:            "tool_choice_refusal_neither_counts_nor_crowns",
			body:            `data: {"error":{"code":400,"message":"` + filters.ToolChoiceUnsupportedMessage + `","type":"BadRequestError"}}` + "\n\n",
			wantErrorSource: "error.BadRequestError",
			wantTerminal:    TerminalCapabilityRefused,
		},
		{
			name:            "context_length_refusal_neither_counts_nor_crowns",
			body:            `data: {"error":{"code":400,"message":"This model's maximum context length is 131072 tokens.","type":"BadRequestError"}}` + "\n\n",
			wantErrorSource: "error.BadRequestError",
			wantTerminal:    TerminalCapabilityRefused,
		},
		{
			name:         "content_crowns",
			body:         `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
			wantSource:   "delta.content",
			wantChunks:   1,
			wantTerminal: TerminalLost,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := replayStream(t, qwenModel, true, []byte(testCase.body))

			if state.contentSource != testCase.wantSource {
				t.Errorf("contentSource = %q, want %q", state.contentSource, testCase.wantSource)
			}
			if state.contentChunks != testCase.wantChunks {
				t.Errorf("contentChunks = %d, want %d", state.contentChunks, testCase.wantChunks)
			}
			if state.errorSource != testCase.wantErrorSource {
				t.Errorf("errorSource = %q, want %q", state.errorSource, testCase.wantErrorSource)
			}
			if state.terminal != testCase.wantTerminal {
				t.Errorf("terminal = %v, want %v", state.terminal, testCase.wantTerminal)
			}
		})
	}
}

// The whole terminal ladder over already-accumulated facts. An attempt with no receipt failed at the
// transport, so no reading of its stream may be charged to the host's answer.
func TestTerminalLadderOverAccumulatedFacts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		state attemptState
		want  Terminal
	}{
		{name: "never_receipted", want: TerminalNoReceipt},
		{name: "no_receipt_with_content", state: attemptState{contentChunks: 1}, want: TerminalNoReceipt},
		{name: "receipt_without_content", state: attemptState{receiptTime: testEpoch}, want: TerminalEmptyStream},
		{name: "receipt_with_content", state: attemptState{receiptTime: testEpoch, contentChunks: 1}, want: TerminalLost},
		{
			name:  "error_stream_is_never_empty",
			state: attemptState{receiptTime: testEpoch, errorSource: "error.server_error"},
			want:  TerminalErrorStream,
		},
		{
			name:  "capability_refusal_is_its_own_terminal",
			state: attemptState{receiptTime: testEpoch, errorSource: "error.BadRequestError", capabilityRefused: true},
			want:  TerminalCapabilityRefused,
		},
		{
			name:  "burned_tokens_separate_the_model_from_the_host",
			state: attemptState{receiptTime: testEpoch, usageCompletionTokens: 100, tokensBurned: true},
			want:  TerminalBurnEmpty,
		},
		{
			name:  "an_error_stream_is_never_burn_empty",
			state: attemptState{receiptTime: testEpoch, errorSource: "error.server_error", tokensBurned: true},
			want:  TerminalErrorStream,
		},
		{
			name:  "content_is_never_burn_empty",
			state: attemptState{receiptTime: testEpoch, contentChunks: 1, tokensBurned: true},
			want:  TerminalLost,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := testCase.state
			state.classify(context.Background(), AttemptSpec{Classifier: &fakeClassifier{}}, nil)

			if state.terminal != testCase.want {
				t.Errorf("terminal = %v, want %v", state.terminal, testCase.want)
			}
		})
	}
}

// completion_tokens is host-reported, so only a thinking-budget route may spend it to escape the
// empty-stream penalty; anywhere else a host could fake usage to do the same.
func TestBurnEmptyIsOnlyReachableOnAThinkingBudgetRoute(t *testing.T) {
	t.Parallel()
	const usageOnly = `data: {"choices":[{"delta":{},"finish_reason":"length"}],"usage":{"prompt_tokens":12,"completion_tokens":100,"total_tokens":112}}` + "\n\n"
	cases := []struct {
		name  string
		model string
		want  Terminal
	}{
		{name: "thinking_budget_route_burned_the_tokens", model: kimiModel, want: TerminalBurnEmpty},
		{name: "any_other_route_answered_with_nothing", model: qwenModel, want: TerminalEmptyStream},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := replayStream(t, testCase.model, true, []byte(usageOnly))

			if state.usageCompletionTokens != 100 {
				t.Errorf("usageCompletionTokens = %d, want 100", state.usageCompletionTokens)
			}
			if state.contentChunks != 0 {
				t.Errorf("contentChunks = %d, want 0: a usage-only chunk carries no content", state.contentChunks)
			}
			if state.terminal != testCase.want {
				t.Errorf("terminal = %v, want %v", state.terminal, testCase.want)
			}
		})
	}
}

func TestFirstContentAndFirstErrorAreTheOnesRecorded(t *testing.T) {
	t.Parallel()
	state := replayStream(t, qwenModel, true,
		[]byte(`data: {"choices":[{"delta":{"reasoning":"think"}}]}`+"\n\n"),
		[]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}`+"\n\n"),
		[]byte(`data: {"error":{"message":"first","type":"server_error"}}`+"\n\n"),
		[]byte(`data: {"error":{"message":"second","type":"other_error"}}`+"\n\n"),
	)

	if state.contentSource != "delta.reasoning" {
		t.Errorf("contentSource = %q, want %q", state.contentSource, "delta.reasoning")
	}
	if state.errorSource != "error.server_error" || state.errorMessage != "first" {
		t.Errorf("error = %q/%q, want error.server_error/first", state.errorSource, state.errorMessage)
	}
	if state.errorPayload != `{"error":{"message":"first","type":"server_error"}}` {
		t.Errorf("errorPayload = %q, want the first error event verbatim", state.errorPayload)
	}
	if state.contentChunks != 4 {
		t.Errorf("contentChunks = %d, want 4", state.contentChunks)
	}
}

type fixtureExpectation struct {
	file            string
	wantSource      string
	wantErrorSource string
	wantUsage       int64
	newlineLess     bool
}

var sseFixtures = []fixtureExpectation{
	{file: "content_stream.sse", wantSource: "delta.content", wantUsage: 2},
	{file: "tool_calls_stream.sse", wantSource: "delta.tool_calls", wantUsage: 20},
	{file: "newlineless_final_content.sse", wantSource: "delta.content", newlineLess: true},
	{file: "newlineless_final_error.sse", wantErrorSource: "error.server_error", newlineLess: true},
}

func readFixture(t *testing.T, file string) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "filters", "testdata", "sse", file))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	if bytes.HasSuffix(body, []byte("\n")) == fixtureIsNewlineLess(file) {
		t.Fatalf("fixture %s no longer matches its recorded line ending", file)
	}
	return body
}

func fixtureIsNewlineLess(file string) bool {
	for _, fixture := range sseFixtures {
		if fixture.file == file {
			return fixture.newlineLess
		}
	}
	return false
}

func assertFixtureState(t *testing.T, fixture fixtureExpectation, state *attemptState) {
	t.Helper()
	if state.contentSource != fixture.wantSource {
		t.Errorf("contentSource = %q, want %q", state.contentSource, fixture.wantSource)
	}
	if state.errorSource != fixture.wantErrorSource {
		t.Errorf("errorSource = %q, want %q", state.errorSource, fixture.wantErrorSource)
	}
	if state.usageCompletionTokens != fixture.wantUsage {
		t.Errorf("usageCompletionTokens = %d, want %d", state.usageCompletionTokens, fixture.wantUsage)
	}
	if state.terminal == TerminalEmptyStream || state.terminal == TerminalBurnEmpty {
		t.Errorf("terminal = %v, want a stream that carried something", state.terminal)
	}
}

func TestFixtureStreamsClassifyWholeBlob(t *testing.T) {
	t.Parallel()
	for _, fixture := range sseFixtures {
		t.Run(fixture.file, func(t *testing.T) {
			t.Parallel()
			assertFixtureState(t, fixture, replayStream(t, qwenModel, true, readFixture(t, fixture.file)))
		})
	}
}

func TestFixtureStreamsClassifyIdenticallyUnderRandomChunking(t *testing.T) {
	t.Parallel()
	for fixtureIndex, fixture := range sseFixtures {
		t.Run(fixture.file, func(t *testing.T) {
			t.Parallel()
			body := readFixture(t, fixture.file)
			randomness := rand.New(rand.NewPCG(42, uint64(fixtureIndex)))
			chunks := make([][]byte, 0, len(body))
			for start := 0; start < len(body); {
				end := min(start+1+randomness.IntN(64), len(body))
				chunks = append(chunks, body[start:end])
				start = end
			}

			assertFixtureState(t, fixture, replayStream(t, qwenModel, true, chunks...))
		})
	}
}

func TestNewlineLessFinalEventLooksEmptyUntilTheStreamCloses(t *testing.T) {
	t.Parallel()
	body := readFixture(t, "newlineless_final_content.sse")
	classifier := newTestClassifier(qwenModel, nil)
	defer classifier.Release()

	state := &attemptState{receiptTime: testEpoch}
	state.record(classifier.Classify(body))

	if state.contentChunks != 0 {
		t.Fatalf("contentChunks = %d, want 0: an unflushed final event carries nothing yet", state.contentChunks)
	}

	state.classify(context.Background(), AttemptSpec{Model: qwenModel, Classifier: classifier}, nil)

	if state.terminal != TerminalLost {
		t.Errorf("terminal = %v, want TerminalLost from the flushed tail", state.terminal)
	}
	if state.contentSource != "delta.content" {
		t.Errorf("contentSource = %q, want %q", state.contentSource, "delta.content")
	}
}

func TestGarbageAndTruncatedInputClassifyWithoutPanicking(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		body string
	}{
		{name: "empty", body: ""},
		{name: "single_newline", body: "\n"},
		{name: "only_prefix", body: "data:"},
		{name: "prefix_with_open_brace", body: "data: {"},
		{name: "prefix_with_open_bracket", body: `data: {"choices":[`},
		{name: "truncated_tool_calls", body: `data: {"choices":[{"delta":{"tool_calls":[`},
		{name: "nul_bytes", body: "data: \x00\x00\x00\n\n"},
		{name: "choices_not_an_array", body: `data: {"choices":{"delta":{"content":"hi"}}}` + "\n\n"},
		{name: "error_not_an_object", body: `data: {"error":"boom"}` + "\n\n"},
		{name: "usage_not_an_object", body: `data: {"usage":7}` + "\n\n"},
		{name: "tool_calls_not_an_array", body: `data: {"choices":[{"delta":{"tool_calls":{"id":"x"}}}]}` + "\n\n"},
		{name: "deeply_quoted", body: `data: "just a string"` + "\n\n"},
		{name: "crlf_only", body: "data: [DONE]\r\n\r\n"},
		{name: "half_an_event_then_done", body: `data: {"choices":[{"delta"` + "\ndata: [DONE]\n\n"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := replayStream(t, qwenModel, true, []byte(testCase.body))

			if state.contentSource != "" {
				t.Errorf("contentSource = %q, want empty", state.contentSource)
			}
			if state.errorSource != "" {
				t.Errorf("errorSource = %q, want empty", state.errorSource)
			}
			if state.terminal != TerminalEmptyStream {
				t.Errorf("terminal = %v, want TerminalEmptyStream", state.terminal)
			}
		})
	}
}

func TestInvalidUTF8ContentStillCountsAsContent(t *testing.T) {
	t.Parallel()
	source, ok := contentSource([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"\xff\xfe\"}}]}\n\n"), false)

	if !ok || source != "delta.content" {
		t.Errorf("contentSource = %q/%v, want delta.content/true", source, ok)
	}
}

func TestClassifyIsPureOverItsInput(t *testing.T) {
	t.Parallel()
	body := []byte(`data: {"choices":[{"delta":{"content":"hi"}}],"usage":{"completion_tokens":5}}` + "\n\n")
	original := bytes.Clone(body)

	classifyChunk(body, true)

	if !bytes.Equal(body, original) {
		t.Errorf("classifyChunk mutated its input: %q", body)
	}
}

func TestTheClassifierReadsTheHostStampOnce(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name  string
		event string
		want  int64
	}{
		{
			name:  "a stamped chunk",
			event: `data: {"created":1786114584,"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
			want:  1786114584,
		},
		{
			name:  "a chunk with no stamp",
			event: `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
		},
		{
			name:  "a stamp of zero is no stamp",
			event: `data: {"created":0,"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
		},
		{
			name: "an unstamped event does not end the search",
			event: `data: {"created":0,"choices":[{"delta":{"content":"hi"}}]}` + "\n\n" +
				`data: {"created":1786114584,"choices":[{"delta":{"content":"there"}}]}` + "\n\n",
			want: 1786114584,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			classifier := newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), "participant", "model", nil)

			facts := classifier.Classify([]byte(testCase.event))

			if facts.Created != testCase.want {
				t.Fatalf("Created = %d, want %d", facts.Created, testCase.want)
			}
		})
	}
}

// The stamp is the one upstream put on the reply when it accepted the request, so a later chunk
// restating it must not overwrite what the first one carried.
func TestAnAttemptKeepsTheFirstHostStamp(t *testing.T) {
	t.Parallel()
	state := &attemptState{}

	state.record(chunkFacts{Content: true, ContentSource: "delta.content", Created: 1786114584})
	state.record(chunkFacts{Content: true, ContentSource: "delta.content", Created: 1786114999})

	if state.hostCreated != 1786114584 {
		t.Fatalf("hostCreated = %d, want the first stamp kept", state.hostCreated)
	}
}

// Every chunk restates the same stamp, so only the first may carry it: reading it again would put a
// decode on the path every chunk of every request takes.
func TestTheClassifierStopsReadingTheStampAfterTheFirstChunk(t *testing.T) {
	t.Parallel()
	classifier := newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), "participant", "model", nil)
	stamped := `data: {"created":1786114584,"choices":[{"delta":{"content":"hi"}}]}` + "\n\n"

	first := classifier.Classify([]byte(stamped))
	second := classifier.Classify([]byte(stamped))

	if first.Created != 1786114584 {
		t.Fatalf("first chunk Created = %d, want the stamp", first.Created)
	}
	if second.Created != 0 {
		t.Errorf("second chunk Created = %d, want the stamp read only once", second.Created)
	}
}

// A reply that arrives as one unterminated event carries its stamp only there, so Flush must read it.
func TestTheClassifierReadsTheStampFromAnUnterminatedTail(t *testing.T) {
	t.Parallel()
	classifier := newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), "participant", "model", nil)
	classifier.Classify([]byte(`data: {"created":1786114584,"choices":[{"delta":{"content":"hi"}}]}`))

	facts := classifier.Flush()

	if facts.Created != 1786114584 {
		t.Fatalf("Created = %d, want the stamp read from the tail", facts.Created)
	}
}

// classify is the only place a dispatch error is read, and a 404 carries no SSE error event, so this is
// the sole path by which a permanent refusal can reach the capability tracker.
func TestClassifyCarriesAVersionRefusalOffTheDispatchError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		body string
		want bool
	}{
		{name: "the host's build is too old", body: `version "v3" not found`, want: true},
		{name: "an escrow torn down mid-flight", body: `{"message":"get escrow: escrow not found"}`},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			state := attemptState{}
			refusal := &transport.UpstreamStatusError{
				Path: "/v1/chat/completions", StatusCode: http.StatusNotFound, Body: testCase.body,
			}

			state.classify(context.Background(), AttemptSpec{Classifier: &fakeClassifier{}}, refusal)

			if state.terminal != TerminalNotFound {
				t.Fatalf("terminal = %v, want %v", state.terminal, TerminalNotFound)
			}
			if got := state.capability.VersionUnsupported; got != testCase.want {
				t.Fatalf("capability.VersionUnsupported = %v, want %v", got, testCase.want)
			}
		})
	}
}
