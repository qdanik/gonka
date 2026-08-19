package filters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func sseStream(events ...string) []byte {
	var stream []byte
	for _, event := range events {
		stream = append(stream, "data: "...)
		stream = append(stream, event...)
		stream = append(stream, "\n\n"...)
	}
	return append(stream, "data: [DONE]\n\n"...)
}

func TestAssembleSSEBodyMergesDeltas(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		events []string
		want   string
	}{
		{
			name: "content accumulates into one message",
			events: []string{
				`{"id":"a","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"He"},"finish_reason":null}]}`,
				`{"id":"a","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"llo"},"finish_reason":null}]}`,
				`{"id":"a","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			},
			want: `{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"Hello","role":"assistant"}}],"id":"a","object":"chat.completion"}`,
		},
		{
			name: "the final usage chunk carries no choice of its own",
			events: []string{
				`{"id":"a","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}`,
				`{"id":"a","object":"chat.completion.chunk","choices":[],"usage":{"completion_tokens":2,"prompt_tokens":5,"total_tokens":7}}`,
			},
			want: `{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"hi"}}],"id":"a","object":"chat.completion","usage":{"completion_tokens":2,"prompt_tokens":5,"total_tokens":7}}`,
		},
		{
			name: "reasoning accumulates beside content",
			events: []string{
				`{"choices":[{"index":0,"delta":{"reasoning_content":"th"}}]}`,
				`{"choices":[{"index":0,"delta":{"reasoning_content":"ink","content":"out"}}]}`,
			},
			want: `{"choices":[{"index":0,"message":{"content":"out","reasoning_content":"think"}}],"object":"chat.completion"}`,
		},
		{
			name: "two tool calls keep their own arguments",
			events: []string{
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"lookup","arguments":"{\"a\":"}}]}}]}`,
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"c2","function":{"name":"send","arguments":"{\"b\":"}}]}}]}`,
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]}}]}`,
				`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"2}"}}]}}]}`,
			},
			want: `{"choices":[{"index":0,"message":{"content":null,"tool_calls":[{"function":{"arguments":"{\"a\":1}","name":"lookup"},"id":"c1","index":0},{"function":{"arguments":"{\"b\":2}","name":"send"},"id":"c2","index":1}]}}],"object":"chat.completion"}`,
		},
		{
			name: "every token's logprobs survive, not just the last chunk's",
			events: []string{
				`{"choices":[{"index":0,"delta":{"content":"a"},"logprobs":{"content":[{"token":"a","logprob":-0.1}]}}]}`,
				`{"choices":[{"index":0,"delta":{"content":"b"},"logprobs":{"content":[{"token":"b","logprob":-0.2}]}}]}`,
			},
			want: `{"choices":[{"index":0,"logprobs":{"content":[{"logprob":-0.1,"token":"a"},{"logprob":-0.2,"token":"b"}]},"message":{"content":"ab"}}],"object":"chat.completion"}`,
		},
		{
			name: "choices come back in index order",
			events: []string{
				`{"choices":[{"index":1,"delta":{"content":"second"}}]}`,
				`{"choices":[{"index":0,"delta":{"content":"first"}}]}`,
			},
			want: `{"choices":[{"index":0,"message":{"content":"first"}},{"index":1,"message":{"content":"second"}}],"object":"chat.completion"}`,
		},
		{
			name: "a host answering with a whole completion is left alone",
			events: []string{
				`{"id":"a","object":"chat.completion","choices":[{"index":0,"message":{"content":"whole"},"finish_reason":"stop"}]}`,
			},
			want: `{"id":"a","object":"chat.completion","choices":[{"index":0,"message":{"content":"whole"},"finish_reason":"stop"}]}`,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := string(AssembleSSEBody(sseStream(testCase.events...))); got != testCase.want {
				t.Fatalf("assembled\n got: %s\nwant: %s", got, testCase.want)
			}
		})
	}
}

func TestAssembleSSEBodyKeepsLogprobsOfEveryChoice(t *testing.T) {
	t.Parallel()
	assembled := string(AssembleSSEBody(sseStream(
		`{"choices":[{"index":0,"delta":{"content":"a"},"logprobs":{"content":[{"token":"a"}]}},{"index":1,"delta":{"content":"x"},"logprobs":{"content":[{"token":"x"}]}}]}`,
	)))
	want := `{"choices":[{"index":0,"logprobs":{"content":[{"token":"a"}]},"message":{"content":"a"}},{"index":1,"logprobs":{"content":[{"token":"x"}]},"message":{"content":"x"}}],"object":"chat.completion"}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}

// Upstream restates a tool call's id and type in every delta, and its name in the last one.
func TestAssembleSSEBodyKeepsIdentityFieldsWhole(t *testing.T) {
	t.Parallel()
	assembled := string(AssembleSSEBody(sseStream(
		`{"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"\"Paris\"}"}}]}}]}`,
	)))
	want := `{"choices":[{"index":0,"message":{"content":null,"role":"assistant","tool_calls":[{"function":{"arguments":"{\"city\":\"Paris\"}","name":"get_weather"},"id":"call_1","index":0,"type":"function"}]}}],"object":"chat.completion"}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}

func TestAssembleSSEBodyJoinsSplitDataLines(t *testing.T) {
	t.Parallel()
	split := []byte("data: {\"id\":\"a\",\"choices\":[{\"index\":0,\"delta\":\ndata: {\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n")

	assembled := string(AssembleSSEBody(split))

	want := `{"choices":[{"index":0,"message":{"content":"hi"}}],"id":"a","object":"chat.completion"}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}

// An index-less choice compared equal to every other, which is not a total order.
func TestAssembleSSEBodyOrdersChoicesWithAnIndexFirst(t *testing.T) {
	t.Parallel()
	assembled := string(AssembleSSEBody(sseStream(
		`{"choices":[{"index":1,"delta":{"content":"one"}}]}`,
		`{"choices":[{"delta":{"content":"none"}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"zero"}}]}`,
	)))
	want := `{"choices":[{"index":0,"message":{"content":"zero"}},{"index":1,"message":{"content":"one"}},{"message":{"content":"none"}}],"object":"chat.completion"}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}

func TestAssembleSSEBodyBoundsTheIndexedMerge(t *testing.T) {
	t.Parallel()
	events := make([]string, 0, maxIndexedElements+8)
	for index := range maxIndexedElements + 8 {
		events = append(events, fmt.Sprintf(`{"choices":[{"index":%d,"delta":{"content":"x"}}]}`, index))
	}

	assembled := AssembleSSEBody(sseStream(events...))

	var decoded struct {
		Choices []json.RawMessage `json:"choices"`
	}
	if err := json.Unmarshal(assembled, &decoded); err != nil {
		t.Fatalf("unmarshalling the assembled body: %v", err)
	}
	if len(decoded.Choices) != maxIndexedElements {
		t.Fatalf("choices = %d, want the merge bounded at %d", len(decoded.Choices), maxIndexedElements)
	}
}

func TestAssembleSSEBodyBoundsTheKeySet(t *testing.T) {
	t.Parallel()
	events := make([]string, 0, maxTopLevelFields+8)
	for index := range maxTopLevelFields + 8 {
		events = append(events, fmt.Sprintf(`{"invented%d":"x","choices":[{"index":0,"delta":{"content":"y"}}]}`, index))
	}

	assembled := AssembleSSEBody(sseStream(events...))

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(assembled, &decoded); err != nil {
		t.Fatalf("unmarshalling the assembled body: %v", err)
	}
	// The bound counts merged keys; the assembler then always writes object, whatever the host sent.
	if len(decoded) > maxTopLevelFields+1 {
		t.Fatalf("fields = %d, want the key set bounded at %d", len(decoded), maxTopLevelFields)
	}
	if _, held := decoded["choices"]; !held {
		t.Error("the bound dropped choices, which the reply cannot do without")
	}
}

// token_ids arrive as a per-chunk delta, unlike every other choice-level field.
func TestAssembleSSEBodyAccumulatesTokenIds(t *testing.T) {
	t.Parallel()
	assembled := string(AssembleSSEBody(sseStream(
		`{"choices":[{"index":0,"delta":{"content":"a"},"token_ids":[11]}]}`,
		`{"choices":[{"index":0,"delta":{"content":"b"},"token_ids":[22]}]}`,
	)))
	want := `{"choices":[{"index":0,"message":{"content":"ab"},"token_ids":[11,22]}],"object":"chat.completion"}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}

func TestAssembleSSEBodyRefusesRatherThanTruncate(t *testing.T) {
	t.Parallel()
	events := make([]string, 0, maxAssembledEvents+16)
	for range maxAssembledEvents + 16 {
		events = append(events, `{"choices":[{"index":0,"delta":{"content":"x"}}]}`)
	}

	assembled := AssembleSSEBody(sseStream(events...))

	if !bytes.Equal(assembled, TruncatedResponseBody) {
		t.Fatalf("assembled = %s, want the truncation error: a prefix is indistinguishable from a complete answer", assembled)
	}
}

func TestAssembleSSEBodyKeepsFoldedDeltasWhenACompletionFollows(t *testing.T) {
	t.Parallel()
	assembled := AssembleSSEBody(sseStream(
		`{"choices":[{"index":0,"delta":{"content":"hello "}}]}`,
		`{"choices":[{"index":0,"delta":{"content":"world"}}]}`,
		`{"object":"chat.completion","choices":[{"index":0,"message":{"content":"discarded"}}]}`,
	))

	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(assembled, &decoded); err != nil {
		t.Fatalf("unmarshalling the assembled body: %v", err)
	}
	if len(decoded.Choices) == 0 {
		t.Fatal("no choices survived")
	}
	if got := decoded.Choices[0].Message.Content; got != "hello world" {
		t.Fatalf("content = %q, want the folded deltas: a trailing completion must not discard them", got)
	}
}

func TestAssembleSSEBodyPassesASoleCompletionThrough(t *testing.T) {
	t.Parallel()
	payload := `{"object":"chat.completion","choices":[{"index":0,"message":{"content":"whole"}}]}`

	assembled := AssembleSSEBody(sseStream(payload))

	if string(assembled) != payload {
		t.Fatalf("assembled = %s, want the already-complete payload unchanged", assembled)
	}
}

func TestAssembleSSEBodyRejectsAPayloadCarryingTwoObjects(t *testing.T) {
	t.Parallel()

	assembled := string(AssembleSSEBody([]byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"real\"}}]} {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"smuggled\"}}]}\n\ndata: [DONE]\n\n")))

	if assembled != string(NoResponseDataBody) {
		t.Fatalf("assembled = %s, want the payload rejected", assembled)
	}
}

// Some upstream versions re-send a tool call's arguments whole on the finish chunk.
func TestAssembleSSEBodyDoesNotDoubleArgumentsResentWhole(t *testing.T) {
	t.Parallel()
	assembled := string(AssembleSSEBody(sseStream(
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"ci"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"Paris\"}"}}]}}]}`,
	)))
	want := `{"choices":[{"index":0,"message":{"content":null,"tool_calls":[{"function":{"arguments":"{\"city\":\"Paris\"}"},"index":0}]}}],"object":"chat.completion"}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}

// A recorded Kimi reply: thinking tags inline in content, an empty tool_calls on every chunk, and the
// usage event last.
func TestAssembleSSEBodyOnARecordedKimiReply(t *testing.T) {
	t.Parallel()
	stream, err := os.ReadFile(filepath.Join("testdata", "sse", "kimi_thinking_stream.sse"))
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}

	assembled := string(AssembleSSEBody(stream))

	want := `{"choices":[{"finish_reason":"length","index":0,"message":{"content":" I'll </think> examining","role":"assistant","tool_calls":[]}}],"created":1786114584,"id":"devshard-46055-1453","model":"moonshotai/Kimi-K2.6","object":"chat.completion","usage":{"completion_tokens":16,"prompt_tokens":46,"total_tokens":62}}`
	if assembled != want {
		t.Fatalf("assembled\n got: %s\nwant: %s", assembled, want)
	}
}
