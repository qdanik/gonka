package filters

import (
	"bytes"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// feedInChunks streams data through rewriter in fixed-size pieces and returns everything emitted,
// failing the test on any Write or Close error.
func feedInChunks(t *testing.T, rewriter *StreamRewriter, data []byte, chunkSize int) []byte {
	t.Helper()
	var emitted bytes.Buffer
	for start := 0; start < len(data); start += chunkSize {
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		out, err := rewriter.Write(data[start:end])
		if err != nil {
			t.Fatalf("Write(%d:%d) = %v", start, end, err)
		}
		emitted.Write(out)
	}
	final, err := rewriter.Close()
	if err != nil {
		t.Fatalf("Close() = %v", err)
	}
	emitted.Write(final)
	return emitted.Bytes()
}

func assertNoInternalFields(t *testing.T, out []byte) {
	t.Helper()
	for _, field := range []string{"logprob", "token_ids", "prompt_logprobs"} {
		if bytes.Contains(out, []byte(field)) {
			t.Errorf("output leaked %q: %s", field, out)
		}
	}
}

// The streaming rewriter skips any event a cheap byte pre-check calls uninteresting, so a stripped
// field the pre-check cannot see is forwarded verbatim. Every field must be reachable on its own,
// because nothing guarantees a host emits it next to a sibling that happens to be recognised.
func TestStreamRewriter_StripsEveryFieldEvenWhenItArrivesAlone(t *testing.T) {
	for _, field := range clientStrippedFields {
		t.Run(field, func(t *testing.T) {
			event := []byte(`data: {"choices":[{"delta":{"content":"ok","` + field + `":[1,2]}}]}` + "\n\n")

			out, err := NewStreamRewriter(LogprobIntent{}).Write(event)
			if err != nil {
				t.Fatalf("Write() = %v", err)
			}

			if bytes.Contains(out, []byte(`"`+field+`"`)) {
				t.Fatalf("%q leaked to the client: %s", field, out)
			}
			if !bytes.Contains(out, []byte(`"content":"ok"`)) {
				t.Fatalf("sibling content field was lost: %s", out)
			}
		})
	}
}

// The space after "data:" is optional on the wire, and an event may carry lines of its own, so a
// framing the strip does not recognise would forward every field the non-streaming path removes.
func TestStreamRewriter_StripsEveryDataLineFraming(t *testing.T) {
	payload := `{"choices":[{"delta":{"content":"ok"},"token_ids":[7]}]}`
	cases := []struct {
		name  string
		event string
	}{
		{name: "with_a_space", event: "data: " + payload + "\n\n"},
		{name: "without_a_space", event: "data:" + payload + "\n\n"},
		{name: "after_an_event_line", event: "event: message\ndata: " + payload + "\n\n"},
		{name: "after_an_id_line", event: "id: 42\ndata: " + payload + "\n\n"},
		{name: "after_a_comment_line", event: ": keep-alive\ndata: " + payload + "\n\n"},
		{name: "crlf_framed_without_a_space", event: "id: 42\r\ndata:" + payload + "\r\n\r\n"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := feedInChunks(t, NewStreamRewriter(LogprobIntent{}), []byte(testCase.event), len(testCase.event))

			assertNoInternalFields(t, got)
			if !bytes.Contains(got, []byte(`"content":"ok"`)) {
				t.Errorf("sibling content field was lost: %s", got)
			}
		})
	}
}

// A host may answer a streaming request with one SSE-wrapped complete response. An OpenAI streaming
// client reads choices[].delta, so it renders nothing from the message the completion carries.
func TestStreamRewriter_WrappedCompletionBecomesChunks(t *testing.T) {
	stream := readSSEFixture(t, "completion_wrapped_stream.sse")

	got := rewriteWholeStream(t, stream)

	assertNoInternalFields(t, got)
	if bytes.Contains(got, []byte(`"message"`)) {
		t.Errorf("the client was handed a message where it reads a delta: %s", got)
	}
	want := strings.Join([]string{
		`data: {"id":"chatcmpl-cw1","object":"chat.completion.chunk","created":13,"model":"model-a","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-cw1","object":"chat.completion.chunk","created":13,"model":"model-a","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":null}]}`,
		`data: {"id":"chatcmpl-cw1","object":"chat.completion.chunk","created":13,"model":"model-a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		// usage comes back in alphabetical order: the strip rebuilds the object from a map, which has
		// none, so the client sees a reordered copy of what the host sent.
		`data: {"id":"chatcmpl-cw1","object":"chat.completion.chunk","created":13,"model":"model-a","choices":[],"usage":{"completion_tokens":2,"prompt_tokens":3,"total_tokens":5}}`,
		`data: [DONE]`,
		``,
	}, "\n\n")
	if string(got) != want {
		t.Errorf("synthesised stream\n got:  %s\n want: %s", got, want)
	}
}

func TestStreamRewriter_ChunkEventsAndErrorsAreNotConverted(t *testing.T) {
	cases := []struct {
		name  string
		event string
	}{
		{
			name:  "a_chunk_quoting_the_word_message",
			event: `data: {"choices":[{"delta":{"content":"the \"message\" field"}}]}` + "\n\n",
		},
		{
			name:  "an_error_carrying_a_message_field",
			event: `data: {"error":{"message":"boom","type":"server_error"}}` + "\n\n",
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := NewStreamRewriter(LogprobIntent{}).Write([]byte(testCase.event))
			if err != nil {
				t.Fatalf("Write() = %v", err)
			}

			if string(got) != testCase.event {
				t.Errorf("Write() = %s, want the event verbatim", got)
			}
		})
	}
}

func TestStreamRewriter_SplitFrameIsRewritten(t *testing.T) {
	stream := readSSEFixture(t, "logprobs_stream.sse")
	target := splitCompleteEvents(t, stream)[1]
	splitAt := bytes.Index(target, []byte("top_logprobs")) + len("top_logprobs")
	if splitAt <= len("top_logprobs") || splitAt >= len(target) {
		t.Fatalf("fixture no longer contains a mid-event top_logprobs split point")
	}
	rewriter := NewStreamRewriter(LogprobIntent{})

	firstOut, err := rewriter.Write(target[:splitAt])
	if err != nil {
		t.Fatalf("first Write() = %v", err)
	}
	secondOut, err := rewriter.Write(target[splitAt:])
	if err != nil {
		t.Fatalf("second Write() = %v", err)
	}

	if len(firstOut) != 0 {
		t.Errorf("incomplete event must be retained, got %q", firstOut)
	}
	joined := append(append([]byte(nil), firstOut...), secondOut...)
	assertNoInternalFields(t, joined)
	if !bytes.Contains(joined, []byte(`"content":"ok"`)) {
		t.Errorf("sibling content field was lost: %s", joined)
	}
}

func TestStreamRewriter_ByteByByteFeedStripsLogprobs(t *testing.T) {
	stream := readSSEFixture(t, "logprobs_stream.sse")

	got := feedInChunks(t, NewStreamRewriter(LogprobIntent{}), stream, 1)

	assertNoInternalFields(t, got)
	if !bytes.Contains(got, []byte(`"content":"ok"`)) {
		t.Errorf("sibling content field was lost: %s", got)
	}
	if !bytes.HasSuffix(got, []byte("data: [DONE]\n\n")) {
		t.Errorf("[DONE] marker not preserved: %s", got)
	}
}

func TestStreamRewriter_ChunkSizeDoesNotChangeOutput(t *testing.T) {
	fixtures := []string{
		"content_stream.sse",
		"tool_calls_stream.sse",
		"logprobs_stream.sse",
		"token_ids_stream.sse",
		"comment_and_blank_lines.sse",
		"newlineless_final_content.sse",
		"completion_wrapped_stream.sse",
	}
	for _, name := range fixtures {
		for _, chunkSize := range []int{1, 3, 17, 4096} {
			t.Run(fmt.Sprintf("%s/chunk=%d", name, chunkSize), func(t *testing.T) {
				stream := readSSEFixture(t, name)
				want := rewriteWholeStream(t, stream)

				got := feedInChunks(t, NewStreamRewriter(LogprobIntent{}), stream, chunkSize)

				if !bytes.Equal(got, want) {
					t.Errorf("chunked output differs from whole-stream output\n got:  %q\n want: %q", got, want)
				}
			})
		}
	}
}

func TestStreamRewriter_SeveralFramesInOneChunk(t *testing.T) {
	stream := readSSEFixture(t, "token_ids_stream.sse")
	rewriter := NewStreamRewriter(LogprobIntent{})

	got, err := rewriter.Write(stream)
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}

	assertNoInternalFields(t, got)
	if want := 4; bytes.Count(got, []byte("data: ")) != want {
		t.Errorf("emitted %d events, want %d: %s", bytes.Count(got, []byte("data: ")), want, got)
	}
}

func TestStreamRewriter_RetainsPartialUntilTerminatorArrives(t *testing.T) {
	stream := readSSEFixture(t, "logprobs_stream.sse")
	target := splitCompleteEvents(t, stream)[1]
	rewriter := NewStreamRewriter(LogprobIntent{})

	withoutTerminator, err := rewriter.Write(bytes.TrimRight(target, "\n"))
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}
	afterTerminator, err := rewriter.Write([]byte("\n\n"))
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}

	if len(withoutTerminator) != 0 {
		t.Errorf("unterminated event must not be emitted, got %q", withoutTerminator)
	}
	if len(afterTerminator) == 0 {
		t.Error("event was not emitted once its terminator arrived")
	}
	assertNoInternalFields(t, afterTerminator)
}

func TestStreamRewriter_CRLFTerminatedEventIsRewritten(t *testing.T) {
	event := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"logprobs\":{\"content\":[]}}]}\r\n\r\ndata: [DONE]\r\n\r\n")
	for _, chunkSize := range []int{1, 3, len(event)} {
		t.Run(fmt.Sprintf("chunk=%d", chunkSize), func(t *testing.T) {
			got := feedInChunks(t, NewStreamRewriter(LogprobIntent{}), event, chunkSize)

			assertNoInternalFields(t, got)
			if !bytes.Contains(got, []byte(`"content":"ok"`)) {
				t.Errorf("sibling content field was lost: %s", got)
			}
			if !bytes.HasSuffix(got, []byte("data: [DONE]\r\n\r\n")) {
				t.Errorf("[DONE] marker not preserved: %q", got)
			}
		})
	}
}

func TestStreamRewriter_CarryOverflowFailsInsteadOfGrowing(t *testing.T) {
	rewriter := NewStreamRewriter(LogprobIntent{})
	atCap, err := rewriter.Write(bytes.Repeat([]byte("x"), MaxStreamCarryBytes))
	if err != nil {
		t.Fatalf("Write() at the cap = %v, want no error", err)
	}
	if len(atCap) != 0 {
		t.Errorf("unterminated bytes must not be emitted, got %d bytes", len(atCap))
	}

	overflow, err := rewriter.Write([]byte("x"))

	if !errors.Is(err, ErrStreamCarryOverflow) {
		t.Fatalf("Write() past the cap = %v, want ErrStreamCarryOverflow", err)
	}
	if len(overflow) != 0 {
		t.Errorf("overflowing Write emitted %d bytes, want none", len(overflow))
	}
	if len(rewriter.carry) != 0 {
		t.Errorf("carry kept %d bytes after overflow, want it released", len(rewriter.carry))
	}
}

func TestStreamRewriter_StaysFailedAfterOverflow(t *testing.T) {
	rewriter := NewStreamRewriter(LogprobIntent{})
	if _, err := rewriter.Write(bytes.Repeat([]byte("x"), MaxStreamCarryBytes+1)); !errors.Is(err, ErrStreamCarryOverflow) {
		t.Fatalf("Write() past the cap = %v, want ErrStreamCarryOverflow", err)
	}

	afterOverflow, writeErr := rewriter.Write(readSSEFixture(t, "logprobs_stream.sse"))
	closed, closeErr := rewriter.Close()

	if !errors.Is(writeErr, ErrStreamCarryOverflow) || len(afterOverflow) != 0 {
		t.Errorf("Write() after overflow = %q, %v; want no output and ErrStreamCarryOverflow", afterOverflow, writeErr)
	}
	if !errors.Is(closeErr, ErrStreamCarryOverflow) || len(closed) != 0 {
		t.Errorf("Close() after overflow = %q, %v; want no output and ErrStreamCarryOverflow", closed, closeErr)
	}
}

func TestStreamRewriter_CloseOnCleanEndEmitsNothing(t *testing.T) {
	rewriter := NewStreamRewriter(LogprobIntent{})
	if _, err := rewriter.Write(readSSEFixture(t, "logprobs_stream.sse")); err != nil {
		t.Fatalf("Write() = %v", err)
	}

	final, err := rewriter.Close()

	if err != nil {
		t.Errorf("Close() = %v, want no error", err)
	}
	if len(final) != 0 {
		t.Errorf("Close() emitted %q, want nothing", final)
	}
}

func TestStreamRewriter_CloseEmitsUnterminatedFinalEvent(t *testing.T) {
	stream := readSSEFixture(t, "newlineless_final_content.sse")
	rewriter := NewStreamRewriter(LogprobIntent{})
	emitted, err := rewriter.Write(stream)
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}

	final, err := rewriter.Close()

	if err != nil {
		t.Errorf("Close() = %v, want no error", err)
	}
	if !bytes.Equal(append(emitted, final...), stream) {
		t.Errorf("well-formed unterminated final event was not preserved\n got:  %q\n want: %q", append(emitted, final...), stream)
	}
}

func TestStreamRewriter_CloseRewritesUnterminatedFinalEvent(t *testing.T) {
	event := []byte(`data: {"choices":[{"delta":{"content":"ok"},"token_ids":[7]}]}`)
	rewriter := NewStreamRewriter(LogprobIntent{})
	if _, err := rewriter.Write(event); err != nil {
		t.Fatalf("Write() = %v", err)
	}

	final, err := rewriter.Close()

	if err != nil {
		t.Errorf("Close() = %v, want no error", err)
	}
	assertNoInternalFields(t, final)
	if !bytes.Contains(final, []byte(`"content":"ok"`)) {
		t.Errorf("sibling content field was lost: %s", final)
	}
}

func TestStreamRewriter_CloseDropsTruncatedFinalEvent(t *testing.T) {
	truncated := []byte(`data: {"choices":[{"delta":{"content":"ok"},"logprobs":{"content":[{"logprob":-0.1`)
	rewriter := NewStreamRewriter(LogprobIntent{})
	if _, err := rewriter.Write(truncated); err != nil {
		t.Fatalf("Write() = %v", err)
	}

	final, err := rewriter.Close()

	if !errors.Is(err, ErrStreamTruncatedEvent) {
		t.Errorf("Close() = %v, want ErrStreamTruncatedEvent", err)
	}
	if len(final) != 0 {
		t.Errorf("Close() forwarded a truncated event: %q", final)
	}
}

func TestStreamRewriter_CloseKeepsUnterminatedNonDataLine(t *testing.T) {
	comment := []byte(": keep-alive")
	rewriter := NewStreamRewriter(LogprobIntent{})
	if _, err := rewriter.Write(comment); err != nil {
		t.Fatalf("Write() = %v", err)
	}

	final, err := rewriter.Close()

	if err != nil {
		t.Errorf("Close() = %v, want no error", err)
	}
	if !bytes.Equal(final, comment) {
		t.Errorf("Close() = %q, want the comment line verbatim", final)
	}
}

func TestStreamRewriter_MalformedFrameIsDropped(t *testing.T) {
	stream := readSSEFixture(t, "malformed_data_line.sse")

	got := feedInChunks(t, NewStreamRewriter(LogprobIntent{}), stream, 1)

	assertNoInternalFields(t, got)
	if bytes.Contains(got, []byte(`"broken":true`)) {
		t.Errorf("malformed event was forwarded: %s", got)
	}
	if !bytes.HasSuffix(got, []byte("data: [DONE]\n\n")) {
		t.Errorf("[DONE] marker not preserved: %s", got)
	}
}

func TestStreamRewriter_ParseableFrameMentioningLogprobsInTextSurvives(t *testing.T) {
	event := []byte(`data: {"choices":[{"delta":{"content":"the \"logprobs\" field"}}]}` + "\n\n")
	rewriter := NewStreamRewriter(LogprobIntent{})

	got, err := rewriter.Write(event)
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}

	if !bytes.Equal(got, event) {
		t.Errorf("Write() = %q, want the event verbatim", got)
	}
}

func TestAssembleSSEBody(t *testing.T) {
	testCases := []struct {
		name string
		body string
		want string
	}{
		{
			name: "the last data event wins",
			body: "data: {\"id\":\"first\"}\n\ndata: {\"id\":\"last\"}\n\ndata: [DONE]\n\n",
			want: `{"id":"last"}`,
		},
		{name: "a space-less data line", body: "data:{\"id\":\"tight\"}\n\n", want: `{"id":"tight"}`},
		{name: "crlf framing", body: "data: {\"id\":\"crlf\"}\r\n\r\ndata: [DONE]\r\n\r\n", want: `{"id":"crlf"}`},
		{
			name: "comment and event lines are not payloads",
			body: ": keep-alive\nevent: message\ndata: {\"id\":\"only\"}\n\n",
			want: `{"id":"only"}`,
		},
		{name: "a plain json body is already assembled", body: `{"id":"plain"}`, want: `{"id":"plain"}`},
		{name: "a terminator and nothing else", body: "data: [DONE]\n\n", want: `{"error":{"message":"no response data"}}`},
		{name: "an empty body", body: "", want: `{"error":{"message":"no response data"}}`},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := AssembleSSEBody([]byte(testCase.body)); string(got) != testCase.want {
				t.Fatalf("AssembleSSEBody(%q) = %s, want %s", testCase.body, got, testCase.want)
			}
		})
	}
}

func TestHasSSEDone(t *testing.T) {
	testCases := []struct {
		name   string
		events string
		want   bool
	}{
		{name: "the terminator", events: "data: [DONE]\n\n", want: true},
		{name: "the terminator over crlf", events: "data: [DONE]\r\n\r\n", want: true},
		{name: "a payload", events: "data: {\"id\":\"x\"}\n\n"},
		{name: "the marker inside a content delta", events: "data: {\"delta\":{\"content\":\"data: [DONE]\"}}\n\n"},
		{name: "nothing", events: ""},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := HasSSEDone([]byte(testCase.events)); got != testCase.want {
				t.Fatalf("HasSSEDone(%q) = %v, want %v", testCase.events, got, testCase.want)
			}
		})
	}
}

// The wire format lets one event carry several data lines, and declining to rewrite those forwards
// whatever the extra lines hold: a host puts a renderable delta on the first and its internal fields
// on the second, and the second reaches the client verbatim.
// A client joins an event's data lines with a newline before parsing, so one object split across two
// lines is one object to the client and must be one to the strip. Two objects on two lines join into
// something no client can parse, and forwarding it would carry whatever the second line hides.
func TestStreamRewriter_DropsAMultiLineEventNoClientCouldParse(t *testing.T) {
	event := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n" +
		"data: {\"prompt_logprobs\":[1,2],\"token_ids\":[7]}\n\n")

	got, err := NewStreamRewriter(LogprobIntent{}).Write(event)
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}

	assertNoInternalFields(t, got)
	if len(bytes.TrimSpace(got)) != 0 {
		t.Fatalf("an unparseable event reached the client: %s", got)
	}
}

// The split a host would actually use: one object across two lines, so neither half parses alone and a
// byte-wise gate sees nothing to do, while the client rejoins it and reads every field.
func TestStreamRewriter_StripsAnObjectSplitAcrossDataLines(t *testing.T) {
	event := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}],\n" +
		"data: \"prompt_logprobs\":[1,2],\"token_ids\":[7]}\n\n")

	got, err := NewStreamRewriter(LogprobIntent{}).Write(event)
	if err != nil {
		t.Fatalf("Write() = %v", err)
	}

	assertNoInternalFields(t, got)
	if !bytes.Contains(got, []byte(`"content":"ok"`)) {
		t.Fatalf("the renderable content was lost: %s", got)
	}
}

// A host can spell an internal field with a \u escape. The pre-check reads raw bytes, so the key
// matches no marker, while the client's decoder turns it back into logprobs and renders it.
func TestAnEscapedInternalKeyIsStrippedFromAStreamedDelta(t *testing.T) {
	event := []byte(`data: {"choices":[{"delta":{"content":"hi","\u006cogprobs":{"x":1}}}]}` + "\n\n")
	if bytes.Contains(event, []byte("logprobs")) {
		t.Fatal("the key is spelled in plain bytes, so the raw-byte scan finds it and the escape is never exercised")
	}

	rewritten := rewriteEvent(event, LogprobIntent{})

	if rewritten == nil {
		t.Fatal("the event was dropped, not stripped")
	}
	var decoded map[string]any
	payload := rewritten[len("data: "):]
	if err := stdjson.Unmarshal(bytes.TrimSpace(payload), &decoded); err != nil {
		t.Fatalf("rewritten event does not parse: %v (%s)", err, rewritten)
	}
	choices, _ := decoded["choices"].([]any)
	if len(choices) != 1 {
		t.Fatalf("choices = %v", decoded)
	}
	choice, _ := choices[0].(map[string]any)
	delta, _ := choice["delta"].(map[string]any)
	if _, leaked := delta["logprobs"]; leaked {
		t.Fatalf("an escaped internal key reached the client: %s", rewritten)
	}
	if delta["content"] != "hi" {
		t.Fatalf("the renderable content did not survive the strip: %s", rewritten)
	}
}

// A host answering a stream with a whole chat.completion has its response converted into the chunks a
// streaming client renders. Typing any host-controlled field lets the host fail that conversion with a
// value of the wrong shape, and the client then renders nothing while the nonce is settled regardless.
func TestAPoisonedIdentityFieldStillConvertsToChunks(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		event string
	}{
		{name: "numeric_id", event: `data: {"id":123,"object":"chat.completion","choices":[{"index":0,"message":{"content":"hi"}}]}`},
		{name: "created_past_float_range", event: `data: {"id":"x","created":1e999,"object":"chat.completion","choices":[{"index":0,"message":{"content":"hi"}}]}`},
		{name: "string_index", event: `data: {"id":"x","object":"chat.completion","choices":[{"index":"0","message":{"content":"hi"}}]}`},
		{name: "numeric_model", event: `data: {"id":"x","model":7,"object":"chat.completion","choices":[{"index":0,"message":{"content":"hi"}}]}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			rewritten := rewriteEvent([]byte(testCase.event+"\n\n"), LogprobIntent{})

			if rewritten == nil {
				t.Fatal("the event was dropped")
			}
			if !bytes.Contains(rewritten, []byte(`"chat.completion.chunk"`)) {
				t.Fatalf("the response was forwarded unconverted, so a streaming client renders nothing: %s", rewritten)
			}
			if !bytes.Contains(rewritten, []byte(`"content":"hi"`)) {
				t.Fatalf("the content did not survive the conversion: %s", rewritten)
			}
		})
	}
}

// The conversion re-encodes the delta, so the encoder it uses decides how generated content reaches
// the client. Escaping is lossless but inflates every < > & to six bytes on a path that carries whole
// model responses.
func TestConvertedChunksCarryGeneratedContentUnescaped(t *testing.T) {
	event := []byte(`data: {"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"content":"a < b & c"}}]}` + "\n\n")

	rewritten := rewriteEvent(event, LogprobIntent{})

	if !bytes.Contains(rewritten, []byte(`"content":"a < b & c"`)) {
		t.Fatalf("generated content was escaped on the way to the client: %s", rewritten)
	}
}

// The streaming path strips separately from the buffered one, so the client's intent has to reach it
// too: a client that asked for logprobs and streams would otherwise get them only when it does not.
func TestTheStreamStripFollowsWhatTheClientAskedFor(t *testing.T) {
	event := []byte(`data: {"choices":[{"delta":{"content":"hi","logprobs":{"content":[{"token":"hi","logprob":-0.5,"top_logprobs":[{"token":"hello"}]}]}},"index":0}],"token_ids":[7]}` + "\n\n")

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

			rewritten := string(rewriteEvent(event, testCase.intent))

			if held := strings.Contains(rewritten, `"logprob"`); held != testCase.wantLogprobs {
				t.Fatalf("logprob present = %v, want %v: %s", held, testCase.wantLogprobs, rewritten)
			}
			if filled := strings.Contains(rewritten, `"hello"`); filled != testCase.wantTopFilled {
				t.Fatalf("alternatives present = %v, want %v: %s", filled, testCase.wantTopFilled, rewritten)
			}
			if strings.Contains(rewritten, "token_ids") {
				t.Fatalf("an internal field reached the client: %s", rewritten)
			}
		})
	}
}

// A host controls the bytes of its own response, so it can spell "message" with a \u escape. A gate
// that reads raw bytes sees no completion to convert and forwards it whole, and a streaming client
// reading choices[].delta renders nothing -- while the nonce settles and the client pays for it.
func TestAnEscapedMessageKeyStillConvertsToChunks(t *testing.T) {
	event := []byte(`data: {"object":"chat.completion","choices":[{"index":0,"\u006dessage":{"content":"hi","\u006cogprobs":{"x":1}}}]}` + "\n\n")
	if bytes.Contains(event, []byte(`"message"`)) {
		t.Fatal("the key is spelled plainly, so the escape is never exercised")
	}

	rewritten := rewriteEvent(event, LogprobIntent{})

	if !bytes.Contains(rewritten, []byte(`"chat.completion.chunk"`)) {
		t.Fatalf("the response was forwarded unconverted, so a streaming client renders nothing: %s", rewritten)
	}
	if !bytes.Contains(rewritten, []byte(`"content":"hi"`)) {
		t.Fatalf("the content did not survive: %s", rewritten)
	}
	assertNoInternalFields(t, rewritten)
}

// An event whose payload spans two data lines and carries nothing to strip must come back out as the
// same object. Written after one prefix, its embedded newline starts a line with no data: prefix, and
// a client drops that line and rejoins a truncated object.
func TestAMultiLineEventWithNothingToStripSurvivesIntact(t *testing.T) {
	event := []byte("data: {\"choices\":[{\"delta\":{\"content\":\n" + "data: \"hi\"}}]}\n\n")

	rewritten := rewriteEvent(event, LogprobIntent{})

	_, payload, held := eventPayload(rewritten)
	if !held {
		t.Fatalf("the rewritten event carries no data line: %q", rewritten)
	}
	var decoded map[string]any
	if err := stdjson.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("a client cannot rejoin the event into an object: %v (%q)", err, rewritten)
	}
	if !bytes.Contains(rewritten, []byte(`"hi"`)) {
		t.Fatalf("the content was lost: %q", rewritten)
	}
}

// A completion carrying a non-finite bareword must still convert. Normalisation makes it parseable,
// but only if the parseable bytes are what travels on: handed the original, the conversion fails on
// the barewords and forwards a response a streaming client renders nothing from -- while the attempt
// is crowned on its content, because the content detector is lenient where the conversion is strict.
func TestACompletionWithNonFiniteNumbersStillConvertsToChunks(t *testing.T) {
	event := []byte(`data: {"id":"x","object":"chat.completion","choices":[{"index":0,"message":{"content":"hi"},` +
		`"logprobs":{"content":[{"token":"hi","logprob":-Infinity,"top_logprobs":[{"token":"hi","logprob":-Infinity}]}]}}]}` + "\n\n")

	rewritten := rewriteEvent(event, LogprobIntent{Keep: true, KeepTop: true})

	if !bytes.Contains(rewritten, []byte(`"chat.completion.chunk"`)) {
		t.Fatalf("forwarded unconverted, so a streaming client renders nothing while the nonce is paid: %s", rewritten)
	}
	if !bytes.Contains(rewritten, []byte(`"content":"hi"`)) {
		t.Fatalf("the client's answer was lost: %s", rewritten)
	}
	if bytes.Contains(rewritten, []byte("Infinity")) {
		t.Fatalf("a bareword no client can parse reached the client: %s", rewritten)
	}
}
