package filters

import (
	"bytes"
	"errors"
	"fmt"
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

func TestStreamRewriter_SplitFrameIsRewritten(t *testing.T) {
	stream := readSSEFixture(t, "logprobs_stream.sse")
	target := splitCompleteEvents(t, stream)[1]
	splitAt := bytes.Index(target, []byte("top_logprobs")) + len("top_logprobs")
	if splitAt <= len("top_logprobs") || splitAt >= len(target) {
		t.Fatalf("fixture no longer contains a mid-event top_logprobs split point")
	}
	rewriter := NewStreamRewriter()

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

	got := feedInChunks(t, NewStreamRewriter(), stream, 1)

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
	}
	for _, name := range fixtures {
		for _, chunkSize := range []int{1, 3, 17, 4096} {
			t.Run(fmt.Sprintf("%s/chunk=%d", name, chunkSize), func(t *testing.T) {
				stream := readSSEFixture(t, name)
				want := rewriteWholeStream(t, stream)

				got := feedInChunks(t, NewStreamRewriter(), stream, chunkSize)

				if !bytes.Equal(got, want) {
					t.Errorf("chunked output differs from whole-stream output\n got:  %q\n want: %q", got, want)
				}
			})
		}
	}
}

func TestStreamRewriter_SeveralFramesInOneChunk(t *testing.T) {
	stream := readSSEFixture(t, "token_ids_stream.sse")
	rewriter := NewStreamRewriter()

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
	rewriter := NewStreamRewriter()

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
			got := feedInChunks(t, NewStreamRewriter(), event, chunkSize)

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
	rewriter := NewStreamRewriter()
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
	rewriter := NewStreamRewriter()
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
	rewriter := NewStreamRewriter()
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
	rewriter := NewStreamRewriter()
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
	rewriter := NewStreamRewriter()
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
	rewriter := NewStreamRewriter()
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
	rewriter := NewStreamRewriter()
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

	got := feedInChunks(t, NewStreamRewriter(), stream, 1)

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
	rewriter := NewStreamRewriter()

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
