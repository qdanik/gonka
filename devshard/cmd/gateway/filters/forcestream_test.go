package filters

import (
	"strings"
	"testing"
)

func normalizeForTest(t *testing.T, body string) Result {
	t.Helper()
	result, err := NormalizeRequest([]byte(body), Options{
		DefaultMaxTokens: 3072,
		MaxTokensCap:     4096,
		RoutedModel:      "model-a",
	})
	if err != nil {
		t.Fatalf("NormalizeRequest(%s) = %v, want acceptance", body, err)
	}
	return result
}

// The divergence the goldens deliberately do not record.
func TestForcesStreamingUpstream(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name             string
		body             string
		wantClientStream bool
		wantClientUsage  bool
	}{
		{
			name: "a client that said nothing about streaming",
			body: `{"model":"model-a","messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "a client that asked for a buffered reply",
			body: `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":false}`,
		},
		{
			name:             "a streaming client that did not ask for usage",
			body:             `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":true}`,
			wantClientStream: true,
		},
		{
			name:             "a streaming client that asked for usage",
			body:             `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`,
			wantClientStream: true,
			wantClientUsage:  true,
		},
		{
			name: "a buffered client whose stream_options the validation stage drops",
			body: `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":false,"stream_options":{"include_usage":true}}`,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result := normalizeForTest(t, testCase.body)

			if !strings.Contains(string(result.Body), `"stream":true`) {
				t.Errorf("body = %s, want stream:true asked of the host", result.Body)
			}
			if !strings.Contains(string(result.Body), `"stream_options":{"include_usage":true}`) {
				t.Errorf("body = %s, want include_usage asked of the host", result.Body)
			}
			if result.ClientStream != testCase.wantClientStream {
				t.Errorf("ClientStream = %v, want %v", result.ClientStream, testCase.wantClientStream)
			}
			if result.ClientUsage != testCase.wantClientUsage {
				t.Errorf("ClientUsage = %v, want %v", result.ClientUsage, testCase.wantClientUsage)
			}
		})
	}
}

func TestForcedStreamOptionsReplaceWhatTheClientSent(t *testing.T) {
	t.Parallel()

	result := normalizeForTest(t, `{"model":"model-a","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":false,"continuous_usage_stats":true}}`)

	if strings.Contains(string(result.Body), "continuous_usage_stats") {
		t.Errorf("body = %s, want the unlisted sub-field gone", result.Body)
	}
	if !strings.Contains(string(result.Body), `"stream_options":{"include_usage":true}`) {
		t.Errorf("body = %s, want include_usage asked of the host", result.Body)
	}
	if result.ClientUsage {
		t.Error("ClientUsage = true, want false: the client asked for include_usage:false")
	}
}

func TestStreamRewriterDropsForcedUsage(t *testing.T) {
	t.Parallel()
	const contentEvent = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n"
	const usageOnlyEvent = "data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}\n\n"
	const contentWithUsageEvent = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}],\"usage\":{\"completion_tokens\":2}}\n\n"
	testCases := []struct {
		name      string
		keepUsage bool
		stream    string
		want      string
	}{
		{
			name:   "the usage-only event disappears for a client that did not ask",
			stream: contentEvent + usageOnlyEvent,
			want:   contentEvent,
		},
		{
			name:      "the usage-only event survives for a client that asked",
			keepUsage: true,
			stream:    contentEvent + usageOnlyEvent,
			want:      contentEvent + usageOnlyEvent,
		},
		{
			name:   "usage riding along with content is removed, the content is not",
			stream: contentWithUsageEvent,
			want:   "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"index\":0}]}\n\n",
		},
		{
			name:   "the terminator is not usage",
			stream: string(SSEDoneEvent),
			want:   string(SSEDoneEvent),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			got, err := NewStreamRewriter(LogprobIntent{}, testCase.keepUsage).Write([]byte(testCase.stream))

			if err != nil {
				t.Fatalf("Write() = %v, want no error", err)
			}
			if string(got) != testCase.want {
				t.Errorf("rewritten\n got: %q\nwant: %q", got, testCase.want)
			}
		})
	}
}

// A host's error event carries no choices either, so it must not be dropped as an empty one.
func TestStreamRewriterKeepsAnErrorEventThatCarriesUsage(t *testing.T) {
	t.Parallel()
	event := []byte("data: {\"error\":{\"message\":\"upstream exploded\"},\"usage\":{\"completion_tokens\":1}}\n\n")

	got, err := NewStreamRewriter(LogprobIntent{}, false).Write(event)

	if err != nil {
		t.Fatalf("Write() = %v, want no error", err)
	}
	if !strings.Contains(string(got), "upstream exploded") {
		t.Fatalf("rewritten = %q, want the host's error preserved", got)
	}
	if strings.Contains(string(got), "usage") {
		t.Errorf("rewritten = %q, want the usage gone", got)
	}
}

// Upstream restates usage as null on every chunk once include_usage is forced.
func TestStreamRewriterRemovesANullUsage(t *testing.T) {
	t.Parallel()
	event := []byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}],\"usage\":null}\n\n")

	got, err := NewStreamRewriter(LogprobIntent{}, false).Write(event)

	if err != nil {
		t.Fatalf("Write() = %v, want no error", err)
	}
	if strings.Contains(string(got), "usage") {
		t.Errorf("rewritten = %q, want the null usage gone", got)
	}
}

// A dropped usage tail is not a truncated stream.
func TestStreamRewriterCloseSeparatesADropFromATruncation(t *testing.T) {
	t.Parallel()
	rewriter := NewStreamRewriter(LogprobIntent{}, false)
	if _, err := rewriter.Write([]byte("data: {\"choices\":[],\"usage\":{\"completion_tokens\":2}}")); err != nil {
		t.Fatalf("Write() = %v, want no error", err)
	}

	tail, err := rewriter.Close()

	if err != nil {
		t.Fatalf("Close() = %v, want no error for a dropped usage tail", err)
	}
	if len(tail) != 0 {
		t.Errorf("Close() = %q, want nothing emitted", tail)
	}
}
