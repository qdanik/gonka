package engine

import (
	"bytes"
	"strconv"
	"testing"

	"devshard/cmd/gateway/filters"
)

func newTestClassifier(model string, overflow func()) *sseClassifier {
	return newSSEClassifier(testBudget(1<<20, 1<<20, 1<<20), testParticipant, model, overflow)
}

func TestClassifierMapsOneChunkToItsFacts(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		model string
		chunk string
		want  chunkFacts
	}{
		{
			name:  "content_chunk",
			model: qwenModel,
			chunk: `data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
			want:  chunkFacts{Content: true, ContentSource: "delta.content"},
		},
		{
			name:  "role_only_chunk_is_neither_content_nor_error",
			model: qwenModel,
			chunk: `data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n",
			want:  chunkFacts{},
		},
		{
			name:  "error_event_counts_as_a_chunk_but_never_crowns",
			model: qwenModel,
			chunk: `data: {"error":{"code":500,"message":"backend failed","type":"server_error"}}` + "\n\n",
			want: chunkFacts{
				Error:        true,
				ErrorSource:  "error.server_error",
				ErrorCode:    "500",
				ErrorType:    "server_error",
				ErrorMessage: "backend failed",
				ErrorPayload: `{"error":{"code":500,"message":"backend failed","type":"server_error"}}`,
			},
		},
		{
			name:  "context_limit_refusal_is_retriable_not_an_error_chunk",
			model: qwenModel,
			chunk: `data: {"error":{"code":400,"message":"` + vllmContextTotalMessage + `","type":"BadRequestError"}}` + "\n\n",
			want: chunkFacts{
				ErrorSource:       "error.BadRequestError",
				ErrorCode:         "400",
				ErrorType:         "BadRequestError",
				ErrorMessage:      vllmContextTotalMessage,
				ErrorPayload:      `{"error":{"code":400,"message":"` + vllmContextTotalMessage + `","type":"BadRequestError"}}`,
				CapabilityRefused: true,
			},
		},
		{
			name:  "tool_refusal_is_retriable_not_an_error_chunk",
			model: qwenModel,
			chunk: `data: {"object":"error","message":"` + filters.ToolChoiceUnsupportedMessage + `","type":"BadRequestError"}` + "\n\n",
			want: chunkFacts{
				ErrorSource:       "error.BadRequestError",
				ErrorType:         "BadRequestError",
				ErrorMessage:      filters.ToolChoiceUnsupportedMessage,
				ErrorPayload:      `{"object":"error","message":"` + filters.ToolChoiceUnsupportedMessage + `","type":"BadRequestError"}`,
				CapabilityRefused: true,
			},
		},
		{
			name:  "usage_burns_tokens_on_a_thinking_budget_route",
			model: kimiModel,
			chunk: `data: {"choices":[{"delta":{}}],"usage":{"completion_tokens":90}}` + "\n\n",
			want:  chunkFacts{UsageCompletionTokens: 90, TokensBurned: true},
		},
		{
			name:  "usage_off_the_thinking_budget_route_burns_nothing",
			model: qwenModel,
			chunk: `data: {"choices":[{"delta":{}}],"usage":{"completion_tokens":90}}` + "\n\n",
			want:  chunkFacts{UsageCompletionTokens: 90},
		},
		{
			name:  "an_empty_stop_with_usage_crowns_on_a_thinking_budget_route",
			model: kimiModel,
			chunk: `data: {"choices":[{"message":{"content":""},"finish_reason":"stop"}],"usage":{"completion_tokens":90}}` + "\n\n",
			want: chunkFacts{
				Content:               true,
				ContentSource:         "message.empty_stop_completion_tokens",
				UsageCompletionTokens: 90,
				TokensBurned:          true,
			},
		},
		{
			name:  "an_empty_stop_with_usage_crowns_nobody_off_that_route",
			model: qwenModel,
			chunk: `data: {"choices":[{"message":{"content":""},"finish_reason":"stop"}],"usage":{"completion_tokens":90}}` + "\n\n",
			want:  chunkFacts{UsageCompletionTokens: 90},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			classifier := newTestClassifier(testCase.model, nil)
			defer classifier.Release()

			got := classifier.Classify([]byte(testCase.chunk))

			if got != testCase.want {
				t.Errorf("Classify() = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestClassifierHoldsASplitEventUntilItsLineCompletes(t *testing.T) {
	t.Parallel()
	classifier := newTestClassifier(qwenModel, nil)
	defer classifier.Release()

	head := classifier.Classify([]byte(`data: {"choices":[{"delta":{"con`))
	tail := classifier.Classify([]byte(`tent":"hi"}}]}` + "\n\n"))

	if head.Content {
		t.Error("a half-delivered event was classified as content")
	}
	if !tail.Content || tail.ContentSource != "delta.content" {
		t.Errorf("completing chunk = %+v, want content from delta.content", tail)
	}
}

func TestClassifierCountsAReassembledEventExactlyOnce(t *testing.T) {
	t.Parallel()
	event := []byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n")
	classifier := newTestClassifier(qwenModel, nil)
	defer classifier.Release()

	contentChunks := 0
	for index := range event {
		if classifier.Classify(event[index : index+1]).Content {
			contentChunks++
		}
	}

	if contentChunks != 1 {
		t.Errorf("content chunks = %d, want 1", contentChunks)
	}
}

func TestClassifierFlushReadsTheNewlineLessFinalEvent(t *testing.T) {
	t.Parallel()
	classifier := newTestClassifier(qwenModel, nil)
	defer classifier.Release()

	streamed := classifier.Classify([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}`))
	flushed := classifier.Flush()

	if streamed.Content {
		t.Error("an unterminated event was classified before the stream closed")
	}
	if !flushed.Content || flushed.ContentSource != "delta.content" {
		t.Errorf("Flush() = %+v, want content from delta.content", flushed)
	}
}

func TestClassifierReleaseRefundsTheHeldFragment(t *testing.T) {
	t.Parallel()
	budget := testBudget(1<<20, 1<<20, 1<<20)
	classifier := newSSEClassifier(budget, testParticipant, qwenModel, nil)

	classifier.Classify([]byte(`data: {"choices":[`))
	assertUsage(t, budget, testParticipant, 18, 18)

	classifier.Release()

	assertUsage(t, budget, testParticipant, 0, 0)
}

func TestClassifierReportsAnOverflowOnceAndKeepsClassifying(t *testing.T) {
	t.Parallel()
	overflows := 0
	classifier := newSSEClassifier(testBudget(8, 1<<20, 1<<20), testParticipant, qwenModel, func() { overflows++ })
	defer classifier.Release()

	oversized := []byte(`data: {"choices":[{"delta":{"content":"much longer than the attempt cap"`)
	classifier.Classify(oversized)
	classifier.Classify(oversized)
	facts := classifier.Classify([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n"))

	if overflows != 1 {
		t.Errorf("overflow reports = %d, want 1", overflows)
	}
	if !facts.Content {
		t.Error("classification did not resume after the cap trip")
	}
}

func TestClassifierReadsTheFixtureCorpusIdenticallyAtEveryChunkSize(t *testing.T) {
	t.Parallel()
	chunkSizes := []int{1, 2, 3, 7, 64, 256, 1024, 4096, 8192}
	for _, fixture := range sseFixtures {
		for _, chunkSize := range chunkSizes {
			t.Run(fixture.file+"/chunk="+strconv.Itoa(chunkSize), func(t *testing.T) {
				t.Parallel()
				body := readFixture(t, fixture.file)

				state := replayStream(t, qwenModel, true, fixedChunks(body, chunkSize)...)

				assertFixtureState(t, fixture, state)
			})
		}
	}
}

func TestClassifierPreservesTheChunkItWasGiven(t *testing.T) {
	t.Parallel()
	chunk := []byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n")
	original := bytes.Clone(chunk)
	classifier := newTestClassifier(qwenModel, nil)
	defer classifier.Release()

	classifier.Classify(chunk)

	if !bytes.Equal(chunk, original) {
		t.Errorf("Classify mutated its input: %q", chunk)
	}
}
