package filters

import (
	"bytes"
	"testing"
)

func foldChunked(t *testing.T, body []byte, intent LogprobIntent, size int) []byte {
	t.Helper()
	folder := NewBodyFolder(intent)
	for offset := 0; offset < len(body); offset += size {
		end := min(offset+size, len(body))
		if _, err := folder.Write(body[offset:end]); err != nil {
			t.Fatalf("Write(): %v", err)
		}
	}
	return folder.Body()
}

func TestFoldingArrivesAtWhatTheWholeBodyWouldHaveAssembled(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		intent LogprobIntent
	}{
		{
			name: "deltas merged into one completion",
			body: "data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"he\"}}]}\n\n" +
				"data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"llo\"}}]}\n\n" +
				"data: [DONE]\n\n",
		},
		{
			name: "a host that answered with a whole completion",
			body: "data: {\"object\":\"chat.completion\",\"choices\":[{\"index\":0,\"message\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
		},
		{
			name: "a stream that carried no payload",
			body: "\n\n",
		},
		{
			name: "a body that is not SSE at all",
			body: "{\"object\":\"chat.completion\",\"choices\":[{\"index\":0,\"message\":{\"content\":\"hi\"}}]}",
		},
		{
			name: "internal fields the client never sees",
			body: "data: {\"object\":\"chat.completion.chunk\",\"prompt_token_ids\":[1,2,3],\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"logprobs\":{\"content\":[{\"token\":\"hi\",\"logprob\":-0.1,\"token_ids\":[7],\"top_logprobs\":[{\"token\":\"h\",\"logprob\":-1.0}]}]}}]}\n\ndata: [DONE]\n\n",
		},
		{
			name:   "logprobs the client asked for",
			intent: LogprobIntent{Keep: true, KeepTop: true},
			body:   "data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"logprobs\":{\"content\":[{\"token\":\"hi\",\"logprob\":-0.1,\"top_logprobs\":[{\"token\":\"h\",\"logprob\":-1.0}]}]}}]}\n\ndata: [DONE]\n\n",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			want := stripResponseBody(assembleSSEBody([]byte(testCase.body)), testCase.intent)

			for _, size := range []int{1, 7, 64, len(testCase.body)} {
				got := foldChunked(t, []byte(testCase.body), testCase.intent, size)
				if !bytes.Equal(got, want) {
					t.Errorf("chunked by %d = %s, want %s", size, got, want)
				}
			}
		})
	}
}

func TestFoldingStopsAtTheEventBudget(t *testing.T) {
	var body bytes.Buffer
	for range maxAssembledEvents + 1 {
		body.WriteString("data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"}}]}\n\n")
	}

	if got := foldChunked(t, body.Bytes(), LogprobIntent{}, 4096); !bytes.Equal(got, TruncatedResponseBody) {
		t.Errorf("body = %s, want the truncation the assembler reports", got)
	}
}
