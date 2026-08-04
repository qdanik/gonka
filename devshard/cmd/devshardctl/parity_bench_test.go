package main

import (
	"fmt"
	"testing"
)

// The fixtures are copied verbatim from the rewrite's own benchmarks so both gateways answer the same
// question on the same bytes. Without that the two numbers are not comparable, only adjacent.
const (
	parityRealChunk = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1700000000,` +
		`"model":"model-a","choices":[{"index":0,"delta":{"content":" the"},"finish_reason":null,` +
		`"logprobs":{"content":[{"token":" the","logprob":-0.31,"bytes":[32,116,104,101],` +
		`"top_logprobs":[{"token":" the","logprob":-0.31},{"token":" a","logprob":-2.1}]}]},` +
		`"token_ids":[262]}]}` + "\n\n"

	parityDirtyChoice = `{"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hello"},` +
		`"logprobs":{"content":[{"token":"hello","logprob":-0.5,"top_logprobs":[{"token":"hi","logprob":-1.2}]}]}}]}`
)

func parityRepeatJSON(payload string, count int) []byte {
	if count == 1 {
		return []byte(payload)
	}
	out := make([]byte, 0, (len(payload)+1)*count+2)
	out = append(out, '[')
	for index := 0; index < count; index++ {
		if index > 0 {
			out = append(out, ',')
		}
		out = append(out, payload...)
	}
	return append(out, ']')
}

func BenchmarkParityStreamRewrite(b *testing.B) {
	chunk := []byte(parityRealChunk)
	intent := logprobClientIntent{}
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	for i := 0; i < b.N; i++ {
		_ = rewriteStreamingPayload(chunk, intent)
	}
}

func BenchmarkParityStripResponseBody(b *testing.B) {
	intent := logprobClientIntent{}
	for _, size := range []int{1, 10, 100} {
		payload := parityRepeatJSON(parityDirtyChoice, size)
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = filterClientInternalFields(payload, intent)
			}
		})
	}
}
