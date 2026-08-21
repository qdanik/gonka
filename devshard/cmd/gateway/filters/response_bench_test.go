package filters

import (
	"fmt"
	"testing"
)

const (
	benchDirtyChoice = `{"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hello"},` +
		`"logprobs":{"content":[{"token":"hello","logprob":-0.5,"top_logprobs":[{"token":"hi","logprob":-1.2}]}]}}]}`
)

func repeatJSON(payload string, count int) []byte {
	if count == 1 {
		return []byte(payload)
	}
	out := make([]byte, 0, (len(payload)+1)*count+2)
	out = append(out, '[')
	for index := range count {
		if index > 0 {
			out = append(out, ',')
		}
		out = append(out, payload...)
	}
	return append(out, ']')
}

func BenchmarkStripResponseBody(b *testing.B) {
	for _, size := range []int{1, 10, 100} {
		payload := repeatJSON(benchDirtyChoice, size)
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = StripResponseBody(payload, LogprobIntent{})
			}
		})
	}
}
