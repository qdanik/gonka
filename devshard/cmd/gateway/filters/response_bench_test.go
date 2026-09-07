package filters

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"testing"
)

const (
	benchCleanChoice = `{"id":"c","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hello"},` +
		`"scores":{"content":[{"token":"hello","score":-0.5,"alternatives":[{"token":"hi","score":-1.2}]}]}}]}`

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
				_ = stripResponseBody(payload, LogprobIntent{})
			}
		})
	}
}

// A delta with nothing to strip still pays the walk, so this measures the walk rather than the delete.
func BenchmarkDeleteFields(b *testing.B) {
	var decoded any
	if err := stdjson.Unmarshal([]byte(benchCleanChoice), &decoded); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		deleteFields(decoded, clientStrippedFieldSet)
	}
}

// The cache asks this of every reply it stores and of every hit it replays, so its cost is paid twice
// per entry on bodies that reach a megabyte.
func BenchmarkIsCacheableResponse(b *testing.B) {
	events := productionStream(64)
	var body bytes.Buffer
	// Everything but the terminator, then the terminal chunk the fixture leaves out, then the terminator.
	for _, event := range events[:len(events)-1] {
		body.Write(event)
	}
	body.WriteString("data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n")
	body.Write(events[len(events)-1])
	stream := body.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(stream)))
	for b.Loop() {
		if !IsCacheableResponse(200, stream) {
			b.Fatal("the production stream must stay cacheable")
		}
	}
}

// What a cache hit pays: the read path asks only whether the stored reply carries a failure.
func BenchmarkHasNonCacheableError(b *testing.B) {
	events := productionStream(64)
	var body bytes.Buffer
	for _, event := range events {
		body.Write(event)
	}
	stream := body.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(stream)))
	for b.Loop() {
		if HasNonCacheableError(stream) {
			b.Fatal("the production stream carries no failure")
		}
	}
}
