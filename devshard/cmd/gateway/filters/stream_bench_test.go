package filters

import "testing"

// What a chunk actually looks like in production: the gateway forces logprobs, top_logprobs and
// return_token_ids upstream, so every chunk carries the fields it then has to strip.
const realChunk = `data: {"id":"chatcmpl-1","object":"chat.completion.chunk","created":1700000000,` +
	`"model":"model-a","choices":[{"index":0,"delta":{"content":" the"},"finish_reason":null,` +
	`"logprobs":{"content":[{"token":" the","logprob":-0.31,"bytes":[32,116,104,101],` +
	`"top_logprobs":[{"token":" the","logprob":-0.31},{"token":" a","logprob":-2.1}]}]},` +
	`"token_ids":[262]}]}` + "\n\n"

func BenchmarkStreamRewriterRealChunk(b *testing.B) {
	chunk := []byte(realChunk)
	b.ReportAllocs()
	b.SetBytes(int64(len(chunk)))
	for i := 0; i < b.N; i++ {
		rewriter := NewStreamRewriter()
		if _, err := rewriter.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRewriteEventRealChunk(b *testing.B) {
	event := []byte(realChunk[:len(realChunk)-2])
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = rewriteEvent(event)
	}
}
