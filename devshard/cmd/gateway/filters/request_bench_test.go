package filters

import "testing"

// The bodies are the ones the legacy gateway's own benchmark uses, so the two sides answer the same
// question on the same input rather than on whatever each happened to pick.
var (
	benchBodyMinimal = []byte(`{"model":"moonshotai/Kimi-K2.6","messages":[{"role":"user","content":"hi"}]}`)
	benchBodyTypical = []byte(`{"model":"moonshotai/Kimi-K2.6","messages":[{"role":"user","content":"hello"}],"temperature":0.7,"top_p":0.95,"max_tokens":512}`)
)

func BenchmarkNormalizeRequest(b *testing.B) {
	for _, testCase := range []struct {
		name string
		body []byte
	}{
		{name: "Minimal", body: benchBodyMinimal},
		{name: "Typical", body: benchBodyTypical},
	} {
		b.Run(testCase.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(testCase.body)))
			for range b.N {
				_, _ = NormalizeRequest(testCase.body, Options{RoutedModel: "moonshotai/Kimi-K2.6"})
			}
		})
	}
}
