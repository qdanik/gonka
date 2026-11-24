package mockopenai_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"devshard/testenv/mockopenai"

	"github.com/stretchr/testify/require"
)

func cacheBody(t *testing.T, stream bool, contents ...string) []byte {
	t.Helper()
	messages := make([]map[string]string, 0, len(contents))
	for _, content := range contents {
		messages = append(messages, map[string]string{"role": "user", "content": content})
	}
	body, err := json.Marshal(map[string]any{"model": "test-model", "stream": stream, "messages": messages})
	require.NoError(t, err)
	return body
}

func postCached(t *testing.T, url string, body []byte) (cached int, firstByte time.Duration) {
	t.Helper()
	start := time.Now()
	resp, err := http.Post(url+"/v1/chat/completions", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	one := make([]byte, 1)
	_, err = resp.Body.Read(one)
	require.NoError(t, err)
	firstByte = time.Since(start)
	_, err = io.Copy(io.Discard, resp.Body)
	require.NoError(t, err)
	cached, err = strconv.Atoi(resp.Header.Get(mockopenai.CachedTokensHeader))
	require.NoError(t, err)
	return cached, firstByte
}

func cacheStats(t *testing.T, url string) mockopenai.Stats {
	t.Helper()
	resp, err := http.Get(url + "/testenv/stats")
	require.NoError(t, err)
	defer resp.Body.Close()
	var stats mockopenai.Stats
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	return stats
}

// Test flow:
// 1. Start a mock with a prefix cache and send a 1000-token prompt: nothing is cached and the first token waits for the prefill.
// 2. Send the same prompt extended by a new message: the first prompt's tokens are cached, reported in the header and the stats, and the first token comes sooner.
func TestPrefixCache_ReportsCachedTokensAndCutsPrefill(t *testing.T) {
	server := httptest.NewServer(mockopenai.NewServer(mockopenai.Config{
		PrefixCache: mockopenai.PrefixCacheConfig{CapacityTokens: 100_000, PrefillPerToken: 300 * time.Microsecond},
	}).Handler())
	defer server.Close()

	first := strings.Repeat("a", 4000)
	cached, cold := postCached(t, server.URL, cacheBody(t, true, first))
	require.Zero(t, cached)
	require.GreaterOrEqual(t, cold, 250*time.Millisecond)

	cached, warm := postCached(t, server.URL, cacheBody(t, true, first, strings.Repeat("b", 400)))
	require.GreaterOrEqual(t, cached, 990)
	require.Less(t, warm, cold-150*time.Millisecond)

	stats := cacheStats(t, server.URL)
	require.EqualValues(t, cached, stats.CachedPromptTokens)
	require.Greater(t, stats.PromptTokens, stats.CachedPromptTokens)
}

// Test flow:
// 1. Start a mock whose cache holds about 500 tokens and send prompt A, then prompt B of the same size.
// 2. Send prompt A again: B pushed out A's tail, so only a short leading part is cached.
// 3. Send prompt A a third time: the second send stored it whole, so it is cached.
func TestPrefixCache_EvictsLeastRecentlyUsedPrefixes(t *testing.T) {
	server := httptest.NewServer(mockopenai.NewServer(mockopenai.Config{
		PrefixCache: mockopenai.PrefixCacheConfig{CapacityTokens: 512},
	}).Handler())
	defer server.Close()

	promptA := strings.Repeat("a", 1600)
	promptB := strings.Repeat("b", 1600)
	postCached(t, server.URL, cacheBody(t, true, promptA))
	postCached(t, server.URL, cacheBody(t, true, promptB))

	cached, _ := postCached(t, server.URL, cacheBody(t, true, promptA))
	require.Less(t, cached, 200)

	cached, _ = postCached(t, server.URL, cacheBody(t, true, promptA))
	require.GreaterOrEqual(t, cached, 390)
}

// Test flow:
// 1. Start a mock without a prefix cache and send a streamed request.
// 2. The cached-tokens header is absent and the stats count no prompt tokens.
func TestPrefixCache_OffByDefault(t *testing.T) {
	server := httptest.NewServer(mockopenai.NewServer(mockopenai.Config{}).Handler())
	defer server.Close()

	resp, err := http.Post(server.URL+"/v1/chat/completions", "application/json", bytes.NewReader(cacheBody(t, true, "hello")))
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	require.Empty(t, resp.Header.Get(mockopenai.CachedTokensHeader))
	require.Zero(t, cacheStats(t, server.URL).PromptTokens)
}

// Test flow:
// 1. Start a mock with a request log and send a turn, then the same conversation extended by two messages.
// 2. The log holds one line per streamed request with the same fingerprint, the message counts 1 and 3, and the cached tokens of the second.
func TestPrefixCache_RequestLogKeepsAConversationFingerprint(t *testing.T) {
	var logged bytes.Buffer
	server := httptest.NewServer(mockopenai.NewServer(mockopenai.Config{
		PrefixCache: mockopenai.PrefixCacheConfig{CapacityTokens: 100_000},
		RequestLog:  &logged,
	}).Handler())
	defer server.Close()

	first := strings.Repeat("a", 800)
	postCached(t, server.URL, cacheBody(t, true, first))
	postCached(t, server.URL, cacheBody(t, true, first, "reply", "more"))

	lines := strings.Split(strings.TrimSpace(logged.String()), "\n")
	require.Len(t, lines, 2)
	var one, two struct {
		Fingerprint string `json:"fp"`
		Messages    int    `json:"msgs"`
		Cached      int    `json:"cached"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &one))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &two))
	require.Equal(t, one.Fingerprint, two.Fingerprint)
	require.Equal(t, []int{1, 3}, []int{one.Messages, two.Messages})
	require.Zero(t, one.Cached)
	require.GreaterOrEqual(t, two.Cached, 190)
}

// Test flow:
// 1. Start a mock with a prefix cache and send a streamed validation replay (a request carrying enforced_tokens), then an execution of the same prompt.
// 2. The replay is not counted, so the execution is the only counted request, and it finds the prompt cached by the replay.
func TestPrefixCache_ValidationReplayWarmsTheCacheWithoutCounting(t *testing.T) {
	server := httptest.NewServer(mockopenai.NewServer(mockopenai.Config{
		PrefixCache: mockopenai.PrefixCacheConfig{CapacityTokens: 100_000},
	}).Handler())
	defer server.Close()
	prompt := strings.Repeat("a", 800)
	replay, err := json.Marshal(map[string]any{
		"model": "test-model", "stream": true, "enforced_tokens": map[string]any{"tokens": []int{1}},
		"messages": []map[string]string{{"role": "user", "content": prompt}},
	})
	require.NoError(t, err)

	postCached(t, server.URL, replay)
	require.Zero(t, cacheStats(t, server.URL).PromptTokens)
	cached, _ := postCached(t, server.URL, cacheBody(t, true, prompt))

	stats := cacheStats(t, server.URL)
	require.GreaterOrEqual(t, cached, 190)
	require.EqualValues(t, cached, stats.CachedPromptTokens)
}

// Test flow:
// 1. Send a non-streamed prompt to a mock with a prefix cache and keep its answer.
// 2. Send the conversation again with that answer as an assistant message and a new user message.
// 3. The cached tokens cover the first prompt and its answer, not only the first prompt.
func TestPrefixCache_ReplyBlocksCountAsPrefixOfTheNextTurn(t *testing.T) {
	server := httptest.NewServer(mockopenai.NewServer(mockopenai.Config{
		PrefixCache: mockopenai.PrefixCacheConfig{CapacityTokens: 100_000},
	}).Handler())
	defer server.Close()
	first := strings.Repeat("a", 36)
	resp, err := http.Post(server.URL+"/v1/chat/completions", "application/json", bytes.NewReader(cacheBody(t, false, first)))
	require.NoError(t, err)
	var answer struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&answer))
	resp.Body.Close()
	reply := answer.Choices[0].Message.Content
	require.NotEmpty(t, reply)

	body, err := json.Marshal(map[string]any{"model": "test-model", "stream": true, "messages": []map[string]string{
		{"role": "user", "content": first}, {"role": "assistant", "content": reply}, {"role": "user", "content": strings.Repeat("b", 400)},
	}})
	require.NoError(t, err)
	cached, _ := postCached(t, server.URL, body)

	firstPromptTokens := (len("user") + 1 + len(first) + 1) / 4
	require.Greater(t, cached, firstPromptTokens)
}
