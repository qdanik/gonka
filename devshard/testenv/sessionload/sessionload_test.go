package sessionload_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"devshard/testenv/sessionload"

	"github.com/stretchr/testify/require"
)

type seen struct {
	Key      string `json:"prompt_cache_key"`
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

func echoServer(t *testing.T, failFirst int32) (*httptest.Server, *[]seen, *sync.Mutex) {
	t.Helper()
	var (
		mu       sync.Mutex
		requests []seen
		failures atomic.Int32
	)
	failures.Store(failFirst)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failures.Add(-1) >= 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var got seen
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		mu.Lock()
		requests = append(requests, got)
		n := len(requests)
		mu.Unlock()
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"\"}}]}\n\n")
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"answer-%d\"}}]}\n\ndata: [DONE]\n\n", n)
	}))
	t.Cleanup(server.Close)
	return server, &requests, &mu
}

func baseConfig(url string) sessionload.Config {
	return sessionload.Config{
		URL: url, Model: "m", Seed: 7, Sessions: 1, Turns: 3, Think: time.Millisecond,
		SystemBytes: 200, UserBytes: 100, MaxTokens: 8, Duration: 400 * time.Millisecond, RequestLimit: 5 * time.Second,
	}
}

// Test flow:
// 1. Run one conversation of three turns against a server that echoes an answer.
// 2. Each request carries the previous requests' messages, then the previous answer, then a new user message, so the prompt grows.
// 3. The turns record a first-token time and no error.
func TestRun_EachTurnResendsTheConversation(t *testing.T) {
	server, requests, mu := echoServer(t, 0)
	cfg := baseConfig(server.URL)
	cfg.Duration = 300 * time.Millisecond

	turns := sessionload.Run(context.Background(), cfg)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(*requests), 3)
	first := (*requests)[:3]
	require.Equal(t, []int{2, 4, 6}, []int{len(first[0].Messages), len(first[1].Messages), len(first[2].Messages)})
	require.Equal(t, first[0].Messages[1], first[1].Messages[1])
	require.Equal(t, "answer-1", first[1].Messages[2].Content)
	require.Equal(t, "assistant", first[1].Messages[2].Role)
	require.Greater(t, len(first[2].Messages[0].Content)+len(first[2].Messages[5].Content), 0)
	for _, turn := range turns {
		require.Empty(t, turn.Error)
		require.Positive(t, turn.TTFT)
	}
}

// Test flow:
// 1. Run 40 one-turn conversations at once with a cache-key share of 0.5.
// 2. About half the requests carry prompt_cache_key; the same seed picks the same keyed conversations again.
func TestRun_CacheKeyShareFollowsTheSeed(t *testing.T) {
	server, _, _ := echoServer(t, 0)
	cfg := baseConfig(server.URL)
	cfg.Sessions, cfg.Turns, cfg.CacheKeyShare, cfg.Duration = 40, 1, 0.5, 100*time.Millisecond

	keyed := func() map[string]bool {
		got := map[string]bool{}
		for _, turn := range sessionload.Run(context.Background(), cfg) {
			if strings.HasSuffix(turn.Conversation, "-0") {
				got[turn.Conversation] = turn.Keyed
			}
		}
		return got
	}
	one, two := keyed(), keyed()

	count := 0
	for id, k := range one {
		require.Equal(t, k, two[id])
		if k {
			count++
		}
	}
	require.InDelta(t, 20, count, 8)
}

// Test flow:
// 1. Run one conversation against a server that answers 503 twice before it serves.
// 2. With two retries allowed the first turn succeeds and records two retries.
func TestRun_RetriesUnavailableAnswers(t *testing.T) {
	server, _, _ := echoServer(t, 2)
	cfg := baseConfig(server.URL)
	cfg.Turns, cfg.MaxRetries, cfg.Duration = 1, 2, time.Second

	turns := sessionload.Run(context.Background(), cfg)

	require.NotEmpty(t, turns)
	require.Empty(t, turns[0].Error)
	require.Equal(t, 2, turns[0].Retries)
}
