package api

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
)

const streamChatBody = `{"model":"qwen","messages":[{"role":"user","content":"hi"}],"stream":true}`

func callerHeaders(key string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + key}
}

// chunkRecorder keeps every write separate, which httptest.ResponseRecorder concatenates away.
type chunkRecorder struct {
	header  http.Header
	status  int
	chunks  []string
	flushes int
}

func newChunkRecorder() *chunkRecorder { return &chunkRecorder{header: http.Header{}} }

func (w *chunkRecorder) Header() http.Header { return w.header }

func (w *chunkRecorder) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}

func (w *chunkRecorder) Write(chunk []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.chunks = append(w.chunks, string(chunk))
	return len(chunk), nil
}

func (w *chunkRecorder) Flush() { w.flushes++ }

func TestASecondIdenticalRequestFromTheSameCallerIsServedFromTheCache(t *testing.T) {
	live := newHarness(t)

	first := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	second := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if first.Body.String() != second.Body.String() {
		t.Fatalf("replay differs:\n first %q\nsecond %q", first.Body.String(), second.Body.String())
	}
	if got := live.inference.runs.Load(); got != 1 {
		t.Fatalf("races: got %d, want 1 (the second request must be a cache hit)", got)
	}
	if got := second.Header().Get("X-Devshard-ID"); got != "7" {
		t.Fatalf("X-Devshard-ID on the replay: got %q, want the escrow that produced the body", got)
	}
}

func TestTheSameRequestFromADifferentCallerIsAMiss(t *testing.T) {
	live := newHarness(t)

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-b"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races: got %d, want 2 (a second caller must not be served the first caller's reply)", got)
	}
}

func TestAnUnauthenticatedCallerIsNotServedAnAuthenticatedCallersReply(t *testing.T) {
	live := newHarness(t)

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races: got %d, want 2", got)
	}
}

func TestTheSameCallerOnADifferentEscrowIsAMiss(t *testing.T) {
	pinned := func(escrowID string) cacheKey {
		request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		request.Header.Set("Authorization", "Bearer caller-a")
		request.SetPathValue("id", escrowID)
		return cacheKeyFor(request, "qwen", []byte(chatBody))
	}
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	entry := cachedResponse{escrowID: "7", status: http.StatusOK, body: []byte(`{"id":"a"}`), bounds: []int{10}}

	cache.put(pinned("7"), entry, now)

	if _, hit := cache.get(pinned("7"), now); !hit {
		t.Fatal("the escrow that stored the entry must hit")
	}
	if _, hit := cache.get(pinned("9"), now); hit {
		t.Fatal("a request pinned to another devshard was served from this one's entry")
	}
	if _, hit := cache.get(pinned(""), now); hit {
		t.Fatal("an unpinned request was served from a pinned entry")
	}
}

func TestAStreamedReplyReplaysChunkForChunk(t *testing.T) {
	live := newHarness(t)
	live.inference.chunks = []string{
		"data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"two\"}}]}\n\n",
		"data: [DONE]\n\n",
	}

	produced := newChunkRecorder()
	live.requestInto(t, produced, http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))
	replayed := newChunkRecorder()
	live.requestInto(t, replayed, http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 1 {
		t.Fatalf("races: got %d, want 1", got)
	}
	if !slices.Equal(produced.chunks, live.inference.chunks) {
		t.Fatalf("live stream chunks: got %q, want %q", produced.chunks, live.inference.chunks)
	}
	if !slices.Equal(replayed.chunks, produced.chunks) {
		t.Fatalf("replay collapsed the stream: got %d chunks %q, want %d %q",
			len(replayed.chunks), replayed.chunks, len(produced.chunks), produced.chunks)
	}
	if replayed.flushes < len(replayed.chunks) {
		t.Fatalf("replay flushes: got %d, want at least one per chunk (%d)", replayed.flushes, len(replayed.chunks))
	}
	if got := replayed.header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("replayed Content-Type: got %q", got)
	}
}

func TestACacheHitTakesNoLimiterSlotAndNoTokenBudget(t *testing.T) {
	live := newHarness(t)

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	acquiresAfterMiss := live.limiter.acquires.Load()
	tokensAfterMiss := live.limiter.tokens.Load()

	hit := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if hit.Code != http.StatusOK {
		t.Fatalf("cache hit: got %d (%s)", hit.Code, hit.Body.String())
	}
	if acquiresAfterMiss != 1 {
		t.Fatalf("the first request must take one slot: got %d", acquiresAfterMiss)
	}
	if got := live.limiter.acquires.Load(); got != acquiresAfterMiss {
		t.Fatalf("a cache hit took a limiter slot: acquires %d, want %d", got, acquiresAfterMiss)
	}
	if got := live.limiter.tokens.Load(); got != tokensAfterMiss {
		t.Fatalf("a cache hit charged the token budget: in flight %d, want %d", got, tokensAfterMiss)
	}
}

func TestAFailedRaceIsNotCached(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = ""
	live.inference.err = engine.ErrStopped
	live.inference.outcome = engine.RaceOutcome{EscrowID: "7"}

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races: got %d, want 2 (a failure must not be replayed)", got)
	}
}

func TestZeroMaxBytesDisablesTheCache(t *testing.T) {
	live := newHarness(t, func(next *config.Config) { next.Cache.ChatCacheMaxBytes = 0 })

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races: got %d, want 2 (the cache is off)", got)
	}
}

func TestAnEntryPoisonedAfterItWasStoredDropsItselfOnRead(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	key := cacheKey{caller: "a", model: "qwen", body: "b"}
	poisoned := []byte(`data: {"error":{"message":"upstream request timeout","type":"server_error"}}` + "\n\n")

	cache.entries[key] = cachedResponse{escrowID: "7", status: http.StatusOK, body: poisoned, bounds: []int{len(poisoned)}, expiresAt: now.Add(time.Hour)}

	if _, hit := cache.get(key, now); hit {
		t.Fatal("a transient upstream failure was replayed from the cache")
	}
	if len(cache.entries) != 0 {
		t.Fatalf("the poisoned entry survived the read: %d entries left", len(cache.entries))
	}
}

func TestAnExpiredEntryIsAMissAndAnOversizedOneIsNeverStored(t *testing.T) {
	now := time.Unix(1700000000, 0)
	key := cacheKey{caller: "a", model: "qwen", body: "b"}
	body := []byte(`{"id":"a"}`)
	entry := cachedResponse{escrowID: "7", status: http.StatusOK, body: body, bounds: []int{len(body)}}

	expiring := newResponseCache(1 << 20)
	expiring.put(key, entry, now)
	if _, hit := expiring.get(key, now.Add(cacheEntryTTL)); hit {
		t.Fatal("an entry at its expiry was still served")
	}

	tiny := newResponseCache(1)
	tiny.put(key, entry, now)
	if len(tiny.entries) != 0 {
		t.Fatalf("an entry larger than the whole cap was stored: %d entries", len(tiny.entries))
	}
}

func TestTheCapEvictsUntilTheCacheFits(t *testing.T) {
	now := time.Unix(1700000000, 0)
	body := make([]byte, 512)
	cache := newResponseCache(4 * (int64(len(body)) + cacheEntryOverhead))

	for index := range 10 {
		key := cacheKey{caller: "a", model: "qwen", body: string(rune('a' + index))}
		cache.put(key, cachedResponse{escrowID: "7", status: http.StatusOK, body: body, bounds: []int{len(body)}}, now)
	}

	if cache.totalBytes > cache.maxBytes {
		t.Fatalf("retained %d bytes over a %d cap", cache.totalBytes, cache.maxBytes)
	}
	if len(cache.entries) == 0 {
		t.Fatal("eviction emptied the cache instead of making room")
	}
}
