package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
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
		return cacheKeyFor(request, "qwen", []byte(chatBody), filters.LogprobIntent{}, false)
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

// A stream commits 200 on its first byte, so a race that fails afterwards is recorded as a success
// carrying an SSE error event. Replaying it would freeze one transient host failure for the whole TTL.
func TestAStreamThatFailsAfterItStartedIsNotCached(t *testing.T) {
	live := newHarness(t)
	live.inference.reply = ""
	live.inference.chunks = []string{"data: {\"choices\":[{\"delta\":{\"content\":\"one\"}}]}\n\n"}
	live.inference.err = engine.ErrAllAttemptsFailed
	live.inference.outcome = engine.RaceOutcome{EscrowID: "7"}

	first := newChunkRecorder()
	live.requestInto(t, first, http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))
	live.requestInto(t, newChunkRecorder(), http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))

	if first.status != http.StatusOK {
		t.Fatalf("a started stream must keep its status: got %d, want 200", first.status)
	}
	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races: got %d, want 2 (a failed stream must not be replayed)", got)
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

// A client must not be able to tell a cache replay from a live race by its headers. The two paths
// never meet -- serveCached short-circuits above the race -- so only this test links them.
func TestCachedReplayAndLiveStreamAgreeOnHeaders(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "non_streaming"
		contentType := "application/json"
		if streaming {
			name, contentType = "streaming", "text/event-stream"
		}
		t.Run(name, func(t *testing.T) {
			live := httptest.NewRecorder()
			stream := newClientStream(live, "req-1", streaming, true, filters.LogprobIntent{})
			stream.beginLocked(contentType)
			live.Header().Set(EscrowHeader, "escrow-1")

			replay := httptest.NewRecorder()
			serveCached(replay, "req-1", cachedResponse{
				escrowID:    "escrow-1",
				contentType: contentType,
				stream:      streaming,
				status:      http.StatusOK,
			})

			if !reflect.DeepEqual(live.Header(), replay.Header()) {
				t.Fatalf("live headers %v != replayed headers %v", live.Header(), replay.Header())
			}
		})
	}
}

// The recorder holds one buffer per request in flight for the request's whole life, and put would
// reject anything larger than the cache itself. Recording past that point costs memory for nothing,
// and at full concurrency it is the difference between bounded and not.
func TestCacheRecorderStopsBufferingPastWhatTheCacheCouldStore(t *testing.T) {
	const limit = 4096
	recorder := &cacheRecorder{ResponseWriter: httptest.NewRecorder(), limit: limit}
	chunk := bytes.Repeat([]byte("x"), 1024)

	for range 12 {
		if _, err := recorder.Write(chunk); err != nil {
			t.Fatalf("Write(): %v", err)
		}
	}

	if !recorder.overflowed {
		t.Fatal("recorder kept buffering past the cache's own limit")
	}
	if buffered := recorder.body.Len(); buffered != 0 {
		t.Fatalf("buffered %d bytes after overflow, want the buffer released", buffered)
	}
	if _, storable := recorder.entry("escrow-1", true, nil); storable {
		t.Fatal("an overflowed recording was offered to the cache")
	}
}

func TestCacheRecorderKeepsARecordingInsideTheLimit(t *testing.T) {
	recorder := &cacheRecorder{ResponseWriter: httptest.NewRecorder(), limit: 1 << 20}
	recorder.WriteHeader(http.StatusOK)

	if _, err := recorder.Write([]byte(`data: {"choices":[]}` + "\n\n")); err != nil {
		t.Fatalf("Write(): %v", err)
	}

	if _, storable := recorder.entry("escrow-1", true, nil); !storable {
		t.Fatal("a recording inside the limit was refused")
	}
}

// The force rules make one client's normalized body identical to another's, so the intent has to be
// part of the key: without it a request that asked for logprobs is answered from an entry stripped of
// them, or the reverse hands a client a shape it never asked for.
func TestTheCacheKeySeparatesRequestsByWhatTheyAskedFor(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	request.Header.Set("Authorization", "Bearer one-caller")
	body := []byte(`{"model":"qwen","logprobs":true}`)

	asked := cacheKeyFor(request, "qwen", body, filters.LogprobIntent{Keep: true}, false)
	askedForAlternatives := cacheKeyFor(request, "qwen", body, filters.LogprobIntent{Keep: true, KeepTop: true}, false)
	askedForNeither := cacheKeyFor(request, "qwen", body, filters.LogprobIntent{}, false)

	if asked == askedForNeither {
		t.Fatal("a request that asked for logprobs shares an entry with one that did not")
	}
	if asked == askedForAlternatives {
		t.Fatal("a request that asked for alternatives shares an entry with one that did not")
	}
}

// TestTheCacheSeparatesReplyShapes guards the key against the forced upstream stream: two callers whose
// bodies are now identical must not share an entry when they asked for different reply shapes.
func TestTheCacheSeparatesReplyShapes(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	body := []byte(chatBody)

	buffered := cacheKeyFor(request, "qwen", body, filters.LogprobIntent{}, false)
	streamed := cacheKeyFor(request, "qwen", body, filters.LogprobIntent{}, true)

	if buffered == streamed {
		t.Fatal("a buffered and a streamed caller share one cache key")
	}
}
