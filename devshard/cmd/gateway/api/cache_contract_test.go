package api

import (
	"net/http"
	"slices"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

func withCacheControl(value string) map[string]string {
	headers := callerHeaders("caller-a")
	headers[CacheControlHeader] = value
	return headers
}

// Test flow:
//  1. Send a chat, then the same chat a minute later.
//  2. Assert the first reply carries neither cache header and the second carries `X-Devshard-Cache: hit` and `Age: 60`.
//  3. Assert the replayed body equals the first byte for byte and the age was observed.
func TestACacheHitCarriesTheHitAndAgeHeadersAndAFreshReplyCarriesNeither(t *testing.T) {
	live := newHarness(t)
	events := newCacheEventsSpy()
	live.server.cacheEvents = events
	clock := time.Unix(1700000000, 0)
	live.server.now = func() time.Time { return clock }
	live.snapshots.snapshot = chain.PhaseSnapshot{LastUpdatedAt: clock.Add(time.Hour), LastHealthyAt: clock.Add(time.Hour)}

	first := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	clock = clock.Add(time.Minute)
	second := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if first.Header().Get(CacheStatusHeader) != "" || first.Header().Get(AgeHeader) != "" {
		t.Fatalf("fresh reply headers = %v, want no cache headers", first.Header())
	}
	if second.Header().Get(CacheStatusHeader) != "hit" || second.Header().Get(AgeHeader) != "60" {
		t.Fatalf("hit headers = %v, want hit and Age 60", second.Header())
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("replay differs: %q vs %q", first.Body.String(), second.Body.String())
	}
	if !slices.Equal(events.ages, []time.Duration{time.Minute}) {
		t.Fatalf("observed ages = %v, want one minute", events.ages)
	}
	if want := []string{"qwen/miss", "qwen/hit"}; !slices.Equal(events.lookups, want) {
		t.Fatalf("lookups = %v, want %v", events.lookups, want)
	}
}

// Test flow:
//  1. Store a reply, change what the engine answers, and send the same chat with `Cache-Control: no-cache`.
//  2. Assert a second race ran and the fresh answer came back without cache headers.
//  3. Send the chat again without the header and assert the fresh answer is now what the cache serves.
func TestNoCacheSkipsTheLookupAndReplacesTheEntryWithTheFreshAnswer(t *testing.T) {
	live := newHarness(t)
	events := newCacheEventsSpy()
	live.server.cacheEvents = events
	live.inference.outcome = winnerOutcome()
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.inference.reply = `{"id":"fresh","choices":[]}`

	bypassed := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, withCacheControl("no-cache"))
	replay := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races = %d, want 2", got)
	}
	if bypassed.Header().Get(CacheStatusHeader) != "" || bypassed.Header().Get(AgeHeader) != "" {
		t.Fatalf("bypassing request headers = %v, want no cache headers", bypassed.Header())
	}
	if replay.Header().Get(CacheStatusHeader) != "hit" || replay.Body.String() != bypassed.Body.String() {
		t.Fatalf("replay = %v %q, want a hit on the fresh answer %q", replay.Header(), replay.Body.String(), bypassed.Body.String())
	}
	if !slices.Equal(events.bypasses, []string{"qwen/no-cache"}) {
		t.Fatalf("bypasses = %v, want one no-cache", events.bypasses)
	}
}

// Test flow:
//  1. Send a chat with `Cache-Control: no-store`, then the same chat plainly.
//  2. Assert both ran a race, so the first stored nothing, and the cache counted the first as bypassed and the second as stored.
func TestNoStoreSkipsTheLookupAndStoresNothing(t *testing.T) {
	live := newHarness(t)
	events := newCacheEventsSpy()
	live.server.cacheEvents = events

	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, withCacheControl("no-store"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races = %d, want 2", got)
	}
	live.cached.mu.Lock()
	defer live.cached.mu.Unlock()
	if want := []string{"qwen/" + cacheResultBypassed, "qwen/" + cacheResultStored}; !slices.Equal(live.cached.results, want) {
		t.Fatalf("cache results = %v, want %v", live.cached.results, want)
	}
	if !slices.Equal(events.bypasses, []string{"qwen/no-store"}) {
		t.Fatalf("bypasses = %v, want one no-store", events.bypasses)
	}
}

// Test flow:
//  1. Store a reply, then send the same chat with each header value in turn.
//  2. Assert the values that name no-store or no-cache bypass the cache in either case, spelling or position, no-store winning when both appear.
//  3. Assert every other directive is ignored and the reply is served from the cache.
func TestOnlyNoCacheAndNoStoreAreActedOn(t *testing.T) {
	cases := []struct {
		header  string
		bypass  bool
		noStore bool
	}{
		{"no-cache", true, false},
		{"NO-CACHE", true, false},
		{"max-age=0, no-cache", true, false},
		{`no-cache="Set-Cookie"`, true, false},
		{"no-store", true, true},
		{"private, No-Store", true, true},
		{"no-cache, no-store", true, true},
		{"max-age=0", false, false},
		{"private", false, false},
		{"only-if-cached", false, false},
		{"no-transform", false, false},
		{"", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.header, func(t *testing.T) {
			live := newHarness(t)
			live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

			live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, withCacheControl(tc.header))

			wantRaces := int64(1)
			if tc.bypass {
				wantRaces = 2
			}
			if got := live.inference.runs.Load(); got != wantRaces {
				t.Fatalf("Cache-Control %q: races = %d, want %d", tc.header, got, wantRaces)
			}
			live.cached.mu.Lock()
			defer live.cached.mu.Unlock()
			if countedBypassed := live.cached.results[len(live.cached.results)-1] == "qwen/"+cacheResultBypassed; countedBypassed != tc.noStore {
				t.Fatalf("Cache-Control %q: last reply counted bypassed = %v, want %v", tc.header, countedBypassed, tc.noStore)
			}
		})
	}
}

// Test flow:
//  1. Store a reply, then block the chain for PoC generation.
//  2. Assert a request that bypasses the cache is refused with 503 like a miss and starts no race.
//  3. Assert the plain request is still served from the cache.
func TestABypassDuringPoCIsRefusedLikeAMiss(t *testing.T) {
	live := newHarness(t)
	live.swapConfig(func(next *config.Config) { next.Modes.PoCMode = config.PoCModeOff })
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.snapshots.snapshot = chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonPoC, EpochPhase: chain.EpochPhasePoCGenerate}

	bypassed := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, withCacheControl("no-cache"))
	replay := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if bypassed.Code != http.StatusServiceUnavailable {
		t.Fatalf("bypass during PoC: got %d, want 503", bypassed.Code)
	}
	if replay.Code != http.StatusOK {
		t.Fatalf("plain replay during PoC: got %d, want 200", replay.Code)
	}
	if got := live.inference.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1", got)
	}
}

// Test flow:
//  1. Send the same streamed chat twice.
//  2. Assert the stored stream ends with the terminator the gateway wrote and the replay equals the first reply chunk for chunk.
func TestAStoredStreamEndsWithTheGatewaysTerminator(t *testing.T) {
	live := newHarness(t)
	live.inference.chunks = []string{"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n"}

	first := newChunkRecorder()
	live.requestInto(t, first, http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))
	second := newChunkRecorder()
	live.requestInto(t, second, http.MethodPost, "/v1/chat/completions", streamChatBody, callerHeaders("caller-a"))

	if len(first.chunks) == 0 || first.chunks[len(first.chunks)-1] != "data: [DONE]\n\n" {
		t.Fatalf("live stream chunks = %q, want the gateway's terminator last", first.chunks)
	}
	if !slices.Equal(first.chunks, second.chunks) {
		t.Fatalf("replay chunks %q != live chunks %q", second.chunks, first.chunks)
	}
}
