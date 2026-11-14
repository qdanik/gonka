package api

import (
	"net/http"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/engine"
)

const (
	invalidationEscrow = "7"
	winningNonce       = 5
	winningParticipant = "host-a"
)

type cacheEventsSpy struct {
	mu            sync.Mutex
	lookups       []string
	bypasses      []string
	ages          []time.Duration
	invalidations map[string]int
	replays       uint64
	overflows     int
}

func newCacheEventsSpy() *cacheEventsSpy {
	return &cacheEventsSpy{invalidations: map[string]int{}}
}

func (s *cacheEventsSpy) Lookup(model, result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookups = append(s.lookups, model+"/"+result)
}

func (s *cacheEventsSpy) Bypassed(model, directive string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bypasses = append(s.bypasses, model+"/"+directive)
}

func (s *cacheEventsSpy) HitAge(age time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ages = append(s.ages, age)
}

func (s *cacheEventsSpy) Invalidated(reason, effect string, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invalidations[reason+"/"+effect] += count
}

func (s *cacheEventsSpy) InvalidatedReplays(count uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replays += count
}

func (s *cacheEventsSpy) RevokedOverflow() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.overflows++
}

func winnerOutcome() engine.RaceOutcome {
	return engine.RaceOutcome{
		EscrowID:    invalidationEscrow,
		Succeeded:   true,
		WinnerNonce: winningNonce,
		Attempts:    []engine.AttemptOutcome{{Participant: winningParticipant, Nonce: winningNonce}},
	}
}

func keyedByBody(body string) cacheKey {
	return cacheKey{escrow: invalidationEscrow, model: "qwen", body: [32]byte{body[0]}}
}

func storedEntry(nonce uint64, participant string) cachedResponse {
	return cachedResponse{
		escrowID: invalidationEscrow, nonce: nonce, participant: participant,
		status: http.StatusOK, body: []byte(`{"id":"a"}`), bounds: []int{10},
	}
}

func assertIndexMatchesEntries(t *testing.T, cache *responseCache) {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	indexed := 0
	var held int64
	for key, entry := range cache.entries {
		held += entrySize(entry)
		if !entry.indexed() {
			continue
		}
		indexed++
		if cache.byNonce[entry.servedRef()] != key {
			t.Fatalf("entry for nonce %d is not reachable through the index", entry.nonce)
		}
	}
	if len(cache.byNonce) != indexed {
		t.Fatalf("index holds %d rows for %d indexed entries", len(cache.byNonce), indexed)
	}
	if cache.totalBytes != held {
		t.Fatalf("totalBytes = %d, entries are charged %d", cache.totalBytes, held)
	}
}

// Test flow:
//  1. Store two entries that won different nonces of one escrow.
//  2. Invalidate the first by its escrow and nonce.
//  3. Assert the first is gone with its index row, the second still hits, and the bytes charged match what is held.
func TestAnEntryIsRemovedByTheEscrowAndNonceThatWonItsRace(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	cache.put(keyedByBody("a"), storedEntry(winningNonce, winningParticipant), now)
	cache.put(keyedByBody("b"), storedEntry(winningNonce+1, winningParticipant), now)

	result := cache.invalidate(nonceRef{escrowID: invalidationEscrow, nonce: winningNonce})

	if result.effect != cacheEffectDropped {
		t.Fatalf("effect = %q, want %q", result.effect, cacheEffectDropped)
	}
	if _, hit := cache.get(keyedByBody("a"), now); hit {
		t.Fatal("the invalidated entry was still served")
	}
	if _, hit := cache.get(keyedByBody("b"), now); !hit {
		t.Fatal("an entry of another nonce was removed")
	}
	assertIndexMatchesEntries(t, cache)
}

// Test flow:
//  1. Store entries on a one-hour TTL and a cap that fits two.
//  2. Let one expire and be read, evict one by storing past the cap, and replace one by storing under its key again.
//  3. Assert after each step that every index row points at a live entry and totalBytes matches.
func TestExpiryEvictionAndReplacementKeepTheIndexClean(t *testing.T) {
	now := time.Unix(1700000000, 0)
	size := entrySize(storedEntry(1, winningParticipant))
	cache := newResponseCache(2 * size)

	cache.put(keyedByBody("a"), storedEntry(1, winningParticipant), now)
	cache.put(keyedByBody("b"), storedEntry(2, winningParticipant), now)
	assertIndexMatchesEntries(t, cache)

	cache.put(keyedByBody("c"), storedEntry(3, winningParticipant), now)
	assertIndexMatchesEntries(t, cache)
	if len(cache.entries) != 2 {
		t.Fatalf("entries = %d, want 2 after eviction", len(cache.entries))
	}

	cache.put(keyedByBody("c"), storedEntry(4, winningParticipant), now)
	assertIndexMatchesEntries(t, cache)
	if _, stale := cache.byNonce[nonceRef{escrowID: invalidationEscrow, nonce: 3}]; stale {
		t.Fatal("a replaced entry left its index row behind")
	}

	later := now.Add(cacheEntryTTL + time.Second)
	for _, body := range []string{"a", "b", "c"} {
		cache.get(keyedByBody(body), later)
	}
	cache.put(keyedByBody("d"), storedEntry(5, winningParticipant), later.Add(cacheSweepInterval))
	assertIndexMatchesEntries(t, cache)
	if len(cache.entries) != 1 {
		t.Fatalf("entries = %d, want only the fresh one after expiry", len(cache.entries))
	}
}

// Test flow:
//  1. Invalidate a nonce whose reply has not been stored yet.
//  2. Offer that reply to the cache.
//  3. Assert the cache refuses it as invalidated and stores nothing, and a repeated verdict reports it already kept.
func TestAVerdictThatArrivesBeforeThePutStopsTheEntryBeingStored(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	ref := nonceRef{escrowID: invalidationEscrow, nonce: winningNonce}

	first := cache.invalidate(ref)
	refusal := cache.put(keyedByBody("a"), storedEntry(winningNonce, winningParticipant), now)
	second := cache.invalidate(ref)

	if first.effect != cacheEffectRevoked || second.effect != cacheEffectAbsent {
		t.Fatalf("effects = %q then %q, want %q then %q", first.effect, second.effect, cacheEffectRevoked, cacheEffectAbsent)
	}
	if refusal != cacheRefusedInvalidated || len(cache.entries) != 0 {
		t.Fatalf("put = %q with %d entries, want %q and none", refusal, len(cache.entries), cacheRefusedInvalidated)
	}
}

// Test flow:
//  1. Record one verdict more than the revoked set holds, each for a different nonce.
//  2. Assert the overflow was reported once.
//  3. Assert the oldest verdict was forgotten, so its reply is stored, and the newest still refuses its reply.
func TestTheSetOfVerdictsAwaitingTheirReplyIsBounded(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	var overflows int
	for nonce := uint64(1); nonce <= revokedLimit+1; nonce++ {
		if cache.invalidate(nonceRef{escrowID: invalidationEscrow, nonce: nonce}).evicted {
			overflows++
		}
	}

	if overflows != 1 {
		t.Fatalf("overflows = %d, want 1", overflows)
	}
	if refusal := cache.put(keyedByBody("a"), storedEntry(1, winningParticipant), now); refusal == cacheRefusedInvalidated {
		t.Fatal("the oldest verdict outlived the limit")
	}
	if refusal := cache.put(keyedByBody("b"), storedEntry(revokedLimit+1, winningParticipant), now); refusal != cacheRefusedInvalidated {
		t.Fatalf("put under the newest verdict = %q, want %q", refusal, cacheRefusedInvalidated)
	}
}

// Test flow:
//  1. Store entries won by two participants.
//  2. Remove one participant's entries.
//  3. Assert only the other participant's entry remains and the index matches.
func TestRemovingAParticipantDropsOnlyItsEntries(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	cache.put(keyedByBody("a"), storedEntry(1, "host-a"), now)
	cache.put(keyedByBody("b"), storedEntry(2, "host-a"), now)
	cache.put(keyedByBody("c"), storedEntry(3, "host-b"), now)

	removed := cache.invalidateParticipant("host-a")

	if removed.dropped != 2 || len(cache.entries) != 1 {
		t.Fatalf("dropped %d leaving %d entries, want 2 dropped and 1 left", removed.dropped, len(cache.entries))
	}
	if _, hit := cache.get(keyedByBody("c"), now); !hit {
		t.Fatal("the other participant's entry was removed")
	}
	assertIndexMatchesEntries(t, cache)
}

// Test flow:
//  1. Serve an entry twice from the cache, then invalidate it.
//  2. Assert the removal reports the two replays it had already served.
func TestAnInvalidationReportsHowOftenTheEntryWasAlreadyServed(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	cache.put(keyedByBody("a"), storedEntry(winningNonce, winningParticipant), now)
	cache.get(keyedByBody("a"), now)
	cache.get(keyedByBody("a"), now)

	result := cache.invalidate(nonceRef{escrowID: invalidationEscrow, nonce: winningNonce})

	if result.replays != 2 {
		t.Fatalf("replays = %d, want 2", result.replays)
	}
}

// Test flow:
//  1. Observe an effective epoch, fill the cache, and observe the same epoch again, an unknown epoch, and a later one.
//  2. Assert the cache keeps its entry for the first three and is cleared by the last, with the removal counted under the epoch reason.
func TestAnEpochChangeClearsTheCache(t *testing.T) {
	live := newHarness(t)
	events := newCacheEventsSpy()
	live.server.cacheEvents = events
	live.inference.outcome = winnerOutcome()
	live.server.ObservePhase(chain.PhaseSnapshot{EffectiveEpochIndex: 10})
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	live.server.ObservePhase(chain.PhaseSnapshot{EffectiveEpochIndex: 10})
	live.server.ObservePhase(chain.PhaseSnapshot{EffectiveEpochIndex: chain.EpochIndexUnknown})
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	if got := live.inference.runs.Load(); got != 1 {
		t.Fatalf("races = %d, want 1: the entry must survive an unchanged or unknown epoch", got)
	}

	live.server.ObservePhase(chain.PhaseSnapshot{EffectiveEpochIndex: 11})
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races = %d, want 2: a new epoch must clear the cache", got)
	}
	if got := events.invalidations["epoch/dropped"]; got != 1 {
		t.Fatalf("epoch removals counted = %d, want 1", got)
	}
}

// Test flow:
//  1. Fill the cache with a reply won by one participant, then add that participant to the never-trust list through the admin route.
//  2. Send the same request again.
//  3. Assert the admin call succeeded, a second race ran, and the removal was counted under the host_pinned reason.
func TestMarkingAHostSuspiciousRemovesItsCachedReplies(t *testing.T) {
	live := newHarness(t)
	events := newCacheEventsSpy()
	live.server.cacheEvents = events
	live.inference.outcome = winnerOutcome()
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	marked := live.request(t, http.MethodPost, "/v1/admin/suspicious-hosts", `{"participant_key":"`+winningParticipant+`"}`, adminHeaders())
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if marked.Code != http.StatusOK {
		t.Fatalf("marking the host: got %d, want 200", marked.Code)
	}
	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races = %d, want 2: a marked host's reply must not be replayed", got)
	}
	if got := events.invalidations["host_pinned/dropped"]; got != 1 {
		t.Fatalf("host_pinned removals counted = %d, want 1", got)
	}
}

// Test flow:
//  1. Send a chat, serve it once from the cache, then invalidate the nonce that won its race.
//  2. Send the same chat a third time.
//  3. Assert the third request ran a race and the removal reported the one replay already served.
func TestAReplyWhoseNonceWasVotedInvalidIsRacedAgain(t *testing.T) {
	live := newHarness(t)
	events := newCacheEventsSpy()
	live.server.cacheEvents = events
	live.inference.outcome = winnerOutcome()
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	live.server.InvalidateCachedReply(invalidationEscrow, winningNonce, CacheReasonInvalidVerdict)
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races = %d, want 2: the invalidated reply must not be served", got)
	}
	if events.invalidations["invalid_verdict/dropped"] != 1 || events.replays != 1 {
		t.Fatalf("invalidations = %v, replays = %d, want one dropped and one replay", events.invalidations, events.replays)
	}
}

// Test flow:
//  1. Deliver a verdict for the nonce a reply is about to win.
//  2. Send the chat, then send it again.
//  3. Assert the first reply reached its caller, the second request ran its own race, and the cache counted the refusal as invalidated.
func TestAVerdictBeforeTheReplyIsStoredStopsItBeingCached(t *testing.T) {
	live := newHarness(t)
	live.inference.outcome = winnerOutcome()
	live.server.InvalidateCachedReply(invalidationEscrow, winningNonce, CacheReasonInvalidVerdict)

	first := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))
	live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, callerHeaders("caller-a"))

	if first.Code != http.StatusOK || first.Body.Len() == 0 {
		t.Fatalf("first reply = %d %q, want the answer delivered", first.Code, first.Body.String())
	}
	if got := live.inference.runs.Load(); got != 2 {
		t.Fatalf("races = %d, want 2", got)
	}
	live.cached.mu.Lock()
	defer live.cached.mu.Unlock()
	if live.cached.results[0] != "qwen/"+cacheRefusedInvalidated {
		t.Fatalf("cache results = %v, want the first counted as %q", live.cached.results, cacheRefusedInvalidated)
	}
}

// Test flow:
//  1. Read the cache's generation as a request does before its race, then clear the cache and mark a participant.
//  2. Offer the reply the earlier race produced.
//  3. Assert the cache refuses it as invalidated, and accepts a reply from a race that began after the removals.
func TestAReplyFromARaceThatBeganBeforeAClearIsNotStored(t *testing.T) {
	cache := newResponseCache(1 << 20)
	now := time.Unix(1700000000, 0)
	before := cache.currentGeneration()
	cache.flush()
	cache.invalidateParticipant("host-a")

	stale := storedEntry(1, "host-a")
	stale.generation = before
	fresh := storedEntry(2, "host-a")
	fresh.generation = cache.currentGeneration()

	if refusal := cache.put(keyedByBody("a"), stale, now); refusal != cacheRefusedInvalidated {
		t.Fatalf("put from a race begun before the clear = %q, want %q", refusal, cacheRefusedInvalidated)
	}
	if refusal := cache.put(keyedByBody("b"), fresh, now); refusal == cacheRefusedInvalidated {
		t.Fatal("a race begun after the removals was refused")
	}
}
