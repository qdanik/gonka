package api

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/filters"
)

func requestedCacheDirective(header http.Header) cacheDirective {
	directive := cacheDirectiveNone
	for _, value := range header.Values(CacheControlHeader) {
		for token := range strings.SplitSeq(value, directiveListSeparator) {
			name, _, _ := strings.Cut(token, directiveValueSeparator)
			switch cacheDirective(strings.ToLower(strings.TrimSpace(name))) {
			case cacheDirectiveNoStore:
				return cacheDirectiveNoStore
			case cacheDirectiveNoCache:
				directive = cacheDirectiveNoCache
			}
		}
	}
	return directive
}

func (d cacheDirective) looksUp() bool { return d == cacheDirectiveNone }

func (d cacheDirective) stores() bool { return d != cacheDirectiveNoStore }

func writeCacheHitHeaders(header http.Header, age time.Duration) {
	header.Set(CacheStatusHeader, cacheHit)
	header.Set(AgeHeader, strconv.FormatInt(int64(age/time.Second), 10))
}

type cacheLookup struct {
	key   cacheKey
	entry cachedResponse
	age   time.Duration
	hit   bool
}

func (s *Server) lookupCached(r *http.Request, normalized filters.Result, directive cacheDirective) cacheLookup {
	var lookup cacheLookup
	if s.cache == nil {
		return lookup
	}
	if directive.stores() {
		lookup.key = cacheKeyFor(r, normalized)
	}
	if !directive.looksUp() {
		s.countBypass(normalized.Model, directive)
		return lookup
	}
	lookedUpAt := s.now()
	lookup.entry, lookup.hit = s.cache.get(lookup.key, lookedUpAt)
	s.countLookup(normalized.Model, lookup.hit)
	if lookup.hit {
		lookup.age = lookedUpAt.Sub(lookup.entry.storedAt)
		s.countHitAge(lookup.age)
	}
	return lookup
}

func (s *Server) countLookup(model string, hit bool) {
	if s.cacheEvents == nil {
		return
	}
	result := cacheMiss
	if hit {
		result = cacheHit
	}
	s.cacheEvents.Lookup(model, result)
}

func (s *Server) countBypass(model string, directive cacheDirective) {
	if s.cacheEvents != nil {
		s.cacheEvents.Bypassed(model, string(directive))
	}
}

func (s *Server) countHitAge(age time.Duration) {
	if s.cacheEvents != nil {
		s.cacheEvents.HitAge(age)
	}
}

func (s *Server) countInvalidations(reason CacheInvalidationReason, effect cacheEffect, count int, replays uint64) {
	if s.cacheEvents == nil || count == 0 {
		return
	}
	s.cacheEvents.Invalidated(string(reason), string(effect), count)
	s.cacheEvents.InvalidatedReplays(replays)
}

// InvalidateCachedReply removes the cached reply the nonce produced, or keeps the verdict for a reply that has not been stored yet.
func (s *Server) InvalidateCachedReply(escrowID string, nonce uint64, reason CacheInvalidationReason) {
	if s.cache == nil {
		return
	}
	result := s.cache.invalidate(nonceRef{escrowID: escrowID, nonce: nonce})
	s.countInvalidations(reason, result.effect, 1, result.replays)
	if result.evicted && s.cacheEvents != nil {
		s.cacheEvents.RevokedOverflow()
	}
}

// ObservePhase clears the cache when the chain's effective epoch changes.
func (s *Server) ObservePhase(snapshot chain.PhaseSnapshot) {
	if s.cache.epochChanged(snapshot.EffectiveEpochIndex) {
		cleared := s.cache.flush()
		s.countInvalidations(cacheReasonEpoch, cacheEffectDropped, cleared.dropped, cleared.replays)
	}
}

func (s *Server) dropCachedParticipant(participant string) {
	removed := s.cache.invalidateParticipant(participant)
	s.countInvalidations(cacheReasonHostPinned, cacheEffectDropped, removed.dropped, removed.replays)
}
