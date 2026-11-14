package api

import "devshard/cmd/gateway/chain"

type nonceRef struct {
	escrowID string
	nonce    uint64
}

func (e cachedResponse) servedRef() nonceRef {
	return nonceRef{escrowID: e.escrowID, nonce: e.nonce}
}

func (e cachedResponse) indexed() bool { return e.nonce != servedNonceNone }

type revokedSet struct {
	refs map[nonceRef]struct{}
	ring []nonceRef
	next int
}

func newRevokedSet() *revokedSet {
	return &revokedSet{refs: make(map[nonceRef]struct{})}
}

func (s *revokedSet) has(ref nonceRef) bool {
	_, revoked := s.refs[ref]
	return revoked
}

func (s *revokedSet) add(ref nonceRef) (fresh, evicted bool) {
	if s.has(ref) {
		return false, false
	}
	if len(s.ring) < revokedLimit {
		s.ring = append(s.ring, ref)
	} else {
		delete(s.refs, s.ring[s.next])
		s.ring[s.next] = ref
		s.next = (s.next + 1) % revokedLimit
		evicted = true
	}
	s.refs[ref] = struct{}{}
	return true, evicted
}

type invalidation struct {
	effect  cacheEffect
	replays uint64
	evicted bool
}

type removal struct {
	dropped int
	replays uint64
}

func (c *responseCache) currentGeneration() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation
}

func (c *responseCache) invalidate(ref nonceRef) invalidation {
	if c == nil {
		return invalidation{effect: cacheEffectAbsent}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := invalidation{effect: cacheEffectAbsent}
	if key, stored := c.byNonce[ref]; stored {
		result.effect, result.replays = cacheEffectDropped, uint64(c.entries[key].hits)
		c.dropLocked(key)
	}
	fresh, evicted := c.revoked.add(ref)
	result.evicted = evicted
	if fresh && result.effect == cacheEffectAbsent {
		result.effect = cacheEffectRevoked
	}
	return result
}

func (c *responseCache) invalidateParticipant(participant string) removal {
	return c.removeWhere(func(entry cachedResponse) bool { return entry.participant == participant })
}

func (c *responseCache) flush() removal {
	return c.removeWhere(func(cachedResponse) bool { return true })
}

func (c *responseCache) removeWhere(matches func(cachedResponse) bool) removal {
	if c == nil {
		return removal{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.generation++
	var result removal
	for key, entry := range c.entries {
		if matches(entry) {
			result.dropped++
			result.replays += uint64(entry.hits)
			c.dropLocked(key)
		}
	}
	return result
}

func (c *responseCache) epochChanged(epoch uint64) bool {
	if c == nil || epoch == chain.EpochIndexUnknown {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	previous := c.epoch
	c.epoch = epoch
	return previous != chain.EpochIndexUnknown && previous != epoch
}
