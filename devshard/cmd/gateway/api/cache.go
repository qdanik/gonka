package api

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/filters"
)

const (
	cacheEntryTTL      = time.Hour
	cacheSweepInterval = time.Minute
	// cacheEntryOverhead approximates a map bucket, the key digests and the struct fields.
	cacheEntryOverhead = 256
)

// cacheKey pins an entry to the caller and to the escrow scope as well as to the request. See
// gateway-request-lifecycle.md, "5. Response cache".
type cacheKey struct {
	caller   string
	escrow   string
	model    string
	body     string
	logprobs filters.LogprobIntent
	stream   bool
	usage    bool
}

type cachedResponse struct {
	escrowID    string
	stream      bool
	status      int
	contentType string
	body        []byte
	bounds      []int
	expiresAt   time.Time
}

type responseCache struct {
	mu         sync.Mutex
	ttl        time.Duration
	maxBytes   int64
	entries    map[cacheKey]cachedResponse
	totalBytes int64
	lastSweep  time.Time
	hits       atomic.Int64
	misses     atomic.Int64
}

// stats is the only account of whether the cache earns the memory it is given: a hit serves a reply
// without a race, so it appears in no attempt, no nonce and no escrow.
func (c *responseCache) stats() (hits, misses, entries, byteSize int64) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits.Load(), c.misses.Load(), int64(len(c.entries)), c.totalBytes
}

// entryLimit is the largest reply put would accept, so a recorder past it is recording for nothing.
func (c *responseCache) entryLimit() int64 {
	if c == nil {
		return 0
	}
	return c.maxBytes
}

func newResponseCache(maxBytes int64) *responseCache {
	if maxBytes <= 0 {
		return nil
	}
	return &responseCache{
		ttl:      cacheEntryTTL,
		maxBytes: maxBytes,
		entries:  make(map[cacheKey]cachedResponse),
	}
}

// The logprob intent is part of the key because the normalized body is not: the force rules make one
// client's request identical to another's, and the two are answered with differently stripped bodies.
// cacheKeyFor takes the client's streaming and usage intents separately: the body now asks every host
// to stream and to report usage, so two callers wanting different reply shapes would otherwise share
// one entry, and whichever arrived first would decide what the second one got.
func cacheKeyFor(r *http.Request, model string, body []byte, logprobs filters.LogprobIntent, stream, usage bool) cacheKey {
	return cacheKey{
		caller:   digest([]byte(strings.TrimSpace(r.Header.Get("Authorization")))),
		escrow:   r.PathValue("id"),
		model:    strings.TrimSpace(model),
		body:     digest(body),
		logprobs: logprobs,
		stream:   stream,
		usage:    usage,
	}
}

func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}

func (c *responseCache) get(key cacheKey, now time.Time) (cachedResponse, bool) {
	if c == nil {
		return cachedResponse{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, held := c.entries[key]
	if !held {
		c.misses.Add(1)
		return cachedResponse{}, false
	}
	if !entry.expiresAt.After(now) || filters.HasNonCacheableError(entry.body) {
		c.dropLocked(key)
		c.misses.Add(1)
		return cachedResponse{}, false
	}
	c.hits.Add(1)
	return entry, true
}

func (c *responseCache) put(key cacheKey, entry cachedResponse, now time.Time) {
	if c == nil || !filters.IsCacheableResponse(entry.status, entry.body) {
		return
	}
	entry.expiresAt = now.Add(c.ttl)
	size := entrySize(entry)
	if size > c.maxBytes {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweepLocked(now)
	c.dropLocked(key)
	c.entries[key] = entry
	c.totalBytes += size
	c.evictToFitLocked(key)
}

func (c *responseCache) dropLocked(key cacheKey) {
	entry, held := c.entries[key]
	if !held {
		return
	}
	delete(c.entries, key)
	c.totalBytes = max(0, c.totalBytes-entrySize(entry))
}

func (c *responseCache) sweepLocked(now time.Time) {
	if now.Sub(c.lastSweep) < cacheSweepInterval {
		return
	}
	c.lastSweep = now
	for key, entry := range c.entries {
		if !entry.expiresAt.After(now) {
			c.dropLocked(key)
		}
	}
}

// evictToFitLocked drops entries in map order. See gateway-request-lifecycle.md, "5. Response cache".
func (c *responseCache) evictToFitLocked(keep cacheKey) {
	for key := range c.entries {
		if c.totalBytes <= c.maxBytes {
			return
		}
		if key != keep {
			c.dropLocked(key)
		}
	}
}

func entrySize(entry cachedResponse) int64 {
	return int64(len(entry.body)+len(entry.contentType)+len(entry.escrowID)+8*len(entry.bounds)) + cacheEntryOverhead
}

// serveCached reports what it managed to write, so a cache hit carries the same delivery record as a race.
func serveCached(w http.ResponseWriter, requestID string, entry cachedResponse) (written int64) {
	writeChatHeaders(w.Header(), requestID, entry.escrowID, entry.contentType, entry.stream)
	w.WriteHeader(entry.status)

	controller := http.NewResponseController(w)
	start := 0
	for _, end := range entry.bounds {
		sent, err := w.Write(entry.body[start:end])
		written += int64(sent)
		if err != nil {
			return written
		}
		if entry.stream {
			_ = controller.Flush()
		}
		start = end
	}
	return written
}

// cacheRecorder mirrors what the client receives; it keeps the first status the handler chose, so a
// write after the response has begun cannot turn a recorded failure into a 200. limit is what the cache
// could ever store, so a reply that passes it is dropped as it streams rather than held to the end and
// rejected by put -- one buffer per request in flight is the memory this bounds.
type cacheRecorder struct {
	http.ResponseWriter
	limit      int64
	status     int
	body       bytes.Buffer
	bounds     []int
	writeErr   error
	overflowed bool
}

func (w *cacheRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *cacheRecorder) Write(chunk []byte) (int, error) {
	written, err := w.ResponseWriter.Write(chunk)
	if err != nil && w.writeErr == nil {
		w.writeErr = err
	}
	if written > 0 && !w.overflowed {
		if int64(w.body.Len()+written) > w.limit {
			w.overflowed = true
			w.body = bytes.Buffer{}
			w.bounds = nil
		} else {
			w.body.Write(chunk[:written])
			w.bounds = append(w.bounds, w.body.Len())
		}
	}
	return written, err
}

func (w *cacheRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// entry is the recorded reply as a replay would send it. unstorable carries the reasons the recorded
// bytes cannot state themselves: a client that left, and a failure a committed 200 hid.
func (w *cacheRecorder) entry(escrowID string, stream bool, unstorable error) (cachedResponse, bool) {
	if w.overflowed || w.writeErr != nil || unstorable != nil || w.body.Len() == 0 || strings.TrimSpace(escrowID) == "" {
		return cachedResponse{}, false
	}
	return cachedResponse{
		escrowID:    escrowID,
		stream:      stream,
		status:      cmp.Or(w.status, http.StatusOK),
		contentType: w.Header().Get("Content-Type"),
		body:        w.body.Bytes(),
		bounds:      w.bounds,
	}, true
}
