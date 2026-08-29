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
	cacheEntryOverhead = 256
)

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

func (c *responseCache) stats() (hits, misses, entries, byteSize int64) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits.Load(), c.misses.Load(), int64(len(c.entries)), c.totalBytes
}

// One entry is bounded by what a single reply may hold, not by the whole cache. See README.md.
func (c *responseCache) entryLimit() int64 {
	if c == nil {
		return 0
	}
	return min(c.maxBytes, maxBufferedResponseBytes)
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

// The key carries everything that varies the answer, including what the normalised body no longer
// states. See README.md, "The response cache".
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

// Reports what it managed to write, so a cache hit carries the same delivery record as a race.
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

// Mirrors what the client receives, bounded per request in flight. See README.md, "The response cache".
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
