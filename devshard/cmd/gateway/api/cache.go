package api

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
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
	caller string
	escrow string
	model  string
	body   string
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

func cacheKeyFor(r *http.Request, model string, body []byte) cacheKey {
	return cacheKey{
		caller: digest([]byte(strings.TrimSpace(r.Header.Get("Authorization")))),
		escrow: r.PathValue("id"),
		model:  strings.TrimSpace(model),
		body:   digest(body),
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
		return cachedResponse{}, false
	}
	if !entry.expiresAt.After(now) || filters.HasNonCacheableError(entry.body) {
		c.dropLocked(key)
		return cachedResponse{}, false
	}
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

func serveCached(w http.ResponseWriter, requestID string, entry cachedResponse) {
	writeChatHeaders(w.Header(), requestID, entry.escrowID, entry.contentType, entry.stream)
	w.WriteHeader(entry.status)

	controller := http.NewResponseController(w)
	start := 0
	for _, end := range entry.bounds {
		if _, err := w.Write(entry.body[start:end]); err != nil {
			return
		}
		if entry.stream {
			_ = controller.Flush()
		}
		start = end
	}
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
