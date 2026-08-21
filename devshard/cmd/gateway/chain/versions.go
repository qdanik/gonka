package chain

import (
	"context"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
)

const (
	// Concurrency and per-fetch timeout together keep one pass well inside the freshness window. See
	// gateway-escrow-lifecycle.md, "What the chain observer provides".
	versionsPollConcurrency = 16
	versionsFetchTimeout    = 2 * time.Second
)

type mlnodeVersionEntry struct {
	NodeID                 string `json:"node_id"`
	PoCValidationInference bool   `json:"poc_validation_inference"`
}

type versionsResponse struct {
	MLNodes []mlnodeVersionEntry `json:"mlnodes"`
}

// versionsEntry is one miner's poll result: node id to validation-inference capability, as of fetchedAt.
type versionsEntry struct {
	capableNodes map[string]bool
	fetchedAt    time.Time
}

// VersionsCache polls each candidate miner's dapi /v1/versions and reports per-node
// PoC-validation-inference capability; unknown, errored or stale reads as false. candidates maps a miner to
// its dapi base URL, which is the participant's inference_url.
type VersionsCache struct {
	client *http.Client
	ttl    time.Duration

	mu         sync.RWMutex
	candidates map[string]string
	entries    map[string]versionsEntry
	now        func() time.Time
}

// NewVersionsCache builds a cache that polls through client with the given
// staleness ttl; now is the injectable clock for fetch timestamps and TTL checks.
func NewVersionsCache(client *http.Client, ttl time.Duration, now func() time.Time) *VersionsCache {
	return &VersionsCache{
		client:     client,
		ttl:        ttl,
		candidates: map[string]string{},
		entries:    map[string]versionsEntry{},
		now:        now,
	}
}

// SetCandidates replaces the miner->URL set polled on the next pass and drops
// entries for miners no longer present.
func (c *VersionsCache) SetCandidates(minerURLs map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := make(map[string]string, len(minerURLs))
	for miner, u := range minerURLs {
		if miner == "" || strings.TrimSpace(u) == "" {
			continue
		}
		next[miner] = u
	}
	c.candidates = next
	for miner := range c.entries {
		if _, ok := next[miner]; !ok {
			delete(c.entries, miner)
		}
	}
}

// versionsURL builds the dapi public /v1/versions endpoint from a miner's
// inference base URL (scheme+host[:port]).
func versionsURL(base string) string {
	return strings.TrimSuffix(strings.TrimSpace(base), "/") + "/v1/versions"
}

func (c *VersionsCache) Poll(ctx context.Context) {
	c.mu.RLock()
	candidates := make(map[string]string, len(c.candidates))
	maps.Copy(candidates, c.candidates)
	c.mu.RUnlock()
	if len(candidates) == 0 {
		return
	}

	type target struct{ miner, base string }
	work := make(chan target)
	var workers sync.WaitGroup
	for range min(versionsPollConcurrency, len(candidates)) {
		workers.Go(func() {
			for next := range work {
				if ctx.Err() != nil {
					continue
				}
				fetchCtx, cancelFetch := context.WithTimeout(ctx, versionsFetchTimeout)
				nodes := c.fetchOne(fetchCtx, next.base)
				cancelFetch()
				c.mu.Lock()
				c.entries[next.miner] = versionsEntry{capableNodes: nodes, fetchedAt: c.now()}
				c.mu.Unlock()
			}
		})
	}
	for miner, base := range candidates {
		work <- target{miner: miner, base: base}
	}
	close(work)
	workers.Wait()
}

// fetchOne returns the per-node capability map, or nil on any error (fail-closed).
func (c *VersionsCache) fetchOne(ctx context.Context, base string) map[string]bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, versionsURL(base), nil)
	if err != nil {
		return nil
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil
	}
	var parsed versionsResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil
	}
	nodes := make(map[string]bool, len(parsed.MLNodes))
	for _, n := range parsed.MLNodes {
		id := strings.TrimSpace(n.NodeID)
		if id == "" {
			continue
		}
		nodes[id] = n.PoCValidationInference
	}
	return nodes
}

// IsNodeValidationCapable reports whether the named node of the named miner is
// currently known to be validation-inference capable and the knowledge is fresh.
func (c *VersionsCache) IsNodeValidationCapable(miner, nodeID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.entries[miner]
	if !ok || entry.capableNodes == nil {
		return false
	}
	if c.now().Sub(entry.fetchedAt) > c.ttl {
		return false
	}
	return entry.capableNodes[nodeID]
}

func (c *VersionsCache) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Poll(ctx)
		}
	}
}
