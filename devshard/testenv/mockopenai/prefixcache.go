package mockopenai

import (
	"container/list"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

const (
	cacheBlockBytes    = 64
	cacheBytesPerTok   = 4
	cacheBlockTokens   = cacheBlockBytes / cacheBytesPerTok
	CachedTokensHeader = "X-Mock-Cached-Tokens"
)

// PrefixCacheConfig models a vLLM-style automatic prefix cache for one host; the zero value turns it off.
// Prefill cost applies to the uncached prompt tokens and is added to the stream first-token delay.
type PrefixCacheConfig struct {
	CapacityTokens  int
	PrefillPerToken time.Duration
	// IgnoreReplays keeps validation replays out of the cache, so only executions warm it.
	IgnoreReplays bool
}

func (c PrefixCacheConfig) enabled() bool { return c.CapacityTokens > 0 }

// prefixCache is an LRU of prompt blocks chained by their prefix hash, so a block is a hit only when every block before it is.
type prefixCache struct {
	mu       sync.Mutex
	capacity int
	order    *list.List
	blocks   map[uint64]*list.Element
}

func newPrefixCache(capacityTokens int) *prefixCache {
	return &prefixCache{
		capacity: capacityTokens / cacheBlockTokens,
		order:    list.New(),
		blocks:   make(map[uint64]*list.Element),
	}
}

// renderedTokens is the token count the cache and the prefill cost use: bytes over four, like promptTokenEstimate.
func renderedTokens(rendered string) int { return len(rendered) / cacheBytesPerTok }

// renderPrompt is the byte string a conversation's turns share as a prefix; a later turn's rendering starts with an earlier turn's prompt plus its reply.
func renderPrompt(messages []ChatMessage, reply string) string {
	var b strings.Builder
	for _, m := range messages {
		b.WriteString(m.Role)
		b.WriteByte(0x1f)
		b.WriteString(m.Content)
		b.WriteByte(0x1e)
	}
	if reply != "" {
		b.WriteString("assistant")
		b.WriteByte(0x1f)
		b.WriteString(reply)
		b.WriteByte(0x1e)
	}
	return b.String()
}

func blockChain(rendered string) []uint64 {
	chain := make([]uint64, 0, len(rendered)/cacheBlockBytes)
	var prev uint64
	for off := 0; off+cacheBlockBytes <= len(rendered); off += cacheBlockBytes {
		h := fnv.New64a()
		var seed [8]byte
		for i := range seed {
			seed[i] = byte(prev >> (8 * i))
		}
		_, _ = h.Write(seed[:])
		_, _ = h.Write([]byte(rendered[off : off+cacheBlockBytes]))
		prev = h.Sum64()
		chain = append(chain, prev)
	}
	return chain
}

// lookup returns how many leading prompt tokens are resident, then stores the prompt and reply blocks.
func (p *prefixCache) lookup(prompt, reply string) (cached, total int) {
	chain := blockChain(prompt)
	full := blockChain(prompt + replySuffix(reply))
	p.mu.Lock()
	defer p.mu.Unlock()
	hits := 0
	for _, key := range chain {
		if _, ok := p.blocks[key]; !ok {
			break
		}
		hits++
	}
	p.store(full)
	return hits * cacheBlockTokens, renderedTokens(prompt)
}

func replySuffix(reply string) string {
	if reply == "" {
		return ""
	}
	return renderPrompt(nil, reply)
}

// store touches blocks deepest first so a prefix outlives its tail under eviction.
func (p *prefixCache) store(chain []uint64) {
	for i := len(chain) - 1; i >= 0; i-- {
		if el, ok := p.blocks[chain[i]]; ok {
			p.order.MoveToFront(el)
			continue
		}
		p.blocks[chain[i]] = p.order.PushFront(chain[i])
	}
	for p.order.Len() > p.capacity {
		oldest := p.order.Back()
		delete(p.blocks, oldest.Value.(uint64))
		p.order.Remove(oldest)
	}
}
