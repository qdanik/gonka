package perf

import "sync"

// capabilityTracker is goroutine-safe: it owns its mutex, unlike hostPerf
// where the caller must hold the lock.
type capabilityTracker struct {
	mu              sync.RWMutex
	contextLimits   map[string]uint64
	toolUnsupported map[string]bool
}

func newCapabilityTracker() *capabilityTracker {
	return &capabilityTracker{
		contextLimits:   make(map[string]uint64),
		toolUnsupported: make(map[string]bool),
	}
}

func (c *capabilityTracker) recordContextLimit(participant string, maxTokens uint64) {
	if maxTokens == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.contextLimits[participant] = maxTokens
}

func (c *capabilityTracker) recordToolUnsupported(participant string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.toolUnsupported[participant] = true
}

func (c *capabilityTracker) cannotServe(participant string, requiresTools bool, contextHint uint64) (reason string, blocked bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if requiresTools && c.toolUnsupported[participant] {
		return "tool_choice_unsupported", true
	}
	if limit := c.contextLimits[participant]; limit > 0 && contextHint > limit {
		return "context_limit_exceeded", true
	}
	return "", false
}
