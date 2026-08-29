package perf

import (
	"sync"
	"time"
)

const CapabilityToolsUnsupported = "tool_choice_unsupported"

// A tool call or a context length is a property of what the model asks for, so those verdicts are
// keyed by model. A protocol version is a property of the build, so that one is not.
type capabilityTracker struct {
	mu              sync.RWMutex
	contextLimits   map[hostKey]contextVerdict
	toolUnsupported map[hostKey]time.Time

	versionRefusals map[string]uint64
	toolRefusals    map[hostKey]uint64
	contextRefusals map[hostKey]uint64
}

type contextVerdict struct {
	limit    uint64
	observed time.Time
}

func newCapabilityTracker() *capabilityTracker {
	return &capabilityTracker{
		contextLimits:   make(map[hostKey]contextVerdict),
		toolUnsupported: make(map[hostKey]time.Time),
		versionRefusals: make(map[string]uint64),
		toolRefusals:    make(map[hostKey]uint64),
		contextRefusals: make(map[hostKey]uint64),
	}
}

func (c *capabilityTracker) recordContextLimit(participant, model string, maxTokens uint64, now time.Time) {
	if maxTokens == 0 {
		return
	}
	served := hostKey{participant: participant, model: model}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.contextLimits[served] = contextVerdict{limit: maxTokens, observed: now}
	c.contextRefusals[served]++
}

func (c *capabilityTracker) recordToolUnsupported(participant, model string, now time.Time) {
	served := hostKey{participant: participant, model: model}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.toolUnsupported[served] = now
	c.toolRefusals[served]++
}

func (c *capabilityTracker) recordVersionUnsupported(participant string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.versionRefusals[participant]++
}

func fresh(observed, now time.Time, staleness time.Duration) bool {
	return staleness > 0 && now.Sub(observed) <= staleness
}

func (c *capabilityTracker) cannotServe(
	participant, model string,
	requiresTools bool,
	contextHint uint64,
	now time.Time,
	staleness time.Duration,
) (reason string, blocked bool) {
	served := hostKey{participant: participant, model: model}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if requiresTools && fresh(c.toolUnsupported[served], now, staleness) {
		return CapabilityToolsUnsupported, true
	}
	if verdict := c.contextLimits[served]; verdict.limit > 0 &&
		contextHint > verdict.limit &&
		fresh(verdict.observed, now, staleness) {
		return "context_limit_exceeded", true
	}
	return "", false
}

func (c *capabilityTracker) evictStale(now time.Time, staleness time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for served, observed := range c.toolUnsupported {
		if !fresh(observed, now, staleness) {
			delete(c.toolUnsupported, served)
		}
	}
	for served, verdict := range c.contextLimits {
		if !fresh(verdict.observed, now, staleness) {
			delete(c.contextLimits, served)
		}
	}
}

func (c *capabilityTracker) capability(participant, model string, now time.Time, staleness time.Duration) (versionBlocked, toolBlocked bool, contextLimit, versionRefusals, toolRefusals, contextRefusals uint64) {
	served := hostKey{participant: participant, model: model}
	c.mu.RLock()
	defer c.mu.RUnlock()
	versionBlocked = c.versionRefusals[participant] > 0
	toolBlocked = fresh(c.toolUnsupported[served], now, staleness)
	if verdict := c.contextLimits[served]; fresh(verdict.observed, now, staleness) {
		contextLimit = verdict.limit
	}
	return versionBlocked, toolBlocked, contextLimit,
		c.versionRefusals[participant], c.toolRefusals[served], c.contextRefusals[served]
}
