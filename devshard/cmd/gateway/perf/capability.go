package perf

import (
	"sync"
	"time"
)

const CapabilityToolsUnsupported = "tool_choice_unsupported"

const CapabilityVersionUnsupported = "protocol_version_unsupported"

type capabilityTracker struct {
	mu                 sync.RWMutex
	contextLimits      map[string]contextVerdict
	toolUnsupported    map[string]time.Time
	versionUnsupported map[string]time.Time

	versionRefusals map[string]uint64
	toolRefusals    map[string]uint64
	contextRefusals map[string]uint64
}

type contextVerdict struct {
	limit    uint64
	observed time.Time
}

func newCapabilityTracker() *capabilityTracker {
	return &capabilityTracker{
		contextLimits:      make(map[string]contextVerdict),
		toolUnsupported:    make(map[string]time.Time),
		versionUnsupported: make(map[string]time.Time),
		versionRefusals:    make(map[string]uint64),
		toolRefusals:       make(map[string]uint64),
		contextRefusals:    make(map[string]uint64),
	}
}

func (c *capabilityTracker) recordContextLimit(participant string, maxTokens uint64, now time.Time) {
	if maxTokens == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.contextLimits[participant] = contextVerdict{limit: maxTokens, observed: now}
	c.contextRefusals[participant]++
}

func (c *capabilityTracker) recordToolUnsupported(participant string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.toolUnsupported[participant] = now
	c.toolRefusals[participant]++
}

func (c *capabilityTracker) recordVersionUnsupported(participant string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.versionUnsupported[participant] = now
	c.versionRefusals[participant]++
}

func fresh(observed, now time.Time, staleness time.Duration) bool {
	return staleness > 0 && now.Sub(observed) <= staleness
}

func (c *capabilityTracker) cannotServe(
	participant string,
	requiresTools bool,
	contextHint uint64,
	now time.Time,
	staleness time.Duration,
) (reason string, blocked bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if fresh(c.versionUnsupported[participant], now, staleness) {
		return CapabilityVersionUnsupported, true
	}
	if requiresTools && fresh(c.toolUnsupported[participant], now, staleness) {
		return CapabilityToolsUnsupported, true
	}
	if verdict := c.contextLimits[participant]; verdict.limit > 0 &&
		contextHint > verdict.limit &&
		fresh(verdict.observed, now, staleness) {
		return "context_limit_exceeded", true
	}
	return "", false
}

func (c *capabilityTracker) evictStale(now time.Time, staleness time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for participant, observed := range c.versionUnsupported {
		if !fresh(observed, now, staleness) {
			delete(c.versionUnsupported, participant)
		}
	}
	for participant, observed := range c.toolUnsupported {
		if !fresh(observed, now, staleness) {
			delete(c.toolUnsupported, participant)
		}
	}
	for participant, verdict := range c.contextLimits {
		if !fresh(verdict.observed, now, staleness) {
			delete(c.contextLimits, participant)
		}
	}
}

func (c *capabilityTracker) capability(participant string, now time.Time, staleness time.Duration) (versionBlocked, toolBlocked bool, contextLimit, versionRefusals, toolRefusals, contextRefusals uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	versionBlocked = fresh(c.versionUnsupported[participant], now, staleness)
	toolBlocked = fresh(c.toolUnsupported[participant], now, staleness)
	if verdict := c.contextLimits[participant]; fresh(verdict.observed, now, staleness) {
		contextLimit = verdict.limit
	}
	return versionBlocked, toolBlocked, contextLimit,
		c.versionRefusals[participant], c.toolRefusals[participant], c.contextRefusals[participant]
}
