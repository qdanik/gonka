package perf

import (
	"sync"
	"time"
)

// CapabilityToolsUnsupported is the refusal reason for a host that cannot serve tool calls. Routing
// reads it to tell a build refusal from a fault waiting fixes, so it is the one capability reason
// that crosses a package boundary by value.
const CapabilityToolsUnsupported = "tool_choice_unsupported"

// CapabilityVersionUnsupported is the refusal of a host whose build does not speak the escrow's
// protocol version. Waiting does not fix it, so routing skips the host until the verdict goes stale
// rather than backing off from it.
const CapabilityVersionUnsupported = "protocol_version_unsupported"

// capabilityTracker is goroutine-safe: it owns its mutex, unlike hostPerf
// where the caller must hold the lock. Verdicts are dated: a refusal describes a build the host is
// free to replace, so it expires on the same window as the rest of the package's host state.
type capabilityTracker struct {
	mu                 sync.RWMutex
	contextLimits      map[string]contextVerdict
	toolUnsupported    map[string]time.Time
	versionUnsupported map[string]time.Time
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
	}
}

func (c *capabilityTracker) recordContextLimit(participant string, maxTokens uint64, now time.Time) {
	if maxTokens == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.contextLimits[participant] = contextVerdict{limit: maxTokens, observed: now}
}

func (c *capabilityTracker) recordToolUnsupported(participant string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.toolUnsupported[participant] = now
}

func (c *capabilityTracker) recordVersionUnsupported(participant string, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.versionUnsupported[participant] = now
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
