package perf

import "sync"

// capabilityTracker is what a host's build has refused, counted rather than judged: nothing here
// withholds a host from routing. A tool call and a context length are properties of what the model
// asks for, so those are keyed by model; a protocol version is a property of the build, so that one
// is not.
type capabilityTracker struct {
	mu sync.RWMutex

	contextLimits   map[hostKey]uint64
	versionRefusals map[string]uint64
	toolRefusals    map[hostKey]uint64
	contextRefusals map[hostKey]uint64
}

func newCapabilityTracker() *capabilityTracker {
	return &capabilityTracker{
		contextLimits:   make(map[hostKey]uint64),
		versionRefusals: make(map[string]uint64),
		toolRefusals:    make(map[hostKey]uint64),
		contextRefusals: make(map[hostKey]uint64),
	}
}

// The three recorders report whether the observation is new, so their caller can say so once instead
// of on every repeat, and can say it outside the lock.
func (c *capabilityTracker) recordContextLimit(participant, model string, maxTokens uint64) (previous uint64, changed bool) {
	if maxTokens == 0 {
		return 0, false
	}
	served := hostKey{participant: participant, model: model}
	c.mu.Lock()
	defer c.mu.Unlock()
	previous = c.contextLimits[served]
	c.contextLimits[served] = maxTokens
	c.contextRefusals[served]++
	return previous, previous != maxTokens
}

func (c *capabilityTracker) recordToolUnsupported(participant, model string) (first bool) {
	served := hostKey{participant: participant, model: model}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.toolRefusals[served]++
	return c.toolRefusals[served] == 1
}

func (c *capabilityTracker) recordVersionUnsupported(participant string) (first bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.versionRefusals[participant]++
	return c.versionRefusals[participant] == 1
}

// capability reports the refusal counts and the smallest context the host has admitted to.
func (c *capabilityTracker) capability(participant, model string) (contextLimit, versionRefusals, toolRefusals, contextRefusals uint64) {
	served := hostKey{participant: participant, model: model}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.contextLimits[served], c.versionRefusals[participant], c.toolRefusals[served], c.contextRefusals[served]
}
