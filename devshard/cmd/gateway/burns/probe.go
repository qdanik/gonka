package burns

import (
	"sync"
	"time"
)

const (
	// probeInterval is the floor between two probes to one host. The picker burns throttled ghosts in a
	// tight loop, so without it a busy host would be probed as fast as it refuses.
	probeInterval = 5 * time.Second
	probeTimeout  = 30 * time.Second
)

// probeGate keeps one probe in flight per participant and no faster than probeInterval. A host that is
// merely busy must not be asked to prove it twice at once: the probe is a real request, and a second
// one costs the host the capacity the first is waiting on.
type probeGate struct {
	mu   sync.Mutex
	last map[string]time.Time
	busy map[string]bool
}

func newProbeGate() *probeGate {
	return &probeGate{last: map[string]time.Time{}, busy: map[string]bool{}}
}

func (g *probeGate) enter(participant string, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.busy[participant] || now.Sub(g.last[participant]) < probeInterval {
		return false
	}
	g.busy[participant], g.last[participant] = true, now
	return true
}

func (g *probeGate) leave(participant string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.busy, participant)
}
