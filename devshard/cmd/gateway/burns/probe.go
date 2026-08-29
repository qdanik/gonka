package burns

import (
	"sync"
	"time"
)

const (
	// probeInterval floors the gap between two probes to one host: the picker burns ghosts in a tight loop.
	probeInterval = 5 * time.Second
	probeTimeout  = 30 * time.Second
)

// probeGate keeps one probe in flight per participant and no faster than probeInterval. See README.md, "What it owns".
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
