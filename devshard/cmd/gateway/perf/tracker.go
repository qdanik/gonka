// Package perf tracks per-host health/ejection, in-flight load, and capability flags.
package perf

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/config"
)

// Tracker composes the per-host performance primitives; sub-components own their locking, so mu only
// guards the host/ejection maps. The two views answer Ejected and Degraded without touching mu or scanning
// every host: the ejection cap is resolved once per state change, while each entry keeps its own expiry so
// time still decides.
type Tracker struct {
	mu           sync.Mutex
	config       *config.Holder
	hosts        map[hostKey]*hostPerf
	ejections    map[hostKey]*ejectionState
	capability   *capabilityTracker
	inflight     *inflightGauge
	now          func() time.Time
	lastSweep    time.Time
	ejectedView  atomic.Pointer[map[hostKey]time.Time]
	degradedView atomic.Pointer[map[hostKey]time.Time]
}

func NewTracker(holder *config.Holder, now func() time.Time) *Tracker {
	return &Tracker{
		config:     holder,
		now:        now,
		hosts:      make(map[hostKey]*hostPerf),
		ejections:  make(map[hostKey]*ejectionState),
		capability: newCapabilityTracker(),
		inflight:   newInflightGauge(),
	}
}

func (t *Tracker) RecordSample(s Sample) {
	now := t.now()
	perf := t.config.Load().Perf
	key := hostKey{participant: s.ParticipantKey, model: s.Model}

	t.mu.Lock()
	defer t.mu.Unlock()

	host, state := t.ensureHostLocked(key, perf)
	host.recordSample(s, now)
	ejectedUntilBefore := state.ejectedUntil
	newEjectionPolicyFromPerf(perf).evaluate(host, state, now)

	evicted := t.evictStaleLocked(now, time.Duration(perf.HostStalenessSeconds)*time.Second)
	// Rebuilding is O(hosts), so it happens only when the membership the cap is computed over
	// actually moved; entries carry their own expiry, so ageing out needs no rebuild.
	if evicted || state.ejectedUntil != ejectedUntilBefore {
		t.rebuildEjectedViewLocked(now, perf)
	}
}

func (t *Tracker) rebuildEjectedViewLocked(now time.Time, perf config.Perf) {
	knownByModel := make(map[string]int, len(t.hosts))
	for key := range t.hosts {
		knownByModel[key.model]++
	}
	ejectedByModel := make(map[string][]hostKey)
	degraded := make(map[hostKey]time.Time, len(t.ejections))
	for key, state := range t.ejections {
		if state.ejected(now) {
			ejectedByModel[key.model] = append(ejectedByModel[key.model], key)
			degraded[key] = state.ejectedUntil
		}
	}

	view := make(map[hostKey]time.Time, len(degraded))
	for model, keys := range ejectedByModel {
		slices.SortFunc(keys, func(a, b hostKey) int { return strings.Compare(a.participant, b.participant) })
		allowed := maxEjectable(perf, knownByModel[model])
		for rank, key := range keys {
			if rank >= allowed {
				break
			}
			view[key] = t.ejections[key].ejectedUntil
		}
	}
	t.ejectedView.Store(&view)
	t.degradedView.Store(&degraded)
}

func (t *Tracker) ensureHostLocked(key hostKey, perf config.Perf) (*hostPerf, *ejectionState) {
	host, ok := t.hosts[key]
	if !ok {
		host = newHostPerf(time.Duration(perf.EWMAHalfLifeSeconds) * time.Second)
		t.hosts[key] = host
	}
	state, ok := t.ejections[key]
	if !ok {
		state = &ejectionState{}
		t.ejections[key] = state
	}
	return host, state
}

// evictStaleLocked sweeps at most once per tenth of the staleness window: entries age out over
// minutes, so scanning every host on every sample costs O(hosts) under the global lock for nothing.
func (t *Tracker) evictStaleLocked(now time.Time, staleness time.Duration) bool {
	if now.Sub(t.lastSweep) < staleness/10 {
		return false
	}
	t.lastSweep = now
	evicted := false
	for key, host := range t.hosts {
		if now.Sub(host.lastSeen) > staleness {
			delete(t.hosts, key)
			delete(t.ejections, key)
			evicted = true
		}
	}
	return evicted
}

func newEjectionPolicyFromPerf(perf config.Perf) ejectionPolicy {
	return newEjectionPolicy(
		int(perf.ConsecutiveFailThreshold),
		perf.FailureRateThreshold,
		perf.FailureRateMinVolume,
		time.Duration(perf.EjectionBaseSeconds)*time.Second,
		time.Duration(perf.EjectionMaxSeconds)*time.Second,
	)
}

func (t *Tracker) RecordContextLimit(participant string, maxTokens uint64) {
	t.capability.recordContextLimit(participant, maxTokens)
}

func (t *Tracker) RecordToolUnsupported(participant string) {
	t.capability.recordToolUnsupported(participant)
}

func (t *Tracker) Acquire(participant string) {
	t.inflight.acquire(participant)
}

func (t *Tracker) Release(participant string) {
	t.inflight.release(participant)
}

// HostState is one tracked participant/model pair as a reader sees it.
type HostState struct {
	Participant string
	Model       string
	Ejected     bool
	Inflight    int
}

// Snapshot returns every tracked pair in participant/model order. The in-flight counts are read after
// the host lock is released, because they live behind their own.
func (t *Tracker) Snapshot() []HostState {
	now := t.now()
	ejected := t.ejectedView.Load()

	t.mu.Lock()
	keys := make([]hostKey, 0, len(t.hosts))
	for key := range t.hosts {
		keys = append(keys, key)
	}
	t.mu.Unlock()

	slices.SortFunc(keys, func(first, second hostKey) int {
		if participants := strings.Compare(first.participant, second.participant); participants != 0 {
			return participants
		}
		return strings.Compare(first.model, second.model)
	})
	states := make([]HostState, 0, len(keys))
	for _, key := range keys {
		states = append(states, HostState{
			Participant: key.participant,
			Model:       key.model,
			Ejected:     ejected != nil && now.Before((*ejected)[key]),
			Inflight:    t.inflight.count(key.participant),
		})
	}
	return states
}

func (t *Tracker) Inflight(participant string) int {
	return t.inflight.count(participant)
}

// Ejected reports whether a host is currently withheld from routing. It is called once per host per
// admission, so it reads the published view instead of scanning: the cap was applied when the view
// was built, and the stored expiry keeps the answer correct as the ejection ages out.
func (t *Tracker) Ejected(participant, model string) bool {
	return ejectedIn(t.ejectedView.Load(), participant, model, t.now())
}

// Degraded reports the ejection verdict before the pool-wide cap, so a host the cap had to leave in
// rotation is still known to be the one the detector wanted out.
func (t *Tracker) Degraded(participant, model string) bool {
	return ejectedIn(t.degradedView.Load(), participant, model, t.now())
}

func ejectedIn(view *map[hostKey]time.Time, participant, model string, now time.Time) bool {
	if view == nil {
		return false
	}
	return now.Before((*view)[hostKey{participant: participant, model: model}])
}

func maxEjectable(perf config.Perf, knownForModel int) int {
	allowed := int(perf.MaxEjectionFraction * float64(knownForModel))
	if byAvailability := knownForModel - int(perf.MinAvailableHosts); byAvailability < allowed {
		allowed = byAvailability
	}
	if allowed < 0 {
		return 0
	}
	return allowed
}

func (t *Tracker) CannotServe(participant string, requiresTools bool, contextHint uint64) (string, bool) {
	return t.capability.cannotServe(participant, requiresTools, contextHint)
}
