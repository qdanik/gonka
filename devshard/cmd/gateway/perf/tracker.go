// Package perf tracks per-host health/ejection, in-flight load, and capability flags.
package perf

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

// Tracker composes the per-host performance primitives; sub-components own their locking, so mu only
// guards the host/ejection maps. The two published views answer Ejected and Degraded without taking mu.
// See gateway-capacity-and-health.md, "Outlier ejection".
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

func (t *Tracker) FirstContentP75(participant, model string) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	host := t.hosts[hostKey{participant: participant, model: model}]
	if host == nil {
		return 0, false
	}
	return host.firstContent.p75(latencyWindowMinimum)
}

func (t *Tracker) TimePerOutputTokenP75(participant, model string) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	host := t.hosts[hostKey{participant: participant, model: model}]
	if host == nil {
		return 0, false
	}
	return host.decode.p75(latencyWindowMinimum)
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

	staleness := time.Duration(perf.HostStalenessSeconds) * time.Second
	evicted := t.evictStaleLocked(now, staleness)
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

// evictStaleLocked sweeps at most once per tenth of the staleness window.
// See gateway-capacity-and-health.md, "Outlier ejection".
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

func (t *Tracker) RecordContextLimit(participant, model string, maxTokens uint64) {
	if previous, changed := t.capability.recordContextLimit(participant, model, maxTokens); changed {
		logging.Info("host admitted a context length it will not exceed", logkey.Host, logkey.ShortHost(participant),
			logkey.Model, model, logkey.ContextLimit, maxTokens, logkey.PreviousContextLimit, previous)
	}
}

func (t *Tracker) RecordToolUnsupported(participant, model string) {
	if t.capability.recordToolUnsupported(participant, model) {
		logging.Info("host build does not implement tool calling", logkey.Host, logkey.ShortHost(participant),
			logkey.Model, model)
	}
}

func (t *Tracker) RecordVersionUnsupported(participant string) {
	if t.capability.recordVersionUnsupported(participant) {
		logging.Info("host build cannot serve the escrow's protocol version", logkey.Host, logkey.ShortHost(participant))
	}
}

func (t *Tracker) Acquire(participant string) {
	t.inflight.acquire(participant)
}

func (t *Tracker) Release(participant string) {
	t.inflight.release(participant)
}

// HostState is one tracked participant/model pair as a reader sees it.
type HostState struct {
	Participant        string
	Model              string
	Ejected            bool
	Inflight           int
	TimePerOutputToken time.Duration
}

// Snapshot returns every tracked pair in participant/model order. The in-flight counts are read after
// the host lock is released, because they live behind their own.
func (t *Tracker) Snapshot() []HostState {
	now := t.now()
	ejected := t.ejectedView.Load()

	// One pass under one lock: reading each host's window through TimePerOutputTokenP75 would take the
	// lock again per host and search the map a second time, for a report that wants a single moment.
	type hostDecode struct {
		key    hostKey
		decode time.Duration
	}
	t.mu.Lock()
	decoded := make([]hostDecode, 0, len(t.hosts))
	for key, host := range t.hosts {
		decode, _ := host.decode.p75(latencyWindowMinimum)
		decoded = append(decoded, hostDecode{key: key, decode: decode})
	}
	t.mu.Unlock()

	slices.SortFunc(decoded, func(first, second hostDecode) int {
		if participants := strings.Compare(first.key.participant, second.key.participant); participants != 0 {
			return participants
		}
		return strings.Compare(first.key.model, second.key.model)
	})
	states := make([]HostState, 0, len(decoded))
	for _, host := range decoded {
		states = append(states, HostState{
			Participant:        host.key.participant,
			Model:              host.key.model,
			Ejected:            ejected != nil && now.Before((*ejected)[host.key]),
			Inflight:           t.inflight.count(host.key.participant),
			TimePerOutputToken: host.decode,
		})
	}
	return states
}

// Ejected reports whether a host is currently withheld from routing, reading the capped view published
// at rebuild time rather than scanning. See gateway-capacity-and-health.md, "Outlier ejection".
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

func (t *Tracker) hostStaleness() time.Duration {
	return time.Duration(t.config.Load().Perf.HostStalenessSeconds) * time.Second
}

func (t *Tracker) Capability(participant, model string) (contextLimit, versionRefusals, toolRefusals, contextRefusals uint64) {
	return t.capability.capability(participant, model)
}
