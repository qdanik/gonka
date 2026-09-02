// Package perf tracks per-host health/ejection, in-flight load, and capability flags.
package perf

import (
	"cmp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

// Tracker's mu guards the host map and the rebuild scratch it reuses. See capacity.md, "Outlier ejection".
type Tracker struct {
	mu            sync.Mutex
	config        *config.Holder
	hosts         map[hostKey]*hostState
	liveEjections []liveEjection
	capability    *capabilityTracker
	inflight      *inflightGauge
	now           func() time.Time
	lastSweep     time.Time
	view          atomic.Pointer[map[hostKey]ejectionView]
}

// ejectionView is what a routing decision asks of one host: the capped verdict and the raw one.
type ejectionView struct {
	ejectedUntil  time.Time
	degradedUntil time.Time
}

// liveEjection carries what the cap sorts on, so the comparison never re-searches the map.
type liveEjection struct {
	key           hostKey
	ejectedUntil  time.Time
	ejectionCount int
}

func NewTracker(holder *config.Holder, now func() time.Time) *Tracker {
	return &Tracker{
		config:     holder,
		now:        now,
		hosts:      make(map[hostKey]*hostState),
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
	return host.perf.firstContent.p75(latencyWindowMinimum)
}

func (t *Tracker) TimePerOutputTokenP75(participant, model string) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	host := t.hosts[hostKey{participant: participant, model: model}]
	if host == nil {
		return 0, false
	}
	return host.perf.decode.p75(latencyWindowMinimum)
}

func (t *Tracker) RecordSample(s Sample) {
	now := t.now()
	perf := t.config.Load().Perf
	key := hostKey{participant: s.ParticipantKey, model: s.Model}

	t.mu.Lock()
	defer t.mu.Unlock()

	host := t.ensureHostLocked(key, perf)
	host.perf.recordSample(s, now)
	ejectedUntilBefore := host.ejection.ejectedUntil
	newEjectionPolicyFromPerf(perf).evaluate(&host.perf, &host.ejection, now)

	staleness := time.Duration(perf.HostStalenessSeconds) * time.Second
	evicted := t.evictStaleLocked(now, staleness)
	if evicted || host.ejection.ejectedUntil != ejectedUntilBefore {
		t.rebuildEjectedViewLocked(now, perf)
	}
}

// rebuildEjectedViewLocked republishes both verdicts as one map: every live ejection is degraded, and the per-model cap decides which of them routing actually withholds.
func (t *Tracker) rebuildEjectedViewLocked(now time.Time, perf config.Perf) {
	live := t.liveEjections[:0]
	knownByModel := make(map[string]int, len(t.hosts))
	for key, host := range t.hosts {
		knownByModel[key.model]++
		if host.ejection.ejected(now) {
			live = append(live, liveEjection{key: key, ejectedUntil: host.ejection.ejectedUntil, ejectionCount: host.ejection.ejectionCount})
		}
	}
	t.liveEjections = live

	slices.SortFunc(live, func(first, second liveEjection) int {
		if models := strings.Compare(first.key.model, second.key.model); models != 0 {
			return models
		}
		if rung := cmp.Compare(second.ejectionCount, first.ejectionCount); rung != 0 {
			return rung
		}
		return strings.Compare(first.key.participant, second.key.participant)
	})

	view := make(map[hostKey]ejectionView, len(live))
	allowed, rank := 0, 0
	for index, ejection := range live {
		if index == 0 || ejection.key.model != live[index-1].key.model {
			allowed, rank = maxEjectable(perf, knownByModel[ejection.key.model]), 0
		}
		entry := ejectionView{degradedUntil: ejection.ejectedUntil}
		if rank < allowed {
			entry.ejectedUntil = ejection.ejectedUntil
		}
		view[ejection.key] = entry
		rank++
	}
	t.view.Store(&view)
}

func (t *Tracker) ensureHostLocked(key hostKey, perf config.Perf) *hostState {
	host, ok := t.hosts[key]
	if !ok {
		host = newHostState(time.Duration(perf.EWMAHalfLifeSeconds) * time.Second)
		t.hosts[key] = host
	}
	return host
}

// evictStaleLocked sweeps at most once per tenth of the staleness window. See capacity.md, "Outlier ejection".
func (t *Tracker) evictStaleLocked(now time.Time, staleness time.Duration) bool {
	if now.Sub(t.lastSweep) < staleness/10 {
		return false
	}
	t.lastSweep = now
	evicted := false
	for key, host := range t.hosts {
		if now.Sub(host.perf.lastSeen) > staleness {
			delete(t.hosts, key)
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

// Snapshot returns every tracked pair in participant/model order, in-flight counts read after the host lock.
func (t *Tracker) Snapshot() []HostState {
	now := t.now()
	view := t.view.Load()

	// One pass under one lock: a per-host read would relock and re-search for a report wanting one moment.
	type hostDecode struct {
		key    hostKey
		decode time.Duration
	}
	t.mu.Lock()
	decoded := make([]hostDecode, 0, len(t.hosts))
	for key, host := range t.hosts {
		decode, _ := host.perf.decode.p75(latencyWindowMinimum)
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
			Ejected:            view != nil && now.Before((*view)[host.key].ejectedUntil),
			Inflight:           t.inflight.count(host.key.participant),
			TimePerOutputToken: host.decode,
		})
	}
	return states
}

// Ejected reads the capped view published at rebuild time. See capacity.md, "Outlier ejection".
func (t *Tracker) Ejected(participant, model string) bool {
	return t.now().Before(t.viewOf(participant, model).ejectedUntil)
}

// Degraded is the verdict before the pool-wide cap. See README.md, "Ejection, and its two views".
func (t *Tracker) Degraded(participant, model string) bool {
	return t.now().Before(t.viewOf(participant, model).degradedUntil)
}

func (t *Tracker) viewOf(participant, model string) ejectionView {
	view := t.view.Load()
	if view == nil {
		return ejectionView{}
	}
	return (*view)[hostKey{participant: participant, model: model}]
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

func (t *Tracker) Capability(participant, model string) (contextLimit, versionRefusals, toolRefusals, contextRefusals uint64) {
	return t.capability.capability(participant, model)
}
