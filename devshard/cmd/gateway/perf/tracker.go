// Package perf tracks per-host latency (peak-EWMA), health/ejection, first-token percentiles, and capability flags.
package perf

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/config"
)

// Tracker composes the per-host performance primitives; sub-components own
// their locking, so mu only guards the host/ejection maps.
type Tracker struct {
	mu         sync.Mutex
	config     *config.Holder
	hosts      map[hostKey]*hostPerf
	ejections  map[hostKey]*ejectionState
	capability *capabilityTracker
	firstToken *firstTokenReservoir
	inflight   *inflightGauge
	now        func() time.Time
	lastSweep  time.Time
	// ejectedView answers Ejected without touching mu or scanning every host: the ejection cap is
	// resolved once per state change, while each entry keeps its own expiry so time still decides.
	ejectedView atomic.Pointer[map[hostKey]time.Time]
}

func NewTracker(holder *config.Holder, now func() time.Time) *Tracker {
	perf := holder.Load().Perf
	return &Tracker{
		config:     holder,
		now:        now,
		hosts:      make(map[hostKey]*hostPerf),
		ejections:  make(map[hostKey]*ejectionState),
		capability: newCapabilityTracker(),
		firstToken: newFirstTokenReservoir(
			int(perf.FirstTokenReservoir),
			int(perf.FirstTokenActivationSamples),
			perf.FirstTokenPercentile,
			time.Duration(perf.FirstTokenStalenessSeconds)*time.Second,
		),
		inflight: newInflightGauge(),
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

// rebuildEjectedViewLocked applies Envoy's max-ejection-percent cap across the hosts of each model,
// ties breaking by participant order, and publishes the survivors with their expiries.
func (t *Tracker) rebuildEjectedViewLocked(now time.Time, perf config.Perf) {
	knownByModel := make(map[string]int, len(t.hosts))
	for key := range t.hosts {
		knownByModel[key.model]++
	}
	ejectedByModel := make(map[string][]hostKey)
	for key, state := range t.ejections {
		if state.ejected(now) {
			ejectedByModel[key.model] = append(ejectedByModel[key.model], key)
		}
	}

	view := make(map[hostKey]time.Time, len(t.ejections))
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
}

func (t *Tracker) ensureHostLocked(key hostKey, perf config.Perf) (*hostPerf, *ejectionState) {
	host, ok := t.hosts[key]
	if !ok {
		host = newHostPerf(time.Duration(perf.EWMAHalfLifeSeconds)*time.Second, perf.ColdStartReceiptMs, perf.ColdStartCTTFLMsPerToken)
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

func (t *Tracker) RecordFirstToken(model string, inputTokens uint64, firstTokenMs float64) {
	t.firstToken.record(model, inputTokens, firstTokenMs, t.now())
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

func (t *Tracker) Estimate(participant, model string, inputTokens uint64) float64 {
	now := t.now()
	perf := t.config.Load().Perf
	key := hostKey{participant: participant, model: model}

	t.mu.Lock()
	defer t.mu.Unlock()

	if host, ok := t.hosts[key]; ok {
		return host.estimate(inputTokens, now)
	}
	return perf.ColdStartReceiptMs + perf.ColdStartCTTFLMsPerToken*float64(inputTokens)
}

func (t *Tracker) Inflight(participant string) int {
	return t.inflight.count(participant)
}

// Ejected reports whether a host is currently withheld from routing. It is called once per host per
// admission, so it reads the published view instead of scanning: the cap was applied when the view
// was built, and the stored expiry keeps the answer correct as the ejection ages out.
func (t *Tracker) Ejected(participant, model string) bool {
	view := t.ejectedView.Load()
	if view == nil {
		return false
	}
	return t.now().Before((*view)[hostKey{participant: participant, model: model}])
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

func (t *Tracker) FirstTokenP95(model string, inputTokens uint64) (time.Duration, bool) {
	return t.firstToken.p95(model, inputTokens, t.now())
}

func (t *Tracker) CannotServe(participant string, requiresTools bool, contextHint uint64) (string, bool) {
	return t.capability.cannotServe(participant, requiresTools, contextHint)
}
