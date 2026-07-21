// Package perf tracks per-host latency (peak-EWMA), health/ejection, first-token percentiles, and capability flags.
package perf

import (
	"sync"
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
	newEjectionPolicyFromPerf(perf).evaluate(host, state, now)

	// Full scan each call: cheap at this scale, simpler than a sweep cursor.
	t.evictStaleLocked(now, time.Duration(perf.HostStalenessSeconds)*time.Second)
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

func (t *Tracker) evictStaleLocked(now time.Time, staleness time.Duration) {
	for key, host := range t.hosts {
		if now.Sub(host.lastSeen) > staleness {
			delete(t.hosts, key)
			delete(t.ejections, key)
		}
	}
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

// Ejected caps how many hosts of a model report ejected at once (Envoy's
// max_ejection_percent) so failures can't empty the pool; over the cap, a
// host keeps its internal ejected state but stops being reported, ties
// breaking by participant order.
func (t *Tracker) Ejected(participant, model string) bool {
	now := t.now()
	key := hostKey{participant: participant, model: model}

	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.ejections[key]
	if !ok || !state.ejected(now) {
		return false
	}

	perf := t.config.Load().Perf
	knownForModel, rank := 0, 0
	for otherKey := range t.hosts {
		if otherKey.model != model {
			continue
		}
		knownForModel++
		if otherKey == key {
			continue
		}
		if otherState, exists := t.ejections[otherKey]; exists && otherState.ejected(now) && otherKey.participant < participant {
			rank++
		}
	}
	return rank < maxEjectable(perf, knownForModel)
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
