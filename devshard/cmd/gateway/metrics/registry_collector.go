package metrics

import (
	"devshard/cmd/gateway/registry"

	"github.com/prometheus/client_golang/prometheus"
)

type EscrowSource interface {
	Snapshot() []registry.EscrowState
}

// RegistrySources pairs the live escrow set with the readers that answer questions about it, passed
// as functions so the registry keeps no edge to limits.
type RegistrySources struct {
	Escrows            EscrowSource
	EscrowWeight       func(escrowID, model string) float64
	Available          func(participant, model string) bool
	DrainCloseFailures func() int64
}

type RegistryCollector struct {
	sources RegistrySources

	active              *prometheus.Desc
	activeRequests      *prometheus.Desc
	escrowWeight        *prometheus.Desc
	participantLimited  *prometheus.Desc
	blockedParticipants *prometheus.Desc
	drainCloseFailures  *prometheus.Desc
}

func NewRegistryCollector(sources RegistrySources) *RegistryCollector {
	return &RegistryCollector{
		sources:             sources,
		active:              gaugeDesc("devshard_runtime_active", "Whether an escrow runtime is accepting requests.", "devshard_id", "model"),
		activeRequests:      gaugeDesc("devshard_runtime_active_requests", "Requests currently assigned to an escrow runtime.", "devshard_id", "model"),
		escrowWeight:        gaugeDesc("devshard_gateway_escrow_weight", "Per-escrow effective host weight used by the capacity-aware picker.", "devshard_id"),
		participantLimited:  gaugeDesc("devshard_gateway_escrow_participant_limited", "Whether at least one of an escrow's participants is currently unavailable.", "devshard_id", "model"),
		blockedParticipants: gaugeDesc("devshard_gateway_escrow_blocked_participants", "Participants of an escrow that are currently unavailable.", "devshard_id", "model"),
		drainCloseFailures:  counterDesc("devshard_gateway_escrow_drain_close_failures_total", "Drained escrows whose snapshot flush or session close failed."),
	}
}

func (c *RegistryCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		c.active, c.activeRequests, c.escrowWeight, c.participantLimited, c.blockedParticipants,
		c.drainCloseFailures,
	} {
		ch <- desc
	}
}

func (c *RegistryCollector) Collect(ch chan<- prometheus.Metric) {
	if c.sources.DrainCloseFailures != nil {
		counter(ch, c.drainCloseFailures, float64(c.sources.DrainCloseFailures()))
	}
	for _, escrow := range c.sources.Escrows.Snapshot() {
		gauge(ch, c.active, boolGauge(escrow.Accepting), escrow.ID, escrow.Model)
		gauge(ch, c.activeRequests, float64(escrow.InFlight), escrow.ID, escrow.Model)
		if c.sources.EscrowWeight != nil {
			gauge(ch, c.escrowWeight, c.sources.EscrowWeight(escrow.ID, escrow.Model), escrow.ID)
		}
		if c.sources.Available == nil {
			continue
		}
		blocked := 0
		for _, participant := range escrow.Participants {
			if !c.sources.Available(participant, escrow.Model) {
				blocked++
			}
		}
		gauge(ch, c.blockedParticipants, float64(blocked), escrow.ID, escrow.Model)
		gauge(ch, c.participantLimited, boolGauge(blocked > 0), escrow.ID, escrow.Model)
	}
}
