package metrics

import "github.com/prometheus/client_golang/prometheus"

// DispatchRecorder satisfies the scheduler's nonce-accounting hook, with a burn, a hold and an
// exhausted burn budget each in their own family. See gateway-operations.md, "Metrics".
type DispatchRecorder struct {
	ghostBurns          *prometheus.CounterVec
	nonceHolds          *prometheus.CounterVec
	burnBudgetExhausted *prometheus.CounterVec
}

func NewDispatchRecorder(telemetry *Metrics) *DispatchRecorder {
	recorder := &DispatchRecorder{
		ghostBurns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_ghost_nonces_burned_total",
			Help: "Total nonces burned as silent ghost probes by escrow, host and reason.",
		}, []string{"devshard_id", "participant", "reason"}),
		nonceHolds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_nonce_holds_total",
			Help: "Total times an escrow parked its next nonce instead of burning or serving it.",
		}, []string{"devshard_id"}),
		burnBudgetExhausted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_burn_budget_exhausted_total",
			Help: "Total drains that stopped because an escrow's per-drain burn budget ran out.",
		}, []string{"devshard_id"}),
	}
	telemetry.Register(recorder.ghostBurns, recorder.nonceHolds, recorder.burnBudgetExhausted)
	return recorder
}

func (r *DispatchRecorder) GhostBurned(escrowID, participant, reason string) {
	r.ghostBurns.WithLabelValues(
		metricLabel(escrowID, labelUnknown),
		metricLabel(participant, labelUnknown),
		metricLabel(reason, labelUnknown),
	).Inc()
}

func (r *DispatchRecorder) NonceHeld(escrowID string) {
	r.nonceHolds.WithLabelValues(metricLabel(escrowID, labelUnknown)).Inc()
}

func (r *DispatchRecorder) BurnBudgetExhausted(escrowID string) {
	r.burnBudgetExhausted.WithLabelValues(metricLabel(escrowID, labelUnknown)).Inc()
}

// EscrowRetired drops every series the escrow left behind. Escrow ids are monotonic and never reused,
// so without this each rotation leaves one nonce-holds series, one budget series and one ghost series
// per reason, permanently -- the sibling registry collector avoids the same growth by rebuilding from
// the live set on every scrape.
func (r *DispatchRecorder) EscrowRetired(escrowID string) {
	labels := prometheus.Labels{"devshard_id": escrowID}
	r.ghostBurns.DeletePartialMatch(labels)
	r.nonceHolds.DeletePartialMatch(labels)
	r.burnBudgetExhausted.DeletePartialMatch(labels)
}
