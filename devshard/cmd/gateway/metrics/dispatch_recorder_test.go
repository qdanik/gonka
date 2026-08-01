package metrics

import "testing"

// The burn family is asserted end to end against a real burn in cmd/gateway's e2e suite; a hold and an
// exhausted burn budget are per-escrow counts no request-level assertion reaches.
func TestTheDispatchRecorderCountsEachNonceOutcomeSeparately(t *testing.T) {
	telemetry := New()
	recorder := NewDispatchRecorder(telemetry)

	recorder.GhostBurned("7", "poc_unavailable_host")
	recorder.GhostBurned("7", "poc_unavailable_host")
	recorder.GhostBurned("7", "participant_throttled_no_send")
	recorder.NonceHeld("7")
	recorder.BurnBudgetExhausted("9")

	expectCounter(t, telemetry, "devshard_gateway_ghost_nonces_burned_total", labels{"devshard_id": "7", "reason": "poc_unavailable_host"}, 2)
	expectCounter(t, telemetry, "devshard_gateway_ghost_nonces_burned_total", labels{"devshard_id": "7", "reason": "participant_throttled_no_send"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_nonce_holds_total", labels{"devshard_id": "7"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_burn_budget_exhausted_total", labels{"devshard_id": "9"}, 1)
}

// Escrow ids are monotonic chain identifiers and are never reused, so a series that outlives its
// escrow grows with uptime alone. Rotation is the normal case, not an edge one.
func TestDispatchRecorderDropsSeriesWhenAnEscrowRetires(t *testing.T) {
	telemetry := New()
	recorder := NewDispatchRecorder(telemetry)

	recorder.GhostBurned("escrow-1", "poc_unavailable_host")
	recorder.NonceHeld("escrow-1")
	recorder.BurnBudgetExhausted("escrow-1")
	recorder.GhostBurned("escrow-2", "participant_throttled_no_send")

	recorder.EscrowRetired("escrow-1")

	families, err := telemetry.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "devshard_id" && label.GetValue() == "escrow-1" {
					t.Fatalf("%s still carries a series for the retired escrow", family.GetName())
				}
			}
		}
	}
}
