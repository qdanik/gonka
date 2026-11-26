package metrics

import (
	"testing"

	"devshard/cmd/gateway/config"
)

// Test flow:
//  1. Register a `ShareCollector` over one model with two hosts, one of which the ledger does not hold.
//  2. Assert the held host reports its target and offered share, one pass counter per outcome, and the model reports its mode as a number.
func TestTheShareCollectorReportsTargetsPassesAndMode(t *testing.T) {
	telemetry := New()
	telemetry.Register(NewShareCollector(ShareSources{
		Models: func() []string { return []string{"model-a"} },
		Hosts:  func(string) []string { return []string{"host-a", "host-b"} },
		Row: func(participant, model string) (ShareRow, bool) {
			return ShareRow{Participant: participant, Model: model, Target: 0.4, Offered: 0.6, Answered: 5, Missed: 4, Owed: 3, Unvoted: 2}, participant == "host-a"
		},
		Mode: func(string) string { return config.HostLoadModeBalance },
	}))

	expectGauge(t, telemetry, "devshard_gateway_host_load_mode", labels{"model": "model-a"}, 2)
	expectGauge(t, telemetry, "devshard_gateway_participant_target_share", labels{"participant_key": "host-a", "model": "model-a"}, 0.4)
	expectGauge(t, telemetry, "devshard_gateway_participant_offered_share", labels{"participant_key": "host-a", "model": "model-a"}, 0.6)
	for outcome, want := range map[string]float64{"answered": 5, "missed": 4, "owed": 3, "unvoted": 2} {
		expectCounter(t, telemetry, "devshard_gateway_participant_passes_total", labels{"participant_key": "host-a", "model": "model-a", "outcome": outcome}, want)
	}
}
