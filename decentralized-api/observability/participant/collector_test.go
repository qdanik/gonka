package participantobs

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestCollectorCollectsSnapshotMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	collector := &Collector{
		provider: func() Snapshot {
			return Snapshot{
				Address:                "gonka1participant",
				ValidatorKey:           "bls-pubkey",
				Status:                 "Active",
				ParticipantStatus:      "ACTIVE",
				CurrentPhase:           "INFERENCE",
				EffectiveWeight:        80,
				ConfirmationWeight:     55,
				RewardHistory:          []RewardSnapshot{{Epoch: 41, RewardedGNK: 3.5, Claimed: false}},
				ModelStatuses:          []ModelStatus{{ModelID: "kimi", Status: "Delegated", DelegateTo: "gonka1delegate"}},
				MLNodes:                []MLNodeSnapshot{{NodeID: "node-1", CurrentStatus: "INFERENCE", IntendedStatus: "INFERENCE", PocStatus: "IDLE", ConfiguredModels: "kimi", ActiveModels: "kimi", PreservedModels: "", Hardware: "gpux2", Version: "1.0.0", Weights: map[string]float64{"kimi": 12}, EffectiveWeights: map[string]float64{"kimi": 10}, Throughputs: map[string]float64{"kimi": 20}}},
			}
		},
	}

	require.NoError(t, registry.Register(collector))
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	names := make(map[string]struct{}, len(metricFamilies))
	for _, family := range metricFamilies {
		names[family.GetName()] = struct{}{}
	}

	require.Contains(t, names, "decentralized_api_participant_info")
	require.Contains(t, names, "decentralized_api_participant_weight")
	require.Contains(t, names, "decentralized_api_participant_model_status")
	require.Contains(t, names, "decentralized_api_participant_model_runtime_status")
	require.Contains(t, names, "decentralized_api_participant_model_weight")
	require.Contains(t, names, "decentralized_api_participant_mlnode_weight")
	require.Contains(t, names, "decentralized_api_participant_mlnode_effective_weight")
	require.Contains(t, names, "decentralized_api_participant_last_poc_commit_nonce_total")
}