package participantprovider

import (
	"testing"

	"decentralized-api/broker"

	inferencetypes "github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/require"
)

func TestDeriveEffectiveWeightFromValidationWeight(t *testing.T) {
	// Subgroup case: MlNodes present — scale ConfirmationWeight by validated fraction.
	subgroupWeight := &inferencetypes.ValidationWeight{
		Weight:             200,
		ConfirmationWeight: 1000,
		MlNodes: []*inferencetypes.MLNodeInfo{
			{NodeId: "node-1", PocWeight: 200},
			{NodeId: "node-2", PocWeight: 800},
		},
	}
	require.Equal(t, 200.0, deriveEffectiveWeight(subgroupWeight))

	// Root group case: no MlNodes stored — return ConfirmationWeight directly.
	rootWeight := &inferencetypes.ValidationWeight{
		Weight:             200,
		ConfirmationWeight: 180,
	}
	require.Equal(t, 180.0, deriveEffectiveWeight(rootWeight))
}

func TestBuildModelWeightsUsesValidationWeightNodeMapping(t *testing.T) {
	nodes := []broker.NodeResponse{
		{
			Node: broker.Node{Id: "node-1", Models: map[string]broker.ModelArgs{"model-a": {}}},
			State: broker.NodeState{EpochMLNodes: map[string]inferencetypes.MLNodeInfo{"model-a": {NodeId: "node-1", PocWeight: 200}}},
		},
		{
			Node: broker.Node{Id: "node-2", Models: map[string]broker.ModelArgs{"model-b": {}}},
			State: broker.NodeState{EpochMLNodes: map[string]inferencetypes.MLNodeInfo{"model-b": {NodeId: "node-2", PocWeight: 800}}},
		},
	}
	validationWeight := &inferencetypes.ValidationWeight{
		MlNodes: []*inferencetypes.MLNodeInfo{
			{NodeId: "node-1", PocWeight: 200},
			{NodeId: "node-2", PocWeight: 800},
		},
	}

	weights := buildModelWeights(nodes, validationWeight, 500, 1000)
	require.Len(t, weights, 2)
	require.Equal(t, "model-a", weights[0].ModelID)
	require.Equal(t, 200.0, weights[0].CurrentWeight)
	require.Equal(t, 100.0, weights[0].EffectiveWeight)
	require.Equal(t, 200.0, weights[0].ConfirmationWeight)
	require.Equal(t, "model-b", weights[1].ModelID)
	require.Equal(t, 800.0, weights[1].CurrentWeight)
	require.Equal(t, 400.0, weights[1].EffectiveWeight)
	require.Equal(t, 800.0, weights[1].ConfirmationWeight)
}

func TestBuildMLNodeSnapshotsScalesEffectiveWeights(t *testing.T) {
	nodes := []broker.NodeResponse{
		{
			Node: broker.Node{Id: "node-1", Models: map[string]broker.ModelArgs{"model-a": {}}},
			State: broker.NodeState{CurrentStatus: inferencetypes.HardwareNodeStatus_INFERENCE, EpochMLNodes: map[string]inferencetypes.MLNodeInfo{"model-a": {NodeId: "node-1", PocWeight: 200}}},
		},
		{
			Node: broker.Node{Id: "node-2", Models: map[string]broker.ModelArgs{"model-b": {}}},
			State: broker.NodeState{CurrentStatus: inferencetypes.HardwareNodeStatus_POC, EpochMLNodes: map[string]inferencetypes.MLNodeInfo{"model-b": {NodeId: "node-2", PocWeight: 800}}},
		},
	}

	snapshots := buildMLNodeSnapshots(nodes, 500)
	require.Len(t, snapshots, 2)
	require.Equal(t, 100.0, snapshots[0].EffectiveWeights["model-a"])
	require.Equal(t, 400.0, snapshots[1].EffectiveWeights["model-b"])
}