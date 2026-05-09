package participantprovider

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"decentralized-api/apiconfig"
	"decentralized-api/broker"
	"decentralized-api/chainphase"
	"decentralized-api/cosmosclient"
	participantobs "decentralized-api/observability/participant"

	inferencetypes "github.com/productscience/inference/x/inference/types"
)

const ngonkaPerGNK = 1_000_000_000

func NewSnapshotProvider(
	recorder *cosmosclient.InferenceCosmosClient,
	nodeBroker *broker.Broker,
	phaseTracker *chainphase.ChainPhaseTracker,
) func() participantobs.Snapshot {
	var mu sync.Mutex
	var cached participantobs.Snapshot
	var cachedAt time.Time

	return func() participantobs.Snapshot {
		mu.Lock()
		defer mu.Unlock()

		if !cachedAt.IsZero() && time.Since(cachedAt) < 10*time.Second {
			return cached
		}

		cached = collectSnapshot(recorder, nodeBroker, phaseTracker)
		cachedAt = time.Now()
		return cached
	}
}

func collectSnapshot(
	recorder *cosmosclient.InferenceCosmosClient,
	nodeBroker *broker.Broker,
	phaseTracker *chainphase.ChainPhaseTracker,
) participantobs.Snapshot {
	snapshot := participantobs.Snapshot{Status: "Inactive"}
	if recorder == nil || nodeBroker == nil || phaseTracker == nil {
		return snapshot
	}

	participantAddress := nodeBroker.GetParticipantAddress()
	if participantAddress == "" {
		participantAddress = recorder.GetAddress()
	}
	if participantAddress == "" {
		return snapshot
	}

	snapshot.Address = participantAddress

	var currentEpoch uint64
	if epochState := phaseTracker.GetCurrentEpochState(); epochState != nil {
		snapshot.CurrentPhase = epochPhaseLabel(epochState.CurrentPhase)
		currentEpoch = epochState.LatestEpoch.EpochIndex
	}

	baseCtx := recorder.GetContext()
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer cancel()

	queryClient := recorder.NewInferenceQueryClient()

	if participantResp, err := queryClient.Participant(ctx, &inferencetypes.QueryGetParticipantRequest{Index: participantAddress}); err == nil && participantResp != nil {
		participant := participantResp.GetParticipant()
		if participant.GetAddress() != "" || participant.GetValidatorKey() != "" || participant.GetWeight() != 0 {
			if participant.GetAddress() != "" {
				snapshot.Address = participant.GetAddress()
			}
			snapshot.ValidatorKey = participant.GetValidatorKey()
			snapshot.ParticipantStatus = participant.GetStatus().String()
		}
	}

	var participantValidationWeight *inferencetypes.ValidationWeight
	if epochGroupResp, err := queryClient.CurrentEpochGroupData(ctx, &inferencetypes.QueryCurrentEpochGroupDataRequest{}); err == nil && epochGroupResp != nil {
		for _, validationWeight := range epochGroupResp.GetEpochGroupData().ValidationWeights {
			if validationWeight == nil || validationWeight.GetMemberAddress() != participantAddress {
				continue
			}
			participantValidationWeight = validationWeight
			snapshot.ConfirmationWeight = float64(validationWeight.GetConfirmationWeight())
			snapshot.Status = "Active"
			break
		}
	}
	snapshot.EffectiveWeight = deriveEffectiveWeight(participantValidationWeight)

	if currentEpoch > 0 {
		for epoch := currentEpoch; epoch > 0 && len(snapshot.RewardHistory) < 5; epoch-- {
			resp, err := queryClient.EpochPerformanceSummaryByParticipant(ctx, &inferencetypes.QueryEpochPerformanceSummaryByParticipantRequest{
				EpochIndex:    epoch,
				ParticipantId: participantAddress,
			})
			if err != nil || resp == nil {
				continue
			}
			summary := resp.GetEpochPerformanceSummary()
			if summary.GetParticipantId() == "" && summary.GetEpochIndex() == 0 {
				continue
			}
			snapshot.RewardHistory = append(snapshot.RewardHistory, participantobs.RewardSnapshot{
				Epoch:       summary.GetEpochIndex(),
				RewardedGNK: baseUnitsToGNKFloat64(summary.GetRewardedCoins()),
				Claimed:     summary.GetClaimed(),
			})
		}
		sort.Slice(snapshot.RewardHistory, func(i, j int) bool {
			return snapshot.RewardHistory[i].Epoch > snapshot.RewardHistory[j].Epoch
		})
	}

	nodes, _ := nodeBroker.GetNodes()
	govModelsResp, _ := nodeBroker.GetChainBridge().GetGovernanceModels()
	delegationResp, _ := queryClient.PoCDelegation(ctx, &inferencetypes.QueryPoCDelegationRequest{Participant: participantAddress})

	effectiveWeightForNodes := snapshot.EffectiveWeight
	if effectiveWeightForNodes == 0 {
		effectiveWeightForNodes = snapshot.ConfirmationWeight
	}
	snapshot.MLNodes = buildMLNodeSnapshots(nodes, effectiveWeightForNodes)
	snapshot.ModelStatuses = buildModelStatuses(nodes, govModelsResp, delegationResp)

	return snapshot
}

func buildMLNodeSnapshots(nodes []broker.NodeResponse, effectiveWeight float64) []participantobs.MLNodeSnapshot {
	if len(nodes) == 0 {
		return nil
	}

	rawNodeTotal := 0.0
	for _, node := range nodes {
		for _, mlNode := range node.State.EpochMLNodes {
			rawNodeTotal += float64(mlNode.PocWeight)
		}
	}

	effectiveFactor := 0.0
	if effectiveWeight > 0 && rawNodeTotal > 0 {
		effectiveFactor = effectiveWeight / rawNodeTotal
	}

	result := make([]participantobs.MLNodeSnapshot, 0, len(nodes))
	for _, node := range nodes {
		weights := make(map[string]float64, len(node.State.EpochMLNodes))
		effectiveWeights := make(map[string]float64, len(node.State.EpochMLNodes))
		throughputs := make(map[string]float64, len(node.State.EpochMLNodes))
		for modelID, mlNode := range node.State.EpochMLNodes {
			weights[modelID] = float64(mlNode.PocWeight)
			effectiveWeights[modelID] = float64(mlNode.PocWeight) * effectiveFactor
			throughputs[modelID] = float64(mlNode.Throughput)
		}

		result = append(result, participantobs.MLNodeSnapshot{
			NodeID:           node.Node.Id,
			CurrentStatus:    node.State.CurrentStatus.String(),
			IntendedStatus:   node.State.IntendedStatus.String(),
			PocStatus:        string(node.State.PocCurrentStatus),
			ConfiguredModels: sortedModelArgsKeys(node.Node.Models),
			ActiveModels:     sortedNodeActiveModels(node.State),
			PreservedModels:  sortedPreservedModels(node.State.PreservedModels),
			Hardware:         formatHardware(node.Node.Hardware),
			Version:          node.State.MlNodeVersion,
			Weights:          weights,
			EffectiveWeights: effectiveWeights,
			Throughputs:      throughputs,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].NodeID < result[j].NodeID
	})
	return result
}

func buildModelStatuses(
	nodes []broker.NodeResponse,
	govModelsResp *inferencetypes.QueryModelsAllResponse,
	delegationResp *inferencetypes.QueryPoCDelegationResponse,
) []participantobs.ModelStatus {
	configuredLocal := map[string]bool{}
	activeLocal := map[string]bool{}
	preservedLocal := map[string]bool{}

	modelSet := map[string]struct{}{}
	for _, node := range nodes {
		for modelID := range node.Node.Models {
			configuredLocal[modelID] = true
			modelSet[modelID] = struct{}{}
		}
		for modelID := range node.State.EpochModels {
			activeLocal[modelID] = true
			modelSet[modelID] = struct{}{}
		}
		for modelID := range node.State.EpochMLNodes {
			activeLocal[modelID] = true
			modelSet[modelID] = struct{}{}
		}
		for modelID, preserved := range node.State.PreservedModels {
			if !preserved {
				continue
			}
			preservedLocal[modelID] = true
			modelSet[modelID] = struct{}{}
		}
	}

	delegatedTo := map[string]string{}
	refused := map[string]bool{}
	intended := map[string]bool{}
	if delegationResp != nil {
		for _, delegation := range delegationResp.Delegations {
			if delegation == nil || delegation.ModelId == "" {
				continue
			}
			delegatedTo[delegation.ModelId] = delegation.DelegateTo
			modelSet[delegation.ModelId] = struct{}{}
		}
		for _, refusal := range delegationResp.Refusals {
			if refusal == nil || refusal.ModelId == "" {
				continue
			}
			refused[refusal.ModelId] = true
			modelSet[refusal.ModelId] = struct{}{}
		}
		for _, intent := range delegationResp.Intents {
			if intent == nil || intent.ModelId == "" {
				continue
			}
			intended[intent.ModelId] = true
			modelSet[intent.ModelId] = struct{}{}
		}
	}

	if govModelsResp != nil {
		for _, model := range govModelsResp.Model {
			if model.Id == "" {
				continue
			}
			modelSet[model.Id] = struct{}{}
		}
	}

	modelIDs := make([]string, 0, len(modelSet))
	for modelID := range modelSet {
		modelIDs = append(modelIDs, modelID)
	}
	sort.Strings(modelIDs)

	statuses := make([]participantobs.ModelStatus, 0, len(modelIDs))
	for _, modelID := range modelIDs {
		status := "Not Delegated"
		delegateTo := ""
		switch {
		case activeLocal[modelID], preservedLocal[modelID], configuredLocal[modelID], intended[modelID]:
			status = "Covered"
		case delegatedTo[modelID] != "":
			status = "Delegated"
			delegateTo = delegatedTo[modelID]
		case refused[modelID]:
			status = "Not Delegated"
		}

		statuses = append(statuses, participantobs.ModelStatus{
			ModelID:    modelID,
			Status:     status,
			DelegateTo: delegateTo,
		})
	}

	return statuses
}

func deriveEffectiveWeight(validationWeight *inferencetypes.ValidationWeight) float64 {
	if validationWeight == nil {
		return 0
	}

	rawTotal := 0.0
	for _, mlNode := range validationWeight.GetMlNodes() {
		if mlNode == nil {
			continue
		}
		rawTotal += float64(mlNode.GetPocWeight())
	}
	if rawTotal == 0 {
		// Root group ValidationWeights don't carry MlNodes (only subgroups do).
		// ConfirmationWeight is already computed with model coefficients at epoch start
		// and can only decrease via confirmation PoC events.
		return float64(validationWeight.GetConfirmationWeight())
	}

	return float64(validationWeight.GetConfirmationWeight()) * float64(validationWeight.GetWeight()) / rawTotal
}

func sortedModelArgsKeys(models map[string]broker.ModelArgs) string {
	if len(models) == 0 {
		return ""
	}
	keys := make([]string, 0, len(models))
	for modelID := range models {
		keys = append(keys, modelID)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func sortedNodeActiveModels(state broker.NodeState) string {
	modelSet := map[string]struct{}{}
	for modelID := range state.EpochModels {
		modelSet[modelID] = struct{}{}
	}
	for modelID := range state.EpochMLNodes {
		modelSet[modelID] = struct{}{}
	}
	for modelID, preserved := range state.PreservedModels {
		if preserved {
			modelSet[modelID] = struct{}{}
		}
	}
	keys := make([]string, 0, len(modelSet))
	for modelID := range modelSet {
		keys = append(keys, modelID)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func sortedPreservedModels(models map[string]bool) string {
	keys := make([]string, 0, len(models))
	for modelID, preserved := range models {
		if preserved {
			keys = append(keys, modelID)
		}
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func formatHardware(hardware []apiconfig.Hardware) string {
	if len(hardware) == 0 {
		return ""
	}
	parts := make([]string, 0, len(hardware))
	for _, item := range hardware {
		if item.Type == "" {
			continue
		}
		parts = append(parts, item.Type+"x"+strconv.FormatUint(uint64(item.Count), 10))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func baseUnitsToGNKFloat64(value uint64) float64 {
	return float64(value) / ngonkaPerGNK
}

func epochPhaseLabel(phase inferencetypes.EpochPhase) string {
	return string(phase)
}