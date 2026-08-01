package chain

import (
	"fmt"
	"maps"
	"sort"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"
)

type epochInfo struct {
	BlockHeight                  int64
	Phase                        EpochPhase
	EpochIndex                   uint64
	PoCStartBlockHeight          int64
	EpochSwitchBlockHeight       int64
	IsConfirmationPoCActive      bool
	ConfirmationPoCPhase         ConfirmationPoCPhase
	ConfirmationPoCTriggerHeight int64
}

type chainEpochInfoResponse struct {
	BlockHeight             jsonInt64                         `json:"block_height"`
	Phase                   string                            `json:"phase"`
	LatestEpoch             chainLatestEpoch                  `json:"latest_epoch"`
	EpochStages             chainEpochStages                  `json:"epoch_stages"`
	NextEpochStages         chainEpochStages                  `json:"next_epoch_stages"`
	IsConfirmationPoCActive bool                              `json:"is_confirmation_poc_active"`
	ActiveConfirmationPoC   *chainConfirmationPoCEventPayload `json:"active_confirmation_poc_event,omitempty"`
}

type chainLatestEpoch struct {
	Index               jsonUint64 `json:"index"`
	PocStartBlockHeight jsonInt64  `json:"poc_start_block_height"`
}

type chainEpochStages struct {
	SetNewValidators jsonInt64 `json:"set_new_validators"`
	NextPoCStart     jsonInt64 `json:"next_poc_start"`
}

type chainConfirmationPoCEventPayload struct {
	Phase         confirmationPoCPhaseValue `json:"phase"`
	TriggerHeight jsonInt64                 `json:"trigger_height"`
}

func parseEpochInfo(body []byte) (epochInfo, error) {
	var payload chainEpochInfoResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return epochInfo{}, fmt.Errorf("parse epoch info: %w", err)
	}

	info := epochInfo{
		BlockHeight:             int64(payload.BlockHeight),
		Phase:                   EpochPhase(strings.TrimSpace(payload.Phase)),
		EpochIndex:              uint64(payload.LatestEpoch.Index),
		PoCStartBlockHeight:     int64(payload.LatestEpoch.PocStartBlockHeight),
		EpochSwitchBlockHeight:  deriveEpochSwitchBlockHeight(payload),
		IsConfirmationPoCActive: payload.IsConfirmationPoCActive,
	}
	if payload.ActiveConfirmationPoC != nil {
		info.ConfirmationPoCPhase = ConfirmationPoCPhase(payload.ActiveConfirmationPoC.Phase)
		info.ConfirmationPoCTriggerHeight = int64(payload.ActiveConfirmationPoC.TriggerHeight)
	}
	return info, nil
}

func deriveEpochSwitchBlockHeight(payload chainEpochInfoResponse) int64 {
	blockHeight := int64(payload.BlockHeight)
	if payload.EpochStages.SetNewValidators > 0 && int64(payload.EpochStages.SetNewValidators) >= blockHeight {
		return int64(payload.EpochStages.SetNewValidators)
	}
	if payload.NextEpochStages.SetNewValidators > 0 {
		return int64(payload.NextEpochStages.SetNewValidators)
	}
	if payload.EpochStages.NextPoCStart > 0 {
		return int64(payload.EpochStages.NextPoCStart)
	}
	return int64(payload.LatestEpoch.PocStartBlockHeight)
}

type participantNode struct {
	Model  string
	NodeID string
	Weight float64
}

// participantsState is the parsed current-participants response, keyed throughout by gonka address.
type participantsState struct {
	Weights            map[string]float64
	WeightsByModel     map[string]map[string]float64
	FullWeights        map[string]float64
	FullWeightsByModel map[string]map[string]float64
	Preserved          []string
	PreservedByModel   map[string][]string
	Excluded           []string
	InferenceURLs      map[string]string
	NodesByParticipant map[string][]participantNode
}

type chainCurrentParticipantsResponse struct {
	ActiveParticipants chainActiveParticipantsGroup `json:"active_participants"`
}

type chainActiveParticipantsGroup struct {
	Participants []chainActiveParticipant `json:"participants"`
}

type chainActiveParticipant struct {
	Index        string              `json:"index"`
	InferenceURL string              `json:"inference_url"`
	Models       []string            `json:"models,omitempty"`
	MLNodes      []chainModelMLNodes `json:"ml_nodes"`
}

// chainModelMLNodes is the raw per-model-slot ML node list (indexed in parallel with Models).
type chainModelMLNodes struct {
	MLNodes []chainMLNodeInfo `json:"ml_nodes"`
}

type chainMLNodeInfo struct {
	NodeID             string     `json:"node_id"`
	TimeslotAllocation []bool     `json:"timeslot_allocation"`
	PoCWeight          jsonUint64 `json:"poc_weight,omitempty"`
}

// preservationMode selects which ML nodes count as preserved when folding participants.
type preservationMode int

const (
	// preservationModeLegacy keeps nodes whose second timeslot allocation is set.
	preservationModeLegacy preservationMode = iota
	// preservationModeSnapshot keeps nodes listed in the preserved-nodes snapshot.
	preservationModeSnapshot
	// preservationModeAll keeps every node.
	preservationModeAll
)

// parseParticipants parses the current-participants response body, partitioning nodes under the
// given preservation rule; preservedNodes backs preservationModeSnapshot membership checks.
func parseParticipants(body []byte, preservation preservationMode, preservedNodes preservedSnapshotState) (participantsState, error) {
	var payload chainCurrentParticipantsResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return participantsState{}, fmt.Errorf("parse participants: %w", err)
	}

	participants := payload.ActiveParticipants.Participants
	state := participantsState{
		Weights:            make(map[string]float64, len(participants)),
		WeightsByModel:     make(map[string]map[string]float64),
		FullWeights:        make(map[string]float64, len(participants)),
		FullWeightsByModel: make(map[string]map[string]float64),
		PreservedByModel:   make(map[string][]string),
		InferenceURLs:      make(map[string]string, len(participants)),
		NodesByParticipant: make(map[string][]participantNode, len(participants)),
	}

	seenPreserved := make(map[string]struct{}, len(participants))
	seenPreservedByModel := make(map[string]map[string]struct{})
	seenExcluded := make(map[string]struct{}, len(participants))

	for _, participant := range participants {
		key := strings.TrimSpace(participant.Index)
		if key == "" {
			continue
		}
		if url := strings.TrimSpace(participant.InferenceURL); url != "" {
			state.InferenceURLs[key] = url
		}
		if nodes := extractParticipantNodes(participant); len(nodes) > 0 {
			state.NodesByParticipant[key] = nodes
		}

		state.Weights[key] = participantWeight(participant, preservation, preservedNodes)
		state.FullWeights[key] = participantWeight(participant, preservationModeAll, preservedSnapshotState{})
		addModelWeights(state.WeightsByModel, key, participantWeightsByModel(participant, preservation, preservedNodes))
		addModelWeights(state.FullWeightsByModel, key, participantWeightsByModel(participant, preservationModeAll, preservedSnapshotState{}))

		if participantHasPreservedNode(participant, preservation, preservedNodes) {
			if _, ok := seenPreserved[key]; !ok {
				seenPreserved[key] = struct{}{}
				state.Preserved = append(state.Preserved, key)
			}
			for _, model := range preservedModelsForParticipant(participant, preservation, preservedNodes) {
				modelSeen := seenPreservedByModel[model]
				if modelSeen == nil {
					modelSeen = map[string]struct{}{}
					seenPreservedByModel[model] = modelSeen
				}
				if _, ok := modelSeen[key]; ok {
					continue
				}
				modelSeen[key] = struct{}{}
				state.PreservedByModel[model] = append(state.PreservedByModel[model], key)
			}
			continue
		}
		if _, ok := seenExcluded[key]; !ok {
			seenExcluded[key] = struct{}{}
			state.Excluded = append(state.Excluded, key)
		}
	}
	sort.Strings(state.Preserved)
	for model := range state.PreservedByModel {
		sort.Strings(state.PreservedByModel[model])
	}
	return state, nil
}

func addModelWeights(dst map[string]map[string]float64, key string, weights map[string]float64) {
	for model, weight := range weights {
		modelWeights := dst[model]
		if modelWeights == nil {
			modelWeights = map[string]float64{}
			dst[model] = modelWeights
		}
		modelWeights[key] = weight
	}
}

func nodePreserved(participantID, model string, node chainMLNodeInfo, preservation preservationMode, preservedNodes preservedSnapshotState) bool {
	switch preservation {
	case preservationModeAll:
		return true
	case preservationModeSnapshot:
		return preservedNodes.Has(model, participantID, node.NodeID)
	default:
		return len(node.TimeslotAllocation) > 1 && node.TimeslotAllocation[1]
	}
}

func participantModelAt(participant chainActiveParticipant, index int) string {
	if index < 0 || index >= len(participant.Models) {
		return ""
	}
	return strings.TrimSpace(participant.Models[index])
}

func modelNodePoCWeight(participantID, model string, modelNodes chainModelMLNodes, preservation preservationMode, preservedNodes preservedSnapshotState) uint64 {
	var weight uint64
	for _, node := range modelNodes.MLNodes {
		if nodePreserved(participantID, model, node, preservation, preservedNodes) {
			weight += uint64(node.PoCWeight)
		}
	}
	return weight
}

func participantWeight(participant chainActiveParticipant, preservation preservationMode, preservedNodes preservedSnapshotState) float64 {
	var weight uint64
	for i, modelNodes := range participant.MLNodes {
		weight += modelNodePoCWeight(participant.Index, participantModelAt(participant, i), modelNodes, preservation, preservedNodes)
	}
	return float64(weight)
}

func participantWeightsByModel(participant chainActiveParticipant, preservation preservationMode, preservedNodes preservedSnapshotState) map[string]float64 {
	weights := make(map[string]float64, len(participant.Models))
	for i, rawModel := range participant.Models {
		model := strings.TrimSpace(rawModel)
		if model == "" {
			continue
		}
		if i >= len(participant.MLNodes) {
			weights[model] = 0
			continue
		}
		weights[model] = float64(modelNodePoCWeight(participant.Index, model, participant.MLNodes[i], preservation, preservedNodes))
	}
	return weights
}

func participantHasPreservedNode(participant chainActiveParticipant, preservation preservationMode, preservedNodes preservedSnapshotState) bool {
	if preservation == preservationModeAll {
		return true
	}
	for i, modelNodes := range participant.MLNodes {
		model := participantModelAt(participant, i)
		for _, node := range modelNodes.MLNodes {
			if nodePreserved(participant.Index, model, node, preservation, preservedNodes) {
				return true
			}
		}
	}
	return false
}

// preservedModelsForParticipant lists, sorted, the models where a participant has a preserved node.
func preservedModelsForParticipant(participant chainActiveParticipant, preservation preservationMode, preservedNodes preservedSnapshotState) []string {
	seen := make(map[string]struct{}, len(participant.Models))
	var models []string
	for i, rawModel := range participant.Models {
		model := strings.TrimSpace(rawModel)
		if model == "" || i >= len(participant.MLNodes) {
			continue
		}
		for _, node := range participant.MLNodes[i].MLNodes {
			if !nodePreserved(participant.Index, model, node, preservation, preservedNodes) {
				continue
			}
			if _, ok := seen[model]; ok {
				continue
			}
			seen[model] = struct{}{}
			models = append(models, model)
			break
		}
	}
	sort.Strings(models)
	return models
}

// extractParticipantNodes flattens a participant's (model, node, weight) triples for the
// validation-capable merge.
func extractParticipantNodes(participant chainActiveParticipant) []participantNode {
	var nodes []participantNode
	for i, rawModel := range participant.Models {
		model := strings.TrimSpace(rawModel)
		if model == "" || i >= len(participant.MLNodes) {
			continue
		}
		for _, node := range participant.MLNodes[i].MLNodes {
			nodeID := strings.TrimSpace(node.NodeID)
			if nodeID == "" {
				continue
			}
			nodes = append(nodes, participantNode{Model: model, NodeID: nodeID, Weight: float64(node.PoCWeight)})
		}
	}
	return nodes
}

func cloneModelWeights(weights map[string]map[string]float64) map[string]map[string]float64 {
	cloned := make(map[string]map[string]float64, len(weights))
	for model, modelWeights := range weights {
		clonedWeights := make(map[string]float64, len(modelWeights))
		maps.Copy(clonedWeights, modelWeights)
		cloned[model] = clonedWeights
	}
	return cloned
}

func participantValidationInferenceWeights(nodes []participantNode, miner string, capable func(miner, nodeID string) bool) (map[string]float64, float64) {
	weights := map[string]float64{}
	if capable == nil {
		return weights, 0
	}
	var total float64
	for _, node := range nodes {
		if capable(miner, node.NodeID) {
			weights[node.Model] += node.Weight
			total += node.Weight
		}
	}
	return weights, total
}

// mergePreservedWithValidationCapable returns the PoC-validation availability views: preserved
// miners keep their current weight, and each excluded miner with validation-inference-capable
// nodes is added back with the summed weight of those nodes (per model). The input state is not
// mutated.
func mergePreservedWithValidationCapable(
	state participantsState,
	capable func(miner, nodeID string) bool,
) (preserved []string, preservedByModel map[string][]string, currentWeights map[string]float64, currentWeightsByModel map[string]map[string]float64) {
	preserved = append(preserved, state.Preserved...)

	preservedByModel = make(map[string][]string, len(state.PreservedByModel))
	for model, miners := range state.PreservedByModel {
		preservedByModel[model] = append([]string(nil), miners...)
	}

	currentWeights = make(map[string]float64, len(state.Weights))
	maps.Copy(currentWeights, state.Weights)
	currentWeightsByModel = cloneModelWeights(state.WeightsByModel)

	for _, miner := range state.Excluded {
		weightsByModel, total := participantValidationInferenceWeights(state.NodesByParticipant[miner], miner, capable)
		if total <= 0 {
			continue
		}
		preserved = append(preserved, miner)
		currentWeights[miner] = total
		for model, weight := range weightsByModel {
			if currentWeightsByModel[model] == nil {
				currentWeightsByModel[model] = map[string]float64{}
			}
			currentWeightsByModel[model][miner] = weight
			preservedByModel[model] = append(preservedByModel[model], miner)
		}
	}
	sort.Strings(preserved)
	for model := range preservedByModel {
		sort.Strings(preservedByModel[model])
	}
	return preserved, preservedByModel, currentWeights, currentWeightsByModel
}

type preservedSnapshotStatus int

const (
	preservedSnapshotUnavailable preservedSnapshotStatus = iota
	preservedSnapshotCurrent
	preservedSnapshotMissingCurrent
)

type preservedSnapshotState struct {
	byModel map[string]map[string]map[string]struct{}
}

// Has reports whether nodeID is preserved for participantID under model; nil-safe on a zero value.
func (s preservedSnapshotState) Has(model, participantID, nodeID string) bool {
	model = strings.TrimSpace(model)
	participantID = strings.TrimSpace(participantID)
	nodeID = strings.TrimSpace(nodeID)
	if model == "" || participantID == "" || nodeID == "" {
		return false
	}
	byParticipant := s.byModel[model]
	if byParticipant == nil {
		return false
	}
	nodes := byParticipant[participantID]
	if nodes == nil {
		return false
	}
	_, ok := nodes[nodeID]
	return ok
}

type chainPreservedNodesSnapshotResponse struct {
	Snapshot *chainPreservedNodesSnapshot `json:"snapshot,omitempty"`
	Found    bool                         `json:"found"`
}

type chainPreservedNodesSnapshot struct {
	EpisodeAnchorHeight jsonInt64                  `json:"episode_anchor_height"`
	ModelPreservedNodes []chainModelPreservedNodes `json:"model_preserved_nodes"`
}

type chainModelPreservedNodes struct {
	ModelID      string                           `json:"model_id"`
	Participants []chainParticipantPreservedNodes `json:"participants"`
}

type chainParticipantPreservedNodes struct {
	ParticipantID string   `json:"participant_id"`
	NodeIDs       []string `json:"node_ids"`
}

// parsePreservedSnapshot parses a preserved-nodes-snapshot body: current only when found and the
// anchor matches (or isn't checked); otherwise missingCurrent, or unavailable if decode fails.
func parsePreservedSnapshot(body []byte, expectedAnchor int64) (preservedSnapshotState, preservedSnapshotStatus, error) {
	var payload chainPreservedNodesSnapshotResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return preservedSnapshotState{}, preservedSnapshotUnavailable, fmt.Errorf("parse preserved snapshot: %w", err)
	}
	if !payload.Found || payload.Snapshot == nil {
		return preservedSnapshotState{}, preservedSnapshotMissingCurrent, nil
	}
	if expectedAnchor > 0 && int64(payload.Snapshot.EpisodeAnchorHeight) != expectedAnchor {
		return preservedSnapshotState{}, preservedSnapshotMissingCurrent, nil
	}
	return newPreservedSnapshotState(payload.Snapshot), preservedSnapshotCurrent, nil
}

func newPreservedSnapshotState(snapshot *chainPreservedNodesSnapshot) preservedSnapshotState {
	state := preservedSnapshotState{byModel: map[string]map[string]map[string]struct{}{}}
	for _, modelNodes := range snapshot.ModelPreservedNodes {
		model := strings.TrimSpace(modelNodes.ModelID)
		if model == "" {
			continue
		}
		byParticipant := state.byModel[model]
		if byParticipant == nil {
			byParticipant = map[string]map[string]struct{}{}
			state.byModel[model] = byParticipant
		}
		for _, participant := range modelNodes.Participants {
			participantID := strings.TrimSpace(participant.ParticipantID)
			if participantID == "" {
				continue
			}
			nodeSet := byParticipant[participantID]
			if nodeSet == nil {
				nodeSet = map[string]struct{}{}
				byParticipant[participantID] = nodeSet
			}
			for _, nodeID := range participant.NodeIDs {
				nodeID = strings.TrimSpace(nodeID)
				if nodeID != "" {
					nodeSet[nodeID] = struct{}{}
				}
			}
		}
	}
	return state
}

// chainParamsResponse is the raw /productscience/inference/inference/params JSON envelope,
// decoding only the devshard_escrow_params.max_nonce field.
type chainParamsResponse struct {
	Params *struct {
		DevshardEscrowParams *struct {
			MaxNonce jsonUint64 `json:"max_nonce,omitempty"`
		} `json:"devshard_escrow_params"`
	} `json:"params"`
}

func parseMaxNonce(body []byte) (uint64, error) {
	var payload chainParamsResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, fmt.Errorf("parse devshard escrow params: %w", err)
	}
	if payload.Params == nil || payload.Params.DevshardEscrowParams == nil {
		return 0, fmt.Errorf("devshard escrow params missing from chain params response")
	}
	return uint64(payload.Params.DevshardEscrowParams.MaxNonce), nil
}

// jsonInt64 decodes an int64 field that may arrive as a JSON number or a numeric string.
type jsonInt64 int64

func (n *jsonInt64) UnmarshalJSON(data []byte) error {
	parsed, err := parseFlexibleInt64(data)
	if err != nil {
		return err
	}
	*n = jsonInt64(parsed)
	return nil
}

// jsonUint64 decodes a uint64 field that may arrive as a JSON number or a numeric string.
type jsonUint64 uint64

func (n *jsonUint64) UnmarshalJSON(data []byte) error {
	parsed, err := parseFlexibleUint64(data)
	if err != nil {
		return err
	}
	*n = jsonUint64(parsed)
	return nil
}

// confirmationPoCPhaseValue decodes a confirmation-PoC phase as either its string enum name or the chain's numeric (0-4) protobuf enum value.
type confirmationPoCPhaseValue string

func (p *confirmationPoCPhaseValue) UnmarshalJSON(data []byte) error {
	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		*p = confirmationPoCPhaseValue(asString)
		return nil
	}

	var asInt int
	if err := json.Unmarshal(data, &asInt); err == nil {
		switch asInt {
		case 0:
			*p = confirmationPoCPhaseValue(ConfirmationPoCInactive)
		case 1:
			*p = confirmationPoCPhaseValue(ConfirmationPoCGracePeriod)
		case 2:
			*p = confirmationPoCPhaseValue(ConfirmationPoCGeneration)
		case 3:
			*p = confirmationPoCPhaseValue(ConfirmationPoCValidation)
		case 4:
			*p = confirmationPoCPhaseValue(ConfirmationPoCCompleted)
		default:
			*p = confirmationPoCPhaseValue(strconv.Itoa(asInt))
		}
		return nil
	}

	return fmt.Errorf("unsupported confirmation PoC phase %s", string(data))
}

func parseFlexibleInt64(data []byte) (int64, error) {
	var asInt int64
	if err := json.Unmarshal(data, &asInt); err == nil {
		return asInt, nil
	}

	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		return strconv.ParseInt(strings.TrimSpace(asString), 10, 64)
	}

	return 0, fmt.Errorf("unsupported int64 value %s", string(data))
}

func parseFlexibleUint64(data []byte) (uint64, error) {
	var asUint uint64
	if err := json.Unmarshal(data, &asUint); err == nil {
		return asUint, nil
	}

	var asString string
	if err := json.Unmarshal(data, &asString); err == nil {
		return strconv.ParseUint(strings.TrimSpace(asString), 10, 64)
	}

	return 0, fmt.Errorf("unsupported uint64 value %s", string(data))
}
