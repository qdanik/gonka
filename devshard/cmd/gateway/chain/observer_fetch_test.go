package chain

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

const epochInfoNumericJSON = `{
	"block_height": 1000,
	"phase": "PoCGenerate",
	"latest_epoch": {"index": 7, "poc_start_block_height": 950},
	"is_confirmation_poc_active": false
}`

const epochInfoStringScalarsJSON = `{
	"block_height": "1000",
	"phase": "Inference",
	"latest_epoch": {"index": "7", "poc_start_block_height": "950"},
	"is_confirmation_poc_active": true,
	"active_confirmation_poc_event": {"phase": "CONFIRMATION_POC_VALIDATION", "trigger_height": "555"}
}`

func epochInfoWithConfirmationPhase(phaseJSON string) string {
	return fmt.Sprintf(`{
		"block_height": 500,
		"phase": "Inference",
		"latest_epoch": {"index": 3, "poc_start_block_height": 400},
		"is_confirmation_poc_active": true,
		"active_confirmation_poc_event": {"phase": %s, "trigger_height": 123}
	}`, phaseJSON)
}

func TestParseEpochInfoNumericFields(t *testing.T) {
	info, err := parseEpochInfo([]byte(epochInfoNumericJSON))
	if err != nil {
		t.Fatalf("parseEpochInfo(): %v", err)
	}
	want := epochInfo{
		BlockHeight:            1000,
		Phase:                  "PoCGenerate",
		EpochIndex:             7,
		PoCStartBlockHeight:    950,
		EpochSwitchBlockHeight: 950,
	}
	if info != want {
		t.Fatalf("parseEpochInfo() = %+v, want %+v", info, want)
	}
}

func TestParseEpochInfoStringTypedScalars(t *testing.T) {
	info, err := parseEpochInfo([]byte(epochInfoStringScalarsJSON))
	if err != nil {
		t.Fatalf("parseEpochInfo(): %v", err)
	}
	want := epochInfo{
		BlockHeight:                  1000,
		Phase:                        "Inference",
		EpochIndex:                   7,
		PoCStartBlockHeight:          950,
		EpochSwitchBlockHeight:       950,
		IsConfirmationPoCActive:      true,
		ConfirmationPoCPhase:         ConfirmationPoCValidation,
		ConfirmationPoCTriggerHeight: 555,
	}
	if info != want {
		t.Fatalf("parseEpochInfo() = %+v, want %+v", info, want)
	}
}

func TestParseEpochInfoConfirmationPhaseFlexibleDecode(t *testing.T) {
	cases := []struct {
		name      string
		phaseJSON string
		wantPhase ConfirmationPoCPhase
		wantErr   bool
	}{
		{"int 0 inactive", "0", ConfirmationPoCInactive, false},
		{"int 1 grace period", "1", ConfirmationPoCGracePeriod, false},
		{"int 2 generation", "2", ConfirmationPoCGeneration, false},
		{"int 3 validation", "3", ConfirmationPoCValidation, false},
		{"int 4 completed", "4", ConfirmationPoCCompleted, false},
		{"unknown int falls back to stringified value", "99", "99", false},
		{"string form passes through verbatim", `"CONFIRMATION_POC_VALIDATION"`, ConfirmationPoCValidation, false},
		{"unsupported json type errors", "true", "", true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			body := []byte(epochInfoWithConfirmationPhase(testCase.phaseJSON))
			info, err := parseEpochInfo(body)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("parseEpochInfo() error = nil, want error for phase JSON %s", testCase.phaseJSON)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseEpochInfo(): %v", err)
			}
			if info.ConfirmationPoCPhase != testCase.wantPhase {
				t.Fatalf("ConfirmationPoCPhase = %q, want %q", info.ConfirmationPoCPhase, testCase.wantPhase)
			}
		})
	}
}

func TestParseEpochInfoMalformedJSONErrors(t *testing.T) {
	_, err := parseEpochInfo([]byte(`{not-json`))
	if err == nil {
		t.Fatal("parseEpochInfo() error = nil, want error for malformed JSON")
	}
}

// epochSwitchJSON builds an epoch info body with a configurable current block height and the
// three fallback-ladder inputs; a zero argument omits that field's contribution to the ladder.
func epochSwitchJSON(blockHeight, setNewValidators, nextSetNewValidators, nextPoCStart int64) string {
	return fmt.Sprintf(`{
		"block_height": %d,
		"phase": "Inference",
		"latest_epoch": {"index": 1, "poc_start_block_height": 900},
		"epoch_stages": {"set_new_validators": %d, "next_poc_start": %d},
		"next_epoch_stages": {"set_new_validators": %d},
		"is_confirmation_poc_active": false
	}`, blockHeight, setNewValidators, nextPoCStart, nextSetNewValidators)
}

// TestParseEpochInfoEpochSwitchBlockHeightFallbackLadder covers all four ladder rungs, including
// the rung-1 guard that ignores a SetNewValidators height already behind the current block.
func TestParseEpochInfoEpochSwitchBlockHeightFallbackLadder(t *testing.T) {
	cases := []struct {
		name                 string
		blockHeight          int64
		setNewValidators     int64
		nextSetNewValidators int64
		nextPoCStart         int64
		want                 int64
	}{
		{"rung1: current epoch set_new_validators ahead of block height wins", 1000, 1200, 1300, 1100, 1200},
		{"rung1 boundary: set_new_validators exactly at block height still wins", 1000, 1000, 1300, 1100, 1000},
		{"rung1 stale falls to rung2: set_new_validators behind block height is ignored", 1000, 500, 1300, 1100, 1300},
		{"rung2: next epoch set_new_validators wins when rung1 is zero", 1000, 0, 1300, 1100, 1300},
		{"rung3: next_poc_start wins when rungs 1-2 are zero", 1000, 0, 0, 1100, 1100},
		{"rung4: latest_epoch poc_start_block_height wins when rungs 1-3 are zero", 1000, 0, 0, 0, 900},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			body := epochSwitchJSON(testCase.blockHeight, testCase.setNewValidators, testCase.nextSetNewValidators, testCase.nextPoCStart)
			info, err := parseEpochInfo([]byte(body))
			if err != nil {
				t.Fatalf("parseEpochInfo(): %v", err)
			}
			if info.EpochSwitchBlockHeight != testCase.want {
				t.Fatalf("EpochSwitchBlockHeight = %d, want %d", info.EpochSwitchBlockHeight, testCase.want)
			}
		})
	}
}

// participantsMultiModelJSON has one participant with two models split across preserved and
// non-preserved nodes, one participant with only a non-preserved node, and one blank-index
// participant that must be dropped entirely.
const participantsMultiModelJSON = `{
	"active_participants": {
		"participants": [
			{
				"index": "gonka1abc",
				"inference_url": "http://host-a:8080",
				"models": ["model-x", "model-y"],
				"ml_nodes": [
					{"ml_nodes": [
						{"node_id": "node1", "timeslot_allocation": [true, true], "poc_weight": 100},
						{"node_id": "node2", "timeslot_allocation": [true, false], "poc_weight": 50}
					]},
					{"ml_nodes": [
						{"node_id": "node3", "timeslot_allocation": [true, true], "poc_weight": 30}
					]}
				]
			},
			{
				"index": "gonka1def",
				"inference_url": "http://host-b:8080",
				"models": ["model-x"],
				"ml_nodes": [
					{"ml_nodes": [
						{"node_id": "node4", "timeslot_allocation": [true, false], "poc_weight": 20}
					]}
				]
			},
			{
				"index": "   ",
				"inference_url": "http://ignored:8080",
				"models": [],
				"ml_nodes": []
			}
		]
	}
}`

func TestParseParticipantsPoCActiveWeightsAndPreservation(t *testing.T) {
	state, err := parseParticipants([]byte(participantsMultiModelJSON), preservationModeLegacy, preservedSnapshotState{})
	if err != nil {
		t.Fatalf("parseParticipants(): %v", err)
	}

	wantWeights := map[string]float64{"gonka1abc": 130, "gonka1def": 0}
	if !reflect.DeepEqual(state.Weights, wantWeights) {
		t.Fatalf("Weights = %v, want %v", state.Weights, wantWeights)
	}

	wantFullWeights := map[string]float64{"gonka1abc": 180, "gonka1def": 20}
	if !reflect.DeepEqual(state.FullWeights, wantFullWeights) {
		t.Fatalf("FullWeights = %v, want %v", state.FullWeights, wantFullWeights)
	}

	wantWeightsByModel := map[string]map[string]float64{
		"model-x": {"gonka1abc": 100, "gonka1def": 0},
		"model-y": {"gonka1abc": 30},
	}
	if !reflect.DeepEqual(state.WeightsByModel, wantWeightsByModel) {
		t.Fatalf("WeightsByModel = %v, want %v", state.WeightsByModel, wantWeightsByModel)
	}

	wantFullWeightsByModel := map[string]map[string]float64{
		"model-x": {"gonka1abc": 150, "gonka1def": 20},
		"model-y": {"gonka1abc": 30},
	}
	if !reflect.DeepEqual(state.FullWeightsByModel, wantFullWeightsByModel) {
		t.Fatalf("FullWeightsByModel = %v, want %v", state.FullWeightsByModel, wantFullWeightsByModel)
	}

	wantInferenceURLs := map[string]string{"gonka1abc": "http://host-a:8080", "gonka1def": "http://host-b:8080"}
	if !reflect.DeepEqual(state.InferenceURLs, wantInferenceURLs) {
		t.Fatalf("InferenceURLs = %v, want %v", state.InferenceURLs, wantInferenceURLs)
	}

	wantPreserved := []string{"gonka1abc"}
	if !reflect.DeepEqual(state.Preserved, wantPreserved) {
		t.Fatalf("Preserved = %v, want %v", state.Preserved, wantPreserved)
	}
	wantExcluded := []string{"gonka1def"}
	if !reflect.DeepEqual(state.Excluded, wantExcluded) {
		t.Fatalf("Excluded = %v, want %v", state.Excluded, wantExcluded)
	}

	wantPreservedByModel := map[string][]string{"model-x": {"gonka1abc"}, "model-y": {"gonka1abc"}}
	if !reflect.DeepEqual(state.PreservedByModel, wantPreservedByModel) {
		t.Fatalf("PreservedByModel = %v, want %v", state.PreservedByModel, wantPreservedByModel)
	}

	wantNodes := []participantNode{
		{Model: "model-x", NodeID: "node1", Weight: 100},
		{Model: "model-x", NodeID: "node2", Weight: 50},
		{Model: "model-y", NodeID: "node3", Weight: 30},
	}
	if !reflect.DeepEqual(state.NodesByParticipant["gonka1abc"], wantNodes) {
		t.Fatalf("NodesByParticipant[gonka1abc] = %v, want %v", state.NodesByParticipant["gonka1abc"], wantNodes)
	}

	if len(state.Weights) != 2 {
		t.Fatalf("len(Weights) = %d, want 2 (blank-index participant must be dropped)", len(state.Weights))
	}
}

func TestParseParticipantsPoCInactiveTreatsAllNodesAsPreserved(t *testing.T) {
	state, err := parseParticipants([]byte(participantsMultiModelJSON), preservationModeAll, preservedSnapshotState{})
	if err != nil {
		t.Fatalf("parseParticipants(): %v", err)
	}
	wantWeights := map[string]float64{"gonka1abc": 180, "gonka1def": 20}
	if !reflect.DeepEqual(state.Weights, wantWeights) {
		t.Fatalf("Weights = %v, want %v", state.Weights, wantWeights)
	}
	if !reflect.DeepEqual(state.Weights, state.FullWeights) {
		t.Fatalf("Weights = %v, FullWeights = %v; want equal when PoC inactive", state.Weights, state.FullWeights)
	}
	wantPreserved := []string{"gonka1abc", "gonka1def"}
	if !reflect.DeepEqual(state.Preserved, wantPreserved) {
		t.Fatalf("Preserved = %v, want %v", state.Preserved, wantPreserved)
	}
	if len(state.Excluded) != 0 {
		t.Fatalf("Excluded = %v, want empty when PoC inactive", state.Excluded)
	}
}

// participantsSnapshotOverrideJSON, parsed with preservationModeSnapshot, exercises membership
// that contradicts the timeslot flags: node2/node4 are timeslot-non-preserved but listed in the
// snapshot, node1/node3 are timeslot-preserved but absent from it.
const participantsSnapshotOverrideJSON = `{
	"found": true,
	"snapshot": {
		"episode_anchor_height": 950,
		"model_preserved_nodes": [
			{
				"model_id": "model-x",
				"participants": [
					{"participant_id": "gonka1abc", "node_ids": ["node2"]},
					{"participant_id": "gonka1def", "node_ids": ["node4"]}
				]
			}
		]
	}
}`

// TestParseParticipantsSnapshotModeUsesSnapshotMembership covers preservationModeSnapshot: the
// snapshot's (model, participant, node) membership replaces the timeslot-allocation rule for
// current weights and preserved sets, while full weights stay all-node.
func TestParseParticipantsSnapshotModeUsesSnapshotMembership(t *testing.T) {
	preservedNodes, status, err := parsePreservedSnapshot([]byte(participantsSnapshotOverrideJSON), 950)
	if err != nil || status != preservedSnapshotCurrent {
		t.Fatalf("parsePreservedSnapshot() = (status %v, err %v), want current/nil", status, err)
	}

	state, err := parseParticipants([]byte(participantsMultiModelJSON), preservationModeSnapshot, preservedNodes)
	if err != nil {
		t.Fatalf("parseParticipants(): %v", err)
	}

	wantWeights := map[string]float64{"gonka1abc": 50, "gonka1def": 20}
	if !reflect.DeepEqual(state.Weights, wantWeights) {
		t.Fatalf("Weights = %v, want %v (snapshot membership, not timeslots)", state.Weights, wantWeights)
	}
	wantWeightsByModel := map[string]map[string]float64{
		"model-x": {"gonka1abc": 50, "gonka1def": 20},
		"model-y": {"gonka1abc": 0},
	}
	if !reflect.DeepEqual(state.WeightsByModel, wantWeightsByModel) {
		t.Fatalf("WeightsByModel = %v, want %v", state.WeightsByModel, wantWeightsByModel)
	}
	wantFullWeights := map[string]float64{"gonka1abc": 180, "gonka1def": 20}
	if !reflect.DeepEqual(state.FullWeights, wantFullWeights) {
		t.Fatalf("FullWeights = %v, want %v (all-node, mode-independent)", state.FullWeights, wantFullWeights)
	}
	wantPreserved := []string{"gonka1abc", "gonka1def"}
	if !reflect.DeepEqual(state.Preserved, wantPreserved) {
		t.Fatalf("Preserved = %v, want %v", state.Preserved, wantPreserved)
	}
	if len(state.Excluded) != 0 {
		t.Fatalf("Excluded = %v, want empty", state.Excluded)
	}
	wantPreservedByModel := map[string][]string{"model-x": {"gonka1abc", "gonka1def"}}
	if !reflect.DeepEqual(state.PreservedByModel, wantPreservedByModel) {
		t.Fatalf("PreservedByModel = %v, want %v", state.PreservedByModel, wantPreservedByModel)
	}
}

// TestParseParticipantsPreservedIsSorted covers the Preserved list coming back sorted regardless
// of wire order.
func TestParseParticipantsPreservedIsSorted(t *testing.T) {
	reverseOrderJSON := `{
		"active_participants": {
			"participants": [
				{
					"index": "gonka1zzz",
					"inference_url": "http://host-z:8080",
					"models": ["model-a"],
					"ml_nodes": [{"ml_nodes": [{"node_id": "nz", "timeslot_allocation": [true, true], "poc_weight": 1}]}]
				},
				{
					"index": "gonka1aaa",
					"inference_url": "http://host-a:8080",
					"models": ["model-a"],
					"ml_nodes": [{"ml_nodes": [{"node_id": "na", "timeslot_allocation": [true, true], "poc_weight": 2}]}]
				}
			]
		}
	}`
	state, err := parseParticipants([]byte(reverseOrderJSON), preservationModeLegacy, preservedSnapshotState{})
	if err != nil {
		t.Fatalf("parseParticipants(): %v", err)
	}
	wantPreserved := []string{"gonka1aaa", "gonka1zzz"}
	if !reflect.DeepEqual(state.Preserved, wantPreserved) {
		t.Fatalf("Preserved = %v, want sorted %v", state.Preserved, wantPreserved)
	}
}

// TestMergePreservedWithValidationCapable covers the validation-phase merge: an excluded miner
// rejoins with the summed weight of its capable nodes, a non-capable or nil capability check
// leaves the views unchanged, and the input state is never mutated.
func TestMergePreservedWithValidationCapable(t *testing.T) {
	buildState := func(t *testing.T) participantsState {
		t.Helper()
		state, err := parseParticipants([]byte(participantsMultiModelJSON), preservationModeLegacy, preservedSnapshotState{})
		if err != nil {
			t.Fatalf("parseParticipants(): %v", err)
		}
		return state
	}
	defCapable := func(miner, nodeID string) bool { return miner == "gonka1def" && nodeID == "node4" }
	noneCapable := func(miner, nodeID string) bool { return false }

	t.Run("capable excluded miner rejoins with capable-node weight", func(t *testing.T) {
		state := buildState(t)

		preserved, preservedByModel, currentWeights, currentWeightsByModel :=
			mergePreservedWithValidationCapable(state, defCapable)

		if want := []string{"gonka1abc", "gonka1def"}; !reflect.DeepEqual(preserved, want) {
			t.Fatalf("preserved = %v, want %v", preserved, want)
		}
		if want := map[string]float64{"gonka1abc": 130, "gonka1def": 20}; !reflect.DeepEqual(currentWeights, want) {
			t.Fatalf("currentWeights = %v, want %v", currentWeights, want)
		}
		wantByModel := map[string]map[string]float64{
			"model-x": {"gonka1abc": 100, "gonka1def": 20},
			"model-y": {"gonka1abc": 30},
		}
		if !reflect.DeepEqual(currentWeightsByModel, wantByModel) {
			t.Fatalf("currentWeightsByModel = %v, want %v", currentWeightsByModel, wantByModel)
		}
		wantPreservedByModel := map[string][]string{"model-x": {"gonka1abc", "gonka1def"}, "model-y": {"gonka1abc"}}
		if !reflect.DeepEqual(preservedByModel, wantPreservedByModel) {
			t.Fatalf("preservedByModel = %v, want %v", preservedByModel, wantPreservedByModel)
		}

		if want := []string{"gonka1abc"}; !reflect.DeepEqual(state.Preserved, want) {
			t.Fatalf("input state.Preserved mutated to %v, want %v", state.Preserved, want)
		}
		if state.Weights["gonka1def"] != 0 || state.WeightsByModel["model-x"]["gonka1def"] != 0 {
			t.Fatalf("input state weights mutated: %v / %v", state.Weights, state.WeightsByModel)
		}
		if want := map[string][]string{"model-x": {"gonka1abc"}, "model-y": {"gonka1abc"}}; !reflect.DeepEqual(state.PreservedByModel, want) {
			t.Fatalf("input state.PreservedByModel mutated to %v, want %v", state.PreservedByModel, want)
		}
	})

	t.Run("non-capable excluded miner stays out", func(t *testing.T) {
		state := buildState(t)

		preserved, preservedByModel, currentWeights, _ :=
			mergePreservedWithValidationCapable(state, noneCapable)

		if want := []string{"gonka1abc"}; !reflect.DeepEqual(preserved, want) {
			t.Fatalf("preserved = %v, want %v", preserved, want)
		}
		if currentWeights["gonka1def"] != 0 {
			t.Fatalf("currentWeights[gonka1def] = %v, want 0", currentWeights["gonka1def"])
		}
		if want := map[string][]string{"model-x": {"gonka1abc"}, "model-y": {"gonka1abc"}}; !reflect.DeepEqual(preservedByModel, want) {
			t.Fatalf("preservedByModel = %v, want %v", preservedByModel, want)
		}
	})

	t.Run("nil capability check keeps views unchanged", func(t *testing.T) {
		state := buildState(t)

		preserved, _, currentWeights, _ := mergePreservedWithValidationCapable(state, nil)

		if want := []string{"gonka1abc"}; !reflect.DeepEqual(preserved, want) {
			t.Fatalf("preserved = %v, want %v", preserved, want)
		}
		if !reflect.DeepEqual(currentWeights, state.Weights) {
			t.Fatalf("currentWeights = %v, want copy of %v", currentWeights, state.Weights)
		}
	})
}

// TestParseFlexibleIntegersUseStrictDecimalParsing covers the string branch of the flexible
// scalar decoders: surrounding whitespace is tolerated, embedded whitespace is an error.
func TestParseFlexibleIntegersUseStrictDecimalParsing(t *testing.T) {
	if value, err := parseFlexibleInt64([]byte(`" 950 "`)); err != nil || value != 950 {
		t.Fatalf(`parseFlexibleInt64(" 950 ") = (%d, %v), want (950, nil)`, value, err)
	}
	if value, err := parseFlexibleUint64([]byte(`" 950 "`)); err != nil || value != 950 {
		t.Fatalf(`parseFlexibleUint64(" 950 ") = (%d, %v), want (950, nil)`, value, err)
	}
	if _, err := parseFlexibleInt64([]byte(`"12 34"`)); err == nil {
		t.Fatal(`parseFlexibleInt64("12 34") error = nil, want error`)
	}
	if _, err := parseFlexibleUint64([]byte(`"12 34"`)); err == nil {
		t.Fatal(`parseFlexibleUint64("12 34") error = nil, want error`)
	}
}

// TestParseParticipantsEmptyArrayYieldsEmptyMapsNotNil covers an empty participants array: every
// map must be non-nil and empty so callers never hit a nil-map panic.
func TestParseParticipantsEmptyArrayYieldsEmptyMapsNotNil(t *testing.T) {
	state, err := parseParticipants([]byte(`{"active_participants":{"participants":[]}}`), preservationModeLegacy, preservedSnapshotState{})
	if err != nil {
		t.Fatalf("parseParticipants(): %v", err)
	}

	mapLengths := map[string]int{
		"Weights":            len(state.Weights),
		"FullWeights":        len(state.FullWeights),
		"WeightsByModel":     len(state.WeightsByModel),
		"FullWeightsByModel": len(state.FullWeightsByModel),
		"InferenceURLs":      len(state.InferenceURLs),
		"PreservedByModel":   len(state.PreservedByModel),
		"NodesByParticipant": len(state.NodesByParticipant),
	}
	for name, length := range mapLengths {
		if length != 0 {
			t.Fatalf("%s len = %d, want 0", name, length)
		}
	}
	if state.Weights == nil || state.FullWeights == nil || state.WeightsByModel == nil ||
		state.FullWeightsByModel == nil || state.InferenceURLs == nil || state.PreservedByModel == nil ||
		state.NodesByParticipant == nil {
		t.Fatal("maps must be non-nil even for an empty participants array")
	}
	if len(state.Preserved) != 0 || len(state.Excluded) != 0 {
		t.Fatalf("Preserved/Excluded = %v/%v, want both empty", state.Preserved, state.Excluded)
	}
}

func TestParseParticipantsMalformedJSONErrors(t *testing.T) {
	_, err := parseParticipants([]byte(`{not-json`), preservationModeLegacy, preservedSnapshotState{})
	if err == nil {
		t.Fatal("parseParticipants() error = nil, want error for malformed JSON")
	}
}

const preservedSnapshotFoundJSON = `{
	"found": true,
	"snapshot": {
		"episode_anchor_height": 950,
		"model_preserved_nodes": [
			{
				"model_id": "model-x",
				"participants": [
					{"participant_id": "gonka1abc", "node_ids": ["node1", "node3"]}
				]
			}
		]
	}
}`

const preservedSnapshotNotFoundJSON = `{"found": false}`

const preservedSnapshotEmptyModelsJSON = `{
	"found": true,
	"snapshot": {"episode_anchor_height": 950, "model_preserved_nodes": []}
}`

// TestParsePreservedSnapshotStatusMatrix covers the found/anchor-match/anchor-mismatch/malformed
// status matrix: current only when found and the anchor matches (or isn't checked).
func TestParsePreservedSnapshotStatusMatrix(t *testing.T) {
	cases := []struct {
		name           string
		body           string
		expectedAnchor int64
		wantStatus     preservedSnapshotStatus
		wantErr        bool
	}{
		{"anchor matches -> current", preservedSnapshotFoundJSON, 950, preservedSnapshotCurrent, false},
		{"zero expected anchor skips check -> current", preservedSnapshotFoundJSON, 0, preservedSnapshotCurrent, false},
		{"anchor mismatch -> missing", preservedSnapshotFoundJSON, 999, preservedSnapshotMissingCurrent, false},
		{"not found -> missing", preservedSnapshotNotFoundJSON, 950, preservedSnapshotMissingCurrent, false},
		{"found but snapshot null -> missing", `{"found": true, "snapshot": null}`, 950, preservedSnapshotMissingCurrent, false},
		{"empty model list -> current with empty map", preservedSnapshotEmptyModelsJSON, 950, preservedSnapshotCurrent, false},
		{"malformed json -> unavailable", `{not-json`, 950, preservedSnapshotUnavailable, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, status, err := parsePreservedSnapshot([]byte(testCase.body), testCase.expectedAnchor)
			if testCase.wantErr && err == nil {
				t.Fatal("parsePreservedSnapshot() error = nil, want error")
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("parsePreservedSnapshot(): %v", err)
			}
			if status != testCase.wantStatus {
				t.Fatalf("status = %v, want %v", status, testCase.wantStatus)
			}
		})
	}
}

func TestParsePreservedSnapshotHasLooksUpByModelParticipantNode(t *testing.T) {
	state, status, err := parsePreservedSnapshot([]byte(preservedSnapshotFoundJSON), 950)
	if err != nil {
		t.Fatalf("parsePreservedSnapshot(): %v", err)
	}
	if status != preservedSnapshotCurrent {
		t.Fatalf("status = %v, want preservedSnapshotCurrent", status)
	}
	cases := []struct {
		name          string
		model         string
		participantID string
		nodeID        string
		want          bool
	}{
		{"preserved node", "model-x", "gonka1abc", "node1", true},
		{"preserved node, second id", "model-x", "gonka1abc", "node3", true},
		{"node absent from snapshot", "model-x", "gonka1abc", "node2", false},
		{"wrong model", "model-y", "gonka1abc", "node1", false},
		{"wrong participant", "model-x", "gonka1xyz", "node1", false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := state.Has(testCase.model, testCase.participantID, testCase.nodeID); got != testCase.want {
				t.Fatalf("Has(%q, %q, %q) = %v, want %v", testCase.model, testCase.participantID, testCase.nodeID, got, testCase.want)
			}
		})
	}
}

// TestParsePreservedSnapshotEmptyModelsHasIsSafe covers Has() against an empty-but-current snapshot: no panic, always false.
func TestParsePreservedSnapshotEmptyModelsHasIsSafe(t *testing.T) {
	state, status, err := parsePreservedSnapshot([]byte(preservedSnapshotEmptyModelsJSON), 950)
	if err != nil {
		t.Fatalf("parsePreservedSnapshot(): %v", err)
	}
	if status != preservedSnapshotCurrent {
		t.Fatalf("status = %v, want preservedSnapshotCurrent", status)
	}
	if state.Has("model-x", "gonka1abc", "node1") {
		t.Fatal("Has() = true against an empty snapshot, want false")
	}
}

func TestParseMaxNonceDecodesField(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		want    uint64
		wantErr bool
	}{
		{"decodes numeric max_nonce", `{"params": {"devshard_escrow_params": {"max_nonce": 19800}}}`, 19800, false},
		{"decodes string-encoded max_nonce", `{"params": {"devshard_escrow_params": {"max_nonce": "19800"}}}`, 19800, false},
		{"zero max_nonce decodes to zero", `{"params": {"devshard_escrow_params": {"max_nonce": 0}}}`, 0, false},
		{"missing devshard_escrow_params errors", `{"params": {}}`, 0, true},
		{"missing params errors", `{}`, 0, true},
		{"malformed json errors", `{not-json`, 0, true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := parseMaxNonce([]byte(testCase.body))
			if testCase.wantErr {
				if err == nil {
					t.Fatal("parseMaxNonce() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMaxNonce(): %v", err)
			}
			if got != testCase.want {
				t.Fatalf("parseMaxNonce() = %d, want %d", got, testCase.want)
			}
		})
	}
}

// The public API has served these numbers both ways: protojson stringifies int64 fields, encoding/json
// leaves them numeric, and gonka-ai/gonka#1526 switches the deployed shape back from the first to the
// second. Parsing only one shape means a weight map that silently reads as zero, which is not "unknown"
// downstream -- it is "everything weighs nothing", and routing stops with no error anywhere.
func TestParseParticipantsAcceptsStringAndNumericIntegers(t *testing.T) {
	stringified := strings.NewReplacer(
		`"poc_weight": 100`, `"poc_weight": "100"`,
		`"poc_weight": 50`, `"poc_weight": "50"`,
		`"poc_weight": 30`, `"poc_weight": "30"`,
		`"poc_weight": 20`, `"poc_weight": "20"`,
	).Replace(participantsMultiModelJSON)
	if stringified == participantsMultiModelJSON {
		t.Fatal("the fixture no longer carries numeric poc_weight values to stringify")
	}

	numericState, err := parseParticipants([]byte(participantsMultiModelJSON), preservationModeLegacy, preservedSnapshotState{})
	if err != nil {
		t.Fatalf("parseParticipants(numeric): %v", err)
	}
	stringState, err := parseParticipants([]byte(stringified), preservationModeLegacy, preservedSnapshotState{})
	if err != nil {
		t.Fatalf("parseParticipants(stringified): %v", err)
	}

	if !reflect.DeepEqual(numericState, stringState) {
		t.Fatalf("the two wire shapes parsed differently:\n numeric = %+v\n string  = %+v", numericState, stringState)
	}
}

// The same flip reaches the epoch payload: block_height, poc_start_block_height and the stage heights
// are all int64 on the wire.
func TestParseEpochAcceptsStringAndNumericIntegers(t *testing.T) {
	numeric := `{"block_height": 1000, "phase": "Inference",
		"latest_epoch": {"index": 7, "poc_start_block_height": 900},
		"epoch_stages": {"set_new_validators": 950, "next_poc_start": 1900},
		"is_confirmation_poc_active": false}`
	stringified := `{"block_height": "1000", "phase": "Inference",
		"latest_epoch": {"index": "7", "poc_start_block_height": "900"},
		"epoch_stages": {"set_new_validators": "950", "next_poc_start": "1900"},
		"is_confirmation_poc_active": false}`

	fromNumeric, err := parseEpochInfo([]byte(numeric))
	if err != nil {
		t.Fatalf("parseEpochInfo(numeric): %v", err)
	}
	fromString, err := parseEpochInfo([]byte(stringified))
	if err != nil {
		t.Fatalf("parseEpochInfo(stringified): %v", err)
	}

	if !reflect.DeepEqual(fromNumeric, fromString) {
		t.Fatalf("the two wire shapes parsed differently:\n numeric = %+v\n string  = %+v", fromNumeric, fromString)
	}
	if fromNumeric.BlockHeight != 1000 {
		t.Fatalf("BlockHeight = %d, want 1000", fromNumeric.BlockHeight)
	}
}
