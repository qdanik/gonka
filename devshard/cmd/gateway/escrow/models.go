// Package escrow manages devshard escrow rotation, settlement, and crash-recovery bookkeeping.
package escrow

import (
	"fmt"
	"strings"

	json "github.com/goccy/go-json"

	"devshard/cmd/gateway/chain"
)

// json tags are the DEVSHARD_ESCROW_ROTATION_MODELS_JSON wire contract, not renameable.
type ModelConfig struct {
	ModelID       string `json:"model_id"`
	TempCount     int    `json:"temp_count"`
	TargetCount   int    `json:"target_count"`
	Amount        uint64 `json:"amount"`
	PrivateKeyEnv string `json:"private_key_env"`
}

// defaultTargetCount applies only when target_count is absent; an explicit 0 is still a mistake to reject.
const defaultTargetCount = 1

func parseModels(raw string) ([]ModelConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var entries []json.RawMessage
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("parse rotation models: %w", err)
	}
	models := make([]ModelConfig, 0, len(entries))
	for _, entry := range entries {
		model := ModelConfig{TargetCount: defaultTargetCount}
		if err := json.Unmarshal(entry, &model); err != nil {
			return nil, fmt.Errorf("parse rotation models: %w", err)
		}
		models = append(models, model)
	}
	for _, model := range models {
		if model.ModelID == "" {
			return nil, fmt.Errorf("rotation model %q: model_id must not be empty", model.ModelID)
		}
		if model.TargetCount < 1 {
			return nil, fmt.Errorf("rotation model %q: target_count must be >= 1", model.ModelID)
		}
		if model.Amount == 0 {
			return nil, fmt.Errorf("rotation model %q: amount must be > 0", model.ModelID)
		}
	}
	return models, nil
}

// known is false only when both weight-by-model maps are empty (cold start): callers must not skip an unknown model.
func servedByNetwork(snapshot chain.PhaseSnapshot, modelID string) (served, known bool) {
	for id := range snapshot.FullWeightsByModel {
		known = true
		if id == modelID {
			served = true
		}
	}
	for id := range snapshot.CurrentWeightsByModel {
		known = true
		if id == modelID {
			served = true
		}
	}
	return served, known
}
