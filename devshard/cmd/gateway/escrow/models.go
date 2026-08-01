// Package escrow manages devshard escrow rotation, settlement, and crash-recovery bookkeeping.
package escrow

import (
	"fmt"
	"strings"

	"devshard/cmd/gateway/chain"

	json "github.com/goccy/go-json"
)

// json tags are the GATEWAY_ROTATION_MODELS_JSON wire contract, not renameable.
type ModelConfig struct {
	ModelID       string `json:"model_id"`
	TempCount     int    `json:"temp_count"`
	TargetCount   int    `json:"target_count"`
	Amount        uint64 `json:"amount"`
	PrivateKeyEnv string `json:"private_key_env"`
}

func parseModels(raw string) ([]ModelConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var models []ModelConfig
	if err := json.Unmarshal([]byte(raw), &models); err != nil {
		return nil, fmt.Errorf("parse rotation models: %w", err)
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
