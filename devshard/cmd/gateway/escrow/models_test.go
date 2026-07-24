package escrow

import (
	"strings"
	"testing"

	"devshard/cmd/gateway/chain"
)

func TestParseModels(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []ModelConfig
		wantErr bool
	}{
		{
			name: "valid multi-model JSON populates every field",
			raw: `[
				{"model_id":"model-a","temp_count":2,"target_count":5,"amount":1000000,"private_key_env":"MODEL_A_KEY"},
				{"model_id":"model-b","temp_count":1,"target_count":3,"amount":2000000,"private_key_env":"MODEL_B_KEY"}
			]`,
			want: []ModelConfig{
				{ModelID: "model-a", TempCount: 2, TargetCount: 5, Amount: 1000000, PrivateKeyEnv: "MODEL_A_KEY"},
				{ModelID: "model-b", TempCount: 1, TargetCount: 3, Amount: 2000000, PrivateKeyEnv: "MODEL_B_KEY"},
			},
		},
		{name: "blank string returns nil", raw: "", want: nil},
		{name: "whitespace-only returns nil", raw: "   \n\t ", want: nil},
		{name: "malformed JSON errors", raw: "{not valid", wantErr: true},
		{name: "empty model_id rejected", raw: `[{"model_id":"","temp_count":1,"target_count":1,"amount":1,"private_key_env":"K"}]`, wantErr: true},
		{name: "zero target_count rejected", raw: `[{"model_id":"m","temp_count":1,"target_count":0,"amount":1,"private_key_env":"K"}]`, wantErr: true},
		{name: "negative target_count rejected", raw: `[{"model_id":"m","temp_count":1,"target_count":-1,"amount":1,"private_key_env":"K"}]`, wantErr: true},
		{name: "zero amount rejected", raw: `[{"model_id":"m","temp_count":1,"target_count":1,"amount":0,"private_key_env":"K"}]`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseModels(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseModels(%q) error = nil, want error", tt.raw)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseModels(%q) unexpected error: %v", tt.raw, err)
			}
			if tt.want == nil {
				if got != nil {
					t.Fatalf("parseModels(%q) = %+v, want nil", tt.raw, got)
				}
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseModels(%q) len = %d, want %d", tt.raw, len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseModels(%q)[%d] = %+v, want %+v", tt.raw, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestParseModelsErrorNamesOffendingModel(t *testing.T) {
	_, err := parseModels(`[{"model_id":"broken-model","temp_count":0,"target_count":0,"amount":5,"private_key_env":"K"}]`)
	if err == nil {
		t.Fatal("parseModels() error = nil, want error naming the offending model")
	}
	if !strings.Contains(err.Error(), "broken-model") {
		t.Fatalf("parseModels() error = %q, want it to mention model %q", err.Error(), "broken-model")
	}
}

func TestServedByNetwork(t *testing.T) {
	tests := []struct {
		name       string
		snapshot   chain.PhaseSnapshot
		modelID    string
		wantServed bool
		wantKnown  bool
	}{
		{
			name: "model present in FullWeightsByModel",
			snapshot: chain.PhaseSnapshot{
				FullWeightsByModel: map[string]map[string]float64{
					"model-a": {"participantA": 100},
				},
			},
			modelID:    "model-a",
			wantServed: true,
			wantKnown:  true,
		},
		{
			name: "model absent but network non-empty",
			snapshot: chain.PhaseSnapshot{
				FullWeightsByModel: map[string]map[string]float64{
					"model-a": {"participantA": 100},
				},
			},
			modelID:    "model-b",
			wantServed: false,
			wantKnown:  true,
		},
		{
			name:       "empty snapshot is cold start",
			snapshot:   chain.PhaseSnapshot{},
			modelID:    "model-a",
			wantServed: false,
			wantKnown:  false,
		},
		{
			name: "model present only in CurrentWeightsByModel counts as served",
			snapshot: chain.PhaseSnapshot{
				CurrentWeightsByModel: map[string]map[string]float64{
					"model-b": {"participantB": 50},
				},
			},
			modelID:    "model-b",
			wantServed: true,
			wantKnown:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			served, known := servedByNetwork(tt.snapshot, tt.modelID)
			if served != tt.wantServed || known != tt.wantKnown {
				t.Errorf("servedByNetwork(%q) = (served=%v, known=%v), want (served=%v, known=%v)",
					tt.modelID, served, known, tt.wantServed, tt.wantKnown)
			}
		})
	}
}
