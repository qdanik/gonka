package config

import (
	"testing"
)

func TestOverridesJSONRoundTrip(t *testing.T) {
	maxTokens := int64(2048)
	disabled := true
	modelMaxConcurrentRequests := int64(64)
	modelMaxInputTokensInFlight := int64(8192)
	original := Overrides{
		DefaultMaxTokens: &maxTokens,
		Disabled:         &disabled,
		ModelAccess:      map[string]string{"model-a": "api_key"},
		ModelLimits: map[string]ModelLimits{
			"model-a": {
				DefaultMaxTokens:       1024,
				MaxTokensCap:           2048,
				MaxConcurrentRequests:  &modelMaxConcurrentRequests,
				MaxInputTokensInFlight: &modelMaxInputTokensInFlight,
			},
		},
	}

	encoded, err := original.MarshalJSONBytes()
	if err != nil {
		t.Fatalf("MarshalJSONBytes(): %v", err)
	}
	decoded, err := ParseOverrides(encoded)
	if err != nil {
		t.Fatalf("ParseOverrides(): %v", err)
	}
	if decoded.DefaultMaxTokens == nil || *decoded.DefaultMaxTokens != 2048 {
		t.Fatalf("DefaultMaxTokens = %v, want 2048", decoded.DefaultMaxTokens)
	}
	if decoded.MaxTokensCap != nil {
		t.Fatalf("MaxTokensCap = %v, want nil (never set)", *decoded.MaxTokensCap)
	}
	if decoded.ModelAccess["model-a"] != "api_key" {
		t.Fatalf("ModelAccess = %v, want model-a→api_key", decoded.ModelAccess)
	}
	if decoded.ModelLimits["model-a"].MaxTokensCap != 2048 {
		t.Fatalf("ModelLimits = %v, want model-a cap 2048", decoded.ModelLimits)
	}
	decodedMaxConcurrentRequests := decoded.ModelLimits["model-a"].MaxConcurrentRequests
	if decodedMaxConcurrentRequests == nil || *decodedMaxConcurrentRequests != 64 {
		t.Fatalf("ModelLimits[model-a].MaxConcurrentRequests = %v, want 64", decodedMaxConcurrentRequests)
	}
	// Amendment E carried case: the round trip must also preserve the
	// per-model MaxInputTokensInFlight pointer, not just MaxConcurrentRequests.
	decodedMaxInputTokensInFlight := decoded.ModelLimits["model-a"].MaxInputTokensInFlight
	if decodedMaxInputTokensInFlight == nil || *decodedMaxInputTokensInFlight != 8192 {
		t.Fatalf("ModelLimits[model-a].MaxInputTokensInFlight = %v, want 8192", decodedMaxInputTokensInFlight)
	}
}

func TestParseOverridesRejectsUnknownFields(t *testing.T) {
	_, err := ParseOverrides([]byte(`{"no_such_setting": 1}`))
	if err == nil {
		t.Fatal("ParseOverrides with unknown field: want error, got nil (admin typos must not be silently ignored)")
	}
}

func TestParseOverridesOfEmptyObjectIsEmpty(t *testing.T) {
	decoded, err := ParseOverrides([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseOverrides(empty): %v", err)
	}
	if decoded.DefaultMaxTokens != nil || decoded.Disabled != nil || len(decoded.ModelAccess) != 0 {
		t.Fatalf("ParseOverrides(empty) = %+v, want zero value", decoded)
	}
}
