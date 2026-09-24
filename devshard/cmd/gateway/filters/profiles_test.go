package filters

import "testing"

// Test flow:
//  1. Look up the profile for kimiModelID.
//  2. Assert it equals kimiProfile.
func TestProfileForMatchesKimi(t *testing.T) {
	profile := ProfileFor(kimiModelID)
	if profile != kimiProfile {
		t.Fatalf("ProfileFor(%q) = %v, want kimiProfile", kimiModelID, profile)
	}
}

// Test flow:
//  1. Look up the profile for minimaxModelID.
//  2. Assert it equals minimaxProfile.
func TestProfileForMatchesMinimax(t *testing.T) {
	profile := ProfileFor(minimaxModelID)
	if profile != minimaxProfile {
		t.Fatalf("ProfileFor(%q) = %v, want minimaxProfile", minimaxModelID, profile)
	}
}

// Test flow:
//  1. Look up the profile for a table of routed model names: the default qwen model, an empty string, near-miss version strings for kimi, minimax and glm, and a kimi ID with leading whitespace.
//  2. Assert ProfileFor returns nil for every case.
func TestProfileForUnknownModelReturnsNil(t *testing.T) {
	tests := []struct {
		name        string
		routedModel string
	}{
		{"qwen uses the default profile", "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"},
		{"empty routed model", ""},
		{"near-miss kimi version does not match", "moonshotai/Kimi-K2.5"},
		{"near-miss minimax version does not match", "MiniMaxAI/MiniMax-M2"},
		{"near-miss glm variant does not match", "zai-org/GLM-5.3"},
		{"kimi id with leading whitespace does not match", " " + kimiModelID},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if profile := ProfileFor(testCase.routedModel); profile != nil {
				t.Errorf("ProfileFor(%q) = %v, want nil", testCase.routedModel, profile)
			}
		})
	}
}

// Test flow:
//  1. Compare kimiModelID, minimaxModelID and glm53FlashModelID against their expected literal strings.
//  2. Assert each constant equals its expected value.
func TestModelIDLiterals(t *testing.T) {
	if kimiModelID != "moonshotai/Kimi-K2.6" {
		t.Errorf("kimiModelID = %q, want %q", kimiModelID, "moonshotai/Kimi-K2.6")
	}
	if minimaxModelID != "MiniMaxAI/MiniMax-M2.7" {
		t.Errorf("minimaxModelID = %q, want %q", minimaxModelID, "MiniMaxAI/MiniMax-M2.7")
	}
	if glm53FlashModelID != "zai-org/GLM-5.3-Flash" {
		t.Errorf("glm53FlashModelID = %q, want %q", glm53FlashModelID, "zai-org/GLM-5.3-Flash")
	}
}
