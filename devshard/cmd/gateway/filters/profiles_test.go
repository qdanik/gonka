package filters

import "testing"

func TestProfileForMatchesKimi(t *testing.T) {
	profile := ProfileFor(kimiModelID)
	if profile != kimiProfile {
		t.Fatalf("ProfileFor(%q) = %v, want kimiProfile", kimiModelID, profile)
	}
}

func TestProfileForMatchesMinimax(t *testing.T) {
	profile := ProfileFor(minimaxModelID)
	if profile != minimaxProfile {
		t.Fatalf("ProfileFor(%q) = %v, want minimaxProfile", minimaxModelID, profile)
	}
}

func TestProfileForUnknownModelReturnsNil(t *testing.T) {
	tests := []struct {
		name        string
		routedModel string
	}{
		{"qwen uses the default profile", "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"},
		{"empty routed model", ""},
		{"near-miss kimi version does not match", "moonshotai/Kimi-K2.5"},
		{"near-miss minimax version does not match", "MiniMaxAI/MiniMax-M2"},
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

// Pins the exact model-ID literals the parameter table dispatches on.
func TestModelIDLiterals(t *testing.T) {
	if kimiModelID != "moonshotai/Kimi-K2.6" {
		t.Errorf("kimiModelID = %q, want %q", kimiModelID, "moonshotai/Kimi-K2.6")
	}
	if minimaxModelID != "MiniMaxAI/MiniMax-M2.7" {
		t.Errorf("minimaxModelID = %q, want %q", minimaxModelID, "MiniMaxAI/MiniMax-M2.7")
	}
}
