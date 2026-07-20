package filters

import "testing"

func TestMinimaxProfileHooks(t *testing.T) {
	if len(minimaxProfile.Models) != 1 || minimaxProfile.Models[0] != minimaxModelID {
		t.Errorf("minimaxProfile.Models = %v, want [%q]", minimaxProfile.Models, minimaxModelID)
	}
	if minimaxProfile.MaxTokensFloor != 0 {
		t.Errorf("minimaxProfile.MaxTokensFloor = %d, want 0", minimaxProfile.MaxTokensFloor)
	}
	if minimaxProfile.Thinking != ThinkingStrip {
		t.Errorf("minimaxProfile.Thinking = %v, want ThinkingStrip", minimaxProfile.Thinking)
	}
	boolHooks := []struct {
		name string
		got  bool
		want bool
	}{
		{"ForceZeroPenalties", minimaxProfile.ForceZeroPenalties, false},
		{"RejectStructuredOutput", minimaxProfile.RejectStructuredOutput, false},
		{"AllowSafetyIdentifier", minimaxProfile.AllowSafetyIdentifier, false},
		{"KeepReasoningSplit", minimaxProfile.KeepReasoningSplit, true},
		{"ThinkingTokenBudget", minimaxProfile.ThinkingTokenBudget, false},
	}
	for _, hook := range boolHooks {
		if hook.got != hook.want {
			t.Errorf("minimaxProfile.%s = %v, want %v", hook.name, hook.got, hook.want)
		}
	}
}
