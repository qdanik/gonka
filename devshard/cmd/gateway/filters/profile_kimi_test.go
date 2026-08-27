package filters

import "testing"

func TestKimiProfileHooks(t *testing.T) {
	if len(kimiProfile.Models) != 1 || kimiProfile.Models[0] != kimiModelID {
		t.Errorf("kimiProfile.Models = %v, want [%q]", kimiProfile.Models, kimiModelID)
	}
	if kimiProfile.Thinking != ThinkingMirrorToKwargs {
		t.Errorf("kimiProfile.Thinking = %v, want ThinkingMirrorToKwargs", kimiProfile.Thinking)
	}
	boolHooks := []struct {
		name string
		got  bool
		want bool
	}{
		{"ForceZeroPenalties", kimiProfile.ForceZeroPenalties, true},
		{"RejectStructuredOutput", kimiProfile.RejectStructuredOutput, true},
		{"AllowSafetyIdentifier", kimiProfile.AllowSafetyIdentifier, true},
		{"KeepReasoningSplit", kimiProfile.KeepReasoningSplit, false},
		{"ThinkingTokenBudget", kimiProfile.ThinkingTokenBudget, true},
	}
	for _, hook := range boolHooks {
		if hook.got != hook.want {
			t.Errorf("kimiProfile.%s = %v, want %v", hook.name, hook.got, hook.want)
		}
	}
}

// Pins the thinking_token_budget resolution constants against their documented values.
func TestKimiThinkingBudgetConstants(t *testing.T) {
	tests := []struct {
		name string
		got  uint64
		want uint64
	}{
		{"force-zero threshold", kimiThinkingBudgetForceZeroBelow, 256},
		{"absolute max", thinkingBudgetAbsoluteMax, 96_000},
		{"content headroom", thinkingBudgetContentHeadroom, 64},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.got != testCase.want {
				t.Errorf("%s = %d, want %d", testCase.name, testCase.got, testCase.want)
			}
		})
	}
}
