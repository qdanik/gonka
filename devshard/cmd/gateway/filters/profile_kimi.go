package filters

// kimi's own thinking_token_budget resolution constants. The clamps every model shares live in rules_reasoning.go.
const (
	kimiThinkingBudgetForceZeroBelow uint64 = 256
)

// kimiProfile is Kimi's delta set. See README.md, "Model profiles".
var kimiProfile = &Profile{
	Models:                      []string{kimiModelID},
	ForceZeroPenalties:          true,
	RejectStructuredOutput:      true,
	AllowSafetyIdentifier:       true,
	Thinking:                    ThinkingMirrorToKwargs,
	ThinkingTokenBudget:         true,
	LiftNonPositiveOutputTokens: true,
}
