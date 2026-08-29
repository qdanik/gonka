package filters

import "devshard"

// Clamps applied to any thinking_token_budget, whichever model it was sent for.
const (
	thinkingBudgetAbsoluteMax     uint64 = 96_000
	thinkingBudgetContentHeadroom uint64 = 64
	thinkingBudgetDivisor         uint64 = 2
)

var allowedReasoningEffortValues = map[string]struct{}{
	"none": {}, "minimal": {}, "low": {}, "medium": {}, "high": {}, "xhigh": {}, "max": {},
}

// reasoningWrapper strips the reasoning wrapper, lifting .effort into a not-yet-present
// reasoning_effort unless the wrapper carries enabled:false.
func reasoningWrapper() RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get("reasoning")
		if !exists {
			return nil
		}
		ctx.Document.Delete("reasoning")
		wrapper, ok := raw.(map[string]any)
		if !ok {
			return nil
		}
		if enabled, ok := wrapper["enabled"].(bool); ok && !enabled {
			ctx.Document.Set("reasoning_effort", "none")
			return nil
		}
		effort, hasEffort := wrapper["effort"]
		if !hasEffort || ctx.Document.Has("reasoning_effort") {
			return nil
		}
		ctx.Document.Set("reasoning_effort", effort)
		return nil
	}
}

// reasoningEffortValidate rejects reasoning_effort when present and not one of the allowed
// enum values. Scoping to the routes that read it is a separate table rule.
func reasoningEffortValidate() RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get("reasoning_effort")
		if !exists {
			return nil
		}
		value, ok := raw.(string)
		if !ok {
			return Reject("reasoning_effort: invalid shape: must be a string")
		}
		if _, allowed := allowedReasoningEffortValues[value]; !allowed {
			return Reject("reasoning_effort: unsupported value: got %q", value)
		}
		return nil
	}
}

func reasoningEffortScope() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile == nil || ctx.Profile.ReasoningEffortDefault == "" {
			ctx.Document.Delete(ctx.Param)
			return nil
		}
		if !ctx.Document.Has(ctx.Param) {
			ctx.Document.Set(ctx.Param, ctx.Profile.ReasoningEffortDefault)
		}
		return nil
	}
}

// enableThinking strips for ThinkingStrip profiles (no matching chat-template knob);
// otherwise validates the bool and mirrors it into chat_template_kwargs.enable_thinking,
// preserving any value already nested there.
func enableThinking() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile != nil && ctx.Profile.Thinking == ThinkingStrip {
			ctx.Document.Delete("enable_thinking")
			return nil
		}
		raw, exists := ctx.Document.Get("enable_thinking")
		if !exists {
			return nil
		}
		value, ok := raw.(bool)
		if !ok {
			return Reject("enable_thinking: must be a boolean: got %T", raw)
		}
		return mirrorFieldIntoKwargs(ctx.Document, "enable_thinking", value)
	}
}

// thinking strips for ThinkingStrip profiles; mirrors into chat_template_kwargs.thinking for
// ThinkingMirrorToKwargs profiles (dropping the top-level field); otherwise normalizes the
// type enum in place and drops the display hint.
func thinking() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile != nil && ctx.Profile.Thinking == ThinkingStrip {
			ctx.Document.Delete("thinking")
			return nil
		}
		raw, exists := ctx.Document.Get("thinking")
		if !exists {
			return nil
		}
		wrapper, ok := raw.(map[string]any)
		if !ok {
			return Reject("thinking: invalid wrapper shape: must be an object")
		}
		typeRaw, hasType := wrapper["type"]
		if !hasType {
			return Reject("thinking.type: must be \"enabled\", \"disabled\", \"adaptive\", or \"auto\": type is required")
		}
		typeValue, ok := typeRaw.(string)
		if !ok {
			return Reject("thinking.type: must be \"enabled\", \"disabled\", \"adaptive\", or \"auto\": type must be a string")
		}
		enabled, ok := resolveThinkingType(typeValue)
		if !ok {
			return Reject("thinking.type: must be \"enabled\", \"disabled\", \"adaptive\", or \"auto\": got %q", typeValue)
		}
		if ctx.Profile != nil && ctx.Profile.Thinking == ThinkingMirrorToKwargs {
			return mirrorFieldIntoKwargs(ctx.Document, "thinking", enabled)
		}
		if enabled {
			wrapper["type"] = "enabled"
		} else {
			wrapper["type"] = "disabled"
		}
		delete(wrapper, "display")
		return nil
	}
}

// resolveThinkingType maps thinking.type to its boolean intent; adaptive/auto both signal
// opt-in thinking with an SDK-chosen budget. The second return is false for an unknown type.
func resolveThinkingType(typeValue string) (bool, bool) {
	switch typeValue {
	case "enabled", "adaptive", "auto":
		return true, true
	case "disabled":
		return false, true
	default:
		return false, false
	}
}

// getOrCreateChatTemplateKwargs returns the document's chat_template_kwargs object, creating
// an empty one when absent; rejects a present non-object value.
func getOrCreateChatTemplateKwargs(document *Document) (map[string]any, error) {
	raw, exists := document.Get("chat_template_kwargs")
	if !exists {
		kwargs := map[string]any{}
		document.Set("chat_template_kwargs", kwargs)
		return kwargs, nil
	}
	kwargs, ok := raw.(map[string]any)
	if !ok {
		return nil, Reject("chat_template_kwargs: invalid wrapper shape: must be an object")
	}
	return kwargs, nil
}

// mirrorFieldIntoKwargs moves a top-level bool into chat_template_kwargs[field], preserving
// any value already nested there; the top-level field is always removed.
func mirrorFieldIntoKwargs(document *Document, field string, value bool) error {
	kwargs, err := getOrCreateChatTemplateKwargs(document)
	if err != nil {
		return err
	}
	document.Delete(field)
	if _, exists := kwargs[field]; exists {
		return nil
	}
	kwargs[field] = value
	return nil
}

// silenceThinkingInKwargs overrules the caller, the way the forced budget already does: a request that
// cannot afford thinking cannot afford it in the template either.
func silenceThinkingInKwargs(document *Document) error {
	kwargs, err := getOrCreateChatTemplateKwargs(document)
	if err != nil {
		return err
	}
	kwargs["thinking"] = false
	return nil
}

// thinkingTokenBudgetResolve clamps whatever budget the request carries so content keeps room to be
// written, for every model. Profiles that own a resolution also get their force-zero and half-split default.
func thinkingTokenBudgetResolve() RuleFunc {
	return func(ctx RuleContext) error {
		maxTokensRaw, exists := ctx.Document.Get("max_tokens")
		if !exists {
			return nil
		}
		maxTokens, ok := devshard.JSONNumericUint64(maxTokensRaw)
		if !ok || maxTokens == 0 {
			return nil
		}
		// Only a profile that declares the budget gets one invented: a host on the V2 model runner rejects
		// the field outright, and V2 is the default for every non-MoE model.
		if ctx.Profile != nil && ctx.Profile.ThinkingTokenBudget {
			if maxTokens < kimiThinkingBudgetForceZeroBelow {
				ctx.Document.Set("thinking_token_budget", uint64(0))
				// The budget alone is a logits processor, which speculative decoding discards, and the
				// thinking rule has already mirrored the caller's answer here -- so this overwrites rather
				// than fills. See gateway-request-filtering.md, "Silencing Kimi's reasoning".
				return silenceThinkingInKwargs(ctx.Document)
			}
			if !ctx.Document.Has("thinking_token_budget") {
				ctx.Document.Set("thinking_token_budget", maxTokens/thinkingBudgetDivisor)
			}
		}
		budgetRaw, exists := ctx.Document.Get("thinking_token_budget")
		if !exists {
			return nil
		}
		budget, ok := devshard.JSONNumericUint64(budgetRaw)
		if !ok {
			return nil
		}
		if budget > thinkingBudgetAbsoluteMax {
			budget = thinkingBudgetAbsoluteMax
		}
		var headroomCap uint64
		if maxTokens > thinkingBudgetContentHeadroom {
			headroomCap = maxTokens - thinkingBudgetContentHeadroom
		}
		if budget > headroomCap {
			budget = headroomCap
		}
		ctx.Document.Set("thinking_token_budget", budget)
		return nil
	}
}

// safetyIdentifier validates and keeps ctx.Param for profiles with AllowSafetyIdentifier;
// strips it for every other profile.
func safetyIdentifier() RuleFunc {
	validate := requireString(safetyIdentifierMaxLen)
	return func(ctx RuleContext) error {
		if ctx.Profile == nil || !ctx.Profile.AllowSafetyIdentifier {
			ctx.Document.Delete(ctx.Param)
			return nil
		}
		return validate(ctx)
	}
}

// reasoningSplit strips the field for profiles that cannot serve it, and fills it for the one that can:
// M2.x thinks unconditionally, so without the split its reasoning arrives inline in content.
func reasoningSplit() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile == nil || !ctx.Profile.KeepReasoningSplit {
			ctx.Document.Delete(ctx.Param)
			return nil
		}
		raw, exists := ctx.Document.Get(ctx.Param)
		if !exists {
			ctx.Document.Set(ctx.Param, true)
			return nil
		}
		if _, ok := raw.(bool); !ok {
			return Reject("%s: must be a boolean: got %T", ctx.Param, raw)
		}
		return nil
	}
}

// forceZeroPenalty overwrites ctx.Param to 0 for profiles with ForceZeroPenalties, but only
// when the field is already present (overwrite-only: never introduces the field).
func forceZeroPenalty() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile == nil || !ctx.Profile.ForceZeroPenalties {
			return nil
		}
		if !ctx.Document.Has(ctx.Param) {
			return nil
		}
		ctx.Document.Set(ctx.Param, 0.0)
		return nil
	}
}
