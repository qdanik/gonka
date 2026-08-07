package filters

import "devshard"

var allowedReasoningEffortValues = map[string]struct{}{
	"none": {}, "minimal": {}, "low": {}, "medium": {}, "high": {}, "xhigh": {},
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
// enum values. The field is stripped by a separate table rule (no route serves it today).
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

func thinkingTokenBudgetStrip() RuleFunc {
	return stripParamUnlessHook(func(profile *Profile) bool {
		return profile != nil && profile.ThinkingTokenBudget
	})
}

// thinkingTokenBudgetResolve implements kimi's budget resolution: force 0 below a small
// max_tokens, else default to max_tokens/divisor, cap at an absolute max, then clamp to
// max_tokens minus a content headroom. A string-typed client value fails the uint64 coercion
// and is left completely untouched -- a documented bypass of every clamp below.
func thinkingTokenBudgetResolve() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile == nil || !ctx.Profile.ThinkingTokenBudget {
			return nil
		}
		maxTokensRaw, exists := ctx.Document.Get("max_tokens")
		if !exists {
			return nil
		}
		maxTokens, ok := devshard.JSONNumericUint64(maxTokensRaw)
		if !ok || maxTokens == 0 {
			return nil
		}
		if maxTokens < kimiThinkingBudgetForceZeroBelow {
			ctx.Document.Set("thinking_token_budget", uint64(0))
			// The budget alone is a logits processor, which speculative decoding discards. See
			// gateway-request-filtering.md, "Silencing Kimi's reasoning".
			return mirrorFieldIntoKwargs(ctx.Document, "thinking", false)
		}
		if !ctx.Document.Has("thinking_token_budget") {
			ctx.Document.Set("thinking_token_budget", maxTokens/kimiThinkingBudgetDivisor)
		}
		budgetRaw, _ := ctx.Document.Get("thinking_token_budget")
		budget, ok := devshard.JSONNumericUint64(budgetRaw)
		if !ok {
			return nil
		}
		if budget > kimiThinkingBudgetAbsoluteMax {
			budget = kimiThinkingBudgetAbsoluteMax
		}
		var headroomCap uint64
		if maxTokens > kimiThinkingBudgetContentHeadroom {
			headroomCap = maxTokens - kimiThinkingBudgetContentHeadroom
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

// stripParamUnlessHook passes ctx.Param through when hook(ctx.Profile) is true, strips it
// otherwise -- the recurring "keep only for profiles carrying this hook" scoping shape.
func stripParamUnlessHook(hook func(*Profile) bool) RuleFunc {
	return func(ctx RuleContext) error {
		if hook(ctx.Profile) {
			return nil
		}
		ctx.Document.Delete(ctx.Param)
		return nil
	}
}

func reasoningSplit() RuleFunc {
	return stripParamUnlessHook(func(profile *Profile) bool {
		return profile != nil && profile.KeepReasoningSplit
	})
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
