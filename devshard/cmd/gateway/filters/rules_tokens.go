package filters

import "devshard"

// Package-default output-token limits, used when Options passes a zero limit and as the single
// source config.Defaults reads.
const (
	DefaultRequestMaxTokens uint64 = 3_072
	RequestMaxTokensCap     uint64 = 4_096
)

type outputTokenLimits struct {
	DefaultMaxTokens uint64
	MaxTokensCap     uint64
}

func normalizedOutputTokenLimits(limits outputTokenLimits) outputTokenLimits {
	if limits.DefaultMaxTokens == 0 {
		limits.DefaultMaxTokens = DefaultRequestMaxTokens
	}
	if limits.MaxTokensCap == 0 {
		limits.MaxTokensCap = RequestMaxTokensCap
	}
	return limits
}

// resolveOutputTokenLimits lets a per-model override replace either global limit; a zero the
// override returns means "not set for this model" and keeps the global one.
func resolveOutputTokenLimits(options Options, routedModel string) outputTokenLimits {
	limits := outputTokenLimits{DefaultMaxTokens: options.DefaultMaxTokens, MaxTokensCap: options.MaxTokensCap}
	if options.ModelTokenLimits == nil {
		return limits
	}
	modelDefault, modelCap := options.ModelTokenLimits(routedModel)
	if modelDefault > 0 {
		limits.DefaultMaxTokens = modelDefault
	}
	if modelCap > 0 {
		limits.MaxTokensCap = modelCap
	}
	return limits
}

// capOutputTokens: 0 always means unset (use default); a nonzero value clamps unless bypassed.
func capOutputTokens(value uint64, bypassLimit bool, limits outputTokenLimits) uint64 {
	limits = normalizedOutputTokenLimits(limits)
	if value == 0 {
		return limits.DefaultMaxTokens
	}
	if !bypassLimit && value > limits.MaxTokensCap {
		return limits.MaxTokensCap
	}
	return value
}

// requestView is the typed projection of the 5 fields the pipeline needs outside the raw document.
type requestView struct {
	Model               string
	Stream              bool
	MaxTokens           uint64
	MaxCompletionTokens uint64
	N                   uint64
}

func decodeRequestView(document *Document) (requestView, error) {
	var view requestView
	if raw, ok := document.Get("model"); ok && raw != nil {
		modelName, isString := raw.(string)
		if !isString {
			return requestView{}, Reject("parse request: model must be a string")
		}
		view.Model = modelName
	}
	if raw, ok := document.Get("stream"); ok && raw != nil {
		streamFlag, isBool := raw.(bool)
		if !isBool {
			return requestView{}, Reject("parse request: stream must be a boolean")
		}
		view.Stream = streamFlag
	}
	if err := decodeUint64Field(document, "max_tokens", &view.MaxTokens); err != nil {
		return requestView{}, err
	}
	if err := decodeUint64Field(document, "max_completion_tokens", &view.MaxCompletionTokens); err != nil {
		return requestView{}, err
	}
	if err := decodeUint64Field(document, "n", &view.N); err != nil {
		return requestView{}, err
	}
	return view, nil
}

// syncRequestView re-decodes view post-StagePostLimits, preserving the token fields.
func syncRequestView(document *Document, view *requestView) error {
	refreshed, err := decodeRequestView(document)
	if err != nil {
		return err
	}
	refreshed.MaxTokens = view.MaxTokens
	refreshed.MaxCompletionTokens = view.MaxCompletionTokens
	*view = refreshed
	return nil
}

func decodeUint64Field(document *Document, name string, dst *uint64) error {
	raw, ok := document.Get(name)
	if !ok || raw == nil {
		return nil
	}
	value, ok := devshard.JSONNumericUint64(raw)
	if !ok {
		return Reject("parse request: %s must be a non-negative integer", name)
	}
	*dst = value
	return nil
}

// clampUintToField clamps ctx.Param down to document[fieldName] when both parse as uint64
// and fieldName's value is nonzero; a missing, zero, or non-numeric fieldName disables the clamp.
func clampUintToField(fieldName string) RuleFunc {
	return func(ctx RuleContext) error {
		value, ok := ctx.Document.Uint(ctx.Param)
		if !ok {
			return nil
		}
		limit, ok := ctx.Document.Uint(fieldName)
		if !ok || limit == 0 {
			return nil
		}
		if value > limit {
			ctx.Document.Set(ctx.Param, limit)
		}
		return nil
	}
}

// stripWhenFieldPresent deletes ctx.Param whenever fieldName is present, regardless of
// fieldName's own value or validity.
func stripWhenFieldPresent(fieldName string) RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Document.Has(fieldName) {
			ctx.Document.Delete(ctx.Param)
		}
		return nil
	}
}

// greedySamplingForceOne coerces n to 1 when temperature reads as exactly 0 (vLLM rejects n>1
// under greedy); must run before temperature's own clamp rule so it reads the raw client value.
func greedySamplingForceOne() RuleFunc {
	return func(ctx RuleContext) error {
		n, ok := ctx.Document.Uint("n")
		if !ok || n <= 1 {
			return nil
		}
		temperatureRaw, exists := ctx.Document.Get("temperature")
		if !exists {
			return nil
		}
		temperature, ok := devshard.JSONNumericFloat64(temperatureRaw)
		if !ok || temperature != 0 {
			return nil
		}
		ctx.Document.Set("n", uint64(1))
		return nil
	}
}

// applyOutputTokenLimits resolves max_tokens from whichever field(s) the client sent (the min
// of both when both are present), then mirrors it into max_completion_tokens iff that was sent.
func applyOutputTokenLimits(document *Document, view *requestView, options Options, routedModel string) {
	_, hasMaxTokens := document.Get("max_tokens")
	_, hasMaxCompletionTokens := document.Get("max_completion_tokens")
	limits := resolveOutputTokenLimits(options, routedModel)

	var resolved uint64
	switch {
	case hasMaxTokens && hasMaxCompletionTokens:
		resolved = min(
			capOutputTokens(view.MaxTokens, options.Admin, limits),
			capOutputTokens(view.MaxCompletionTokens, options.Admin, limits),
		)
	case hasMaxTokens:
		resolved = capOutputTokens(view.MaxTokens, options.Admin, limits)
	case hasMaxCompletionTokens:
		resolved = capOutputTokens(view.MaxCompletionTokens, options.Admin, limits)
	default:
		resolved = capOutputTokens(0, options.Admin, limits)
	}
	document.Set("max_tokens", resolved)
	view.MaxTokens = resolved
	if hasMaxCompletionTokens {
		document.Set("max_completion_tokens", resolved)
		view.MaxCompletionTokens = resolved
	} else {
		view.MaxCompletionTokens = 0
	}
}

// maxTokensFloor lifts ctx.Param up to the profile's floor when the parsed value falls below it.
func maxTokensFloor() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile == nil || ctx.Profile.MaxTokensFloor == 0 {
			return nil
		}
		value, ok := ctx.Document.Uint(ctx.Param)
		if !ok {
			return nil
		}
		if value < ctx.Profile.MaxTokensFloor {
			ctx.Document.Set(ctx.Param, ctx.Profile.MaxTokensFloor)
		}
		return nil
	}
}

// rejectNonPositiveOutputTokens rejects ctx.Param when present, numeric, and <= 0. Skipped for
// profiles with their own floor, which normalize a low value instead of rejecting it.
func rejectNonPositiveOutputTokens() RuleFunc {
	return func(ctx RuleContext) error {
		if ctx.Profile != nil && ctx.Profile.MaxTokensFloor > 0 {
			return nil
		}
		raw, exists := ctx.Document.Get(ctx.Param)
		if !exists {
			return nil
		}
		number, ok := devshard.JSONNumericFloat64(raw)
		if !ok {
			return nil
		}
		if number > 0 {
			return nil
		}
		return Reject("%s: must be greater than 0", ctx.Param)
	}
}
