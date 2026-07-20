package filters

import (
	"math"

	"devshard"
)

// requireUint rejects ctx.Param when present and not a non-negative integer; absent/null pass.
func requireUint() RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get(ctx.Param)
		if !exists || raw == nil {
			return nil
		}
		if _, ok := devshard.JSONNumericUint64(raw); !ok {
			return Reject("%s: must be a non-negative integer", ctx.Param)
		}
		return nil
	}
}

// requireBool rejects ctx.Param when present and not a boolean; absent/null pass.
func requireBool() RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get(ctx.Param)
		if !exists || raw == nil {
			return nil
		}
		if _, ok := raw.(bool); !ok {
			return Reject("%s: must be a boolean", ctx.Param)
		}
		return nil
	}
}

// requireString rejects ctx.Param when present and not a string, or longer than maxBytes.
// Unlike requireUint/requireBool, an explicit null is rejected rather than treated as absent.
func requireString(maxBytes int) RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get(ctx.Param)
		if !exists {
			return nil
		}
		value, ok := raw.(string)
		if !ok {
			return Reject("%s: invalid wrapper shape: must be a string", ctx.Param)
		}
		if len(value) > maxBytes {
			return Reject("%s: length exceeded: %d > %d", ctx.Param, len(value), maxBytes)
		}
		return nil
	}
}

// stripParameter unconditionally deletes ctx.Param.
func stripParameter() RuleFunc {
	return func(ctx RuleContext) error {
		ctx.Document.Delete(ctx.Param)
		return nil
	}
}

// forceLiteral unconditionally overwrites ctx.Param with value, present or absent.
func forceLiteral(value any) RuleFunc {
	return func(ctx RuleContext) error {
		ctx.Document.Set(ctx.Param, value)
		return nil
	}
}

// clampFloat coerces ctx.Param to float64, strips it when absent, unparseable, or
// non-finite, then clamps the result into [min, max] and writes it back.
func clampFloat(min, max float64) RuleFunc {
	return func(ctx RuleContext) error {
		number, ok := sanitizeFloatField(ctx.Document, ctx.Param)
		if !ok {
			return nil
		}
		if number < min {
			number = min
		}
		if number > max {
			number = max
		}
		ctx.Document.Set(ctx.Param, number)
		return nil
	}
}

// rejectNonPositiveThenClamp clamps ctx.Param down to max, then rejects a non-positive
// result — an exclusive lower bound can't be enforced by clamping without producing an
// illegal value.
func rejectNonPositiveThenClamp(max float64) RuleFunc {
	return func(ctx RuleContext) error {
		number, ok := sanitizeFloatField(ctx.Document, ctx.Param)
		if !ok {
			return nil
		}
		if number > max {
			number = max
		}
		if number <= 0 {
			return Reject("%s: must be greater than 0", ctx.Param)
		}
		ctx.Document.Set(ctx.Param, number)
		return nil
	}
}

// validTopK strips non-finite/unparseable values, else rejects anything but -1 (disabled)
// or a value >= 1.
func validTopK() RuleFunc {
	return func(ctx RuleContext) error {
		number, ok := sanitizeFloatField(ctx.Document, ctx.Param)
		if !ok {
			return nil
		}
		if number != -1 && number < 1 {
			return Reject("%s: must be -1 or a positive integer", ctx.Param)
		}
		ctx.Document.Set(ctx.Param, number)
		return nil
	}
}

// capUint clamps ctx.Param into [min, max] when it parses as a uint64; a non-numeric or
// absent field passes through untouched.
func capUint(min, max uint64) RuleFunc {
	return func(ctx RuleContext) error {
		raw, exists := ctx.Document.Get(ctx.Param)
		if !exists {
			return nil
		}
		value, ok := devshard.JSONNumericUint64(raw)
		if !ok {
			return nil
		}
		if value < min {
			ctx.Document.Set(ctx.Param, min)
			return nil
		}
		if value > max {
			ctx.Document.Set(ctx.Param, max)
		}
		return nil
	}
}

// sanitizeFloatField coerces document[name] to float64. ok is false, and the field is
// deleted, whenever it's absent, unparseable, or non-finite (NaN/Inf).
func sanitizeFloatField(document *Document, name string) (float64, bool) {
	raw, exists := document.Get(name)
	if !exists {
		return 0, false
	}
	number, ok := devshard.JSONNumericFloat64(raw)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) {
		document.Delete(name)
		return 0, false
	}
	return number, true
}
