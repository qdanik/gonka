package filters

import "strings"

// runPipeline runs the fixed normalization stage order: whitelist, pre-validation rules,
// message hygiene, output-token limits, post-limit rules, then marshal.
func runPipeline(document *Document, options Options) (Result, error) {
	routedModel := resolveRoutedModel(document, options.RoutedModel)
	profile := ProfileFor(routedModel)

	unwrapExtraBody(document)
	if err := rejectUnknownParameters(document); err != nil {
		return Result{}, err
	}
	if err := applyStage(StagePreValidation, document, routedModel, profile, options.Admin); err != nil {
		return Result{}, err
	}
	if err := normalizeMessages(document, routedModel); err != nil {
		return Result{}, err
	}
	if err := validateMessages(document, routedModel); err != nil {
		return Result{}, err
	}
	view, err := decodeRequestView(document)
	if err != nil {
		return Result{}, err
	}
	applyOutputTokenLimits(document, &view, options)
	if err := applyStage(StagePostLimits, document, routedModel, profile, options.Admin); err != nil {
		return Result{}, err
	}
	if err := syncRequestView(document, &view); err != nil {
		return Result{}, err
	}
	body, err := document.Marshal()
	if err != nil {
		return Result{}, err
	}
	return Result{
		Body:                body,
		Model:               view.Model,
		Stream:              view.Stream,
		MaxTokens:           view.MaxTokens,
		MaxCompletionTokens: view.MaxCompletionTokens,
		N:                   view.N,
	}, nil
}

// resolveRoutedModel: trimmed body.model wins, else fallback.
func resolveRoutedModel(document *Document, fallback string) string {
	if raw, ok := document.Get("model"); ok {
		if modelName, isString := raw.(string); isString {
			if trimmed := strings.TrimSpace(modelName); trimmed != "" {
				return trimmed
			}
		}
	}
	return fallback
}

// applyStage runs every rule registered for stage, in table order.
func applyStage(stage Stage, document *Document, routedModel string, profile *Profile, admin bool) error {
	for _, spec := range parameterTable {
		for _, rule := range spec.Rules {
			if rule.Stage != stage || rule.Apply == nil {
				continue
			}
			ruleContext := RuleContext{
				Document:    document,
				Param:       spec.Name,
				RoutedModel: routedModel,
				Admin:       admin,
				Profile:     profile,
			}
			if err := rule.Apply(ruleContext); err != nil {
				return err
			}
		}
	}
	return nil
}
