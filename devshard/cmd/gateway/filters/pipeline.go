package filters

import "strings"

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
	if err := normalizeMessages(document); err != nil {
		return Result{}, err
	}
	if err := validateMessages(document); err != nil {
		return Result{}, err
	}
	view, err := decodeRequestView(document)
	if err != nil {
		return Result{}, err
	}
	applyOutputTokenLimits(document, &view, options, routedModel)
	// Read before StagePostLimits: that stage forces logprobs on for validation, so afterwards the
	// document says what the gateway wants, not what the client asked for.
	logprobs := decodeLogprobIntent(document)
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
		Logprobs:            logprobs,
	}, nil
}

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
