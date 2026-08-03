package filters

import "strings"

func runPipeline(document *Document, options Options) (Result, error) {
	routedModel := resolveRoutedModel(document, options.RoutedModel)
	profile := ProfileFor(routedModel)

	unwrapExtraBody(document)
	if err := rejectUnknownParameters(document); err != nil {
		return Result{}, err
	}
	if err := applyStage(StagePreValidation, document, profile); err != nil {
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
	if err := applyStage(StagePostLimits, document, profile); err != nil {
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
		RequiresTools:       requiresTools(document),
		Model:               view.Model,
		Stream:              view.Stream,
		MaxTokens:           view.MaxTokens,
		MaxCompletionTokens: view.MaxCompletionTokens,
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

func applyStage(stage Stage, document *Document, profile *Profile) error {
	for _, spec := range parameterTable {
		for _, rule := range spec.Rules {
			if rule.Stage != stage || rule.Apply == nil {
				continue
			}
			ruleContext := RuleContext{
				Document: document,
				Param:    spec.Name,
				Profile:  profile,
			}
			if err := rule.Apply(ruleContext); err != nil {
				return err
			}
		}
	}
	return nil
}

// requiresTools reads the decoded document the pipeline already holds. The answer used to come from a
// second full unmarshal of the finished body, on every request, to produce one boolean.
func requiresTools(document *Document) bool {
	if tools, exists := document.Array("tools"); exists && len(tools) > 0 {
		return true
	}
	choice, exists := document.Get("tool_choice")
	if !exists {
		return false
	}
	if named, isString := choice.(string); isString {
		return named != "none"
	}
	return true
}
