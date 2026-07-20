package filters

import "sort"

// Stage identifies which pipeline phase a rule runs in.
type Stage int

const (
	// StagePreValidation runs before message hygiene and the token decode.
	StagePreValidation Stage = iota
	// StagePostLimits runs after output-token defaults/caps resolve.
	StagePostLimits
)

// RuleContext is the argument passed to every RuleFunc.
type RuleContext struct {
	Document    *Document
	Param       string
	RoutedModel string
	Admin       bool
	Profile     *Profile
}

// RuleFunc is one parameter rule: inspect/mutate ctx.Document, or reject.
type RuleFunc func(RuleContext) error

// StagedRule pairs a RuleFunc with the pipeline stage it runs in.
type StagedRule struct {
	Stage Stage
	Apply RuleFunc
}

// ParameterSpec declares one top-level parameter: its name and rules.
type ParameterSpec struct {
	Name  string
	Rules []StagedRule
}

// Per-parameter bounds wired into parameterTable below.
const (
	minChatChoices uint64 = 1
	maxChatChoices uint64 = 5

	minTemperature = 0.0
	maxTemperature = 2.0

	maxRepetitionPenalty = 2.0

	minPMin = 0.0
	minPMax = 1.0
	topPMax = 1.0

	penaltyMin = -2.0
	penaltyMax = 2.0

	topLogprobsForcedValue = 5

	userMaxLen             = 512
	safetyIdentifierMaxLen = 512

	messagesMaxEntries = 2048

	stopMaxEntries  = 16
	stopMaxEntryLen = 256

	stopTokenIdsMaxEntries = 64

	badWordsMaxEntries  = 64
	badWordsMaxEntryLen = 128

	logitBiasMinValue   = -100
	logitBiasMaxValue   = 100
	logitBiasMaxEntries = 1024

	metadataMaxKeys     = 16
	metadataMaxKeyLen   = 64
	metadataMaxValueLen = 512
)

// spec builds a ParameterSpec with a single staged rule.
func spec(name string, stage Stage, rule RuleFunc) ParameterSpec {
	return ParameterSpec{Name: name, Rules: []StagedRule{{Stage: stage, Apply: rule}}}
}

// parameterTable is THE single registration point; knownParameterSet derives from it.
var parameterTable = []ParameterSpec{
	{Name: "model"},
	{Name: "stream"},
	{Name: "max_tokens", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: maxTokensFloor()},
		{Stage: StagePreValidation, Apply: rejectNonPositiveOutputTokens()},
	}},
	{Name: "max_completion_tokens", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: maxTokensFloor()},
		{Stage: StagePreValidation, Apply: rejectNonPositiveOutputTokens()},
	}},
	{Name: "messages", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: capListLength(messagesMaxEntries, 0)},
	}},
	spec("seed", StagePreValidation, requireUint()),
	// n's greedy rule must stay after its own cap and before "temperature" below: greedy
	// reads temperature's raw wire value, ahead of temperature's own clamp rule.
	{Name: "n", Rules: []StagedRule{
		{Stage: StagePostLimits, Apply: capUint(minChatChoices, maxChatChoices)},
		{Stage: StagePostLimits, Apply: greedySamplingForceOne()},
	}},
	spec("temperature", StagePostLimits, clampFloat(minTemperature, maxTemperature)),
	spec("repetition_penalty", StagePostLimits, rejectNonPositiveThenClamp(maxRepetitionPenalty)),
	spec("top_p", StagePostLimits, rejectNonPositiveThenClamp(topPMax)),
	spec("min_p", StagePostLimits, clampFloat(minPMin, minPMax)),
	spec("top_k", StagePostLimits, validTopK()),
	{Name: "frequency_penalty", Rules: []StagedRule{
		{Stage: StagePostLimits, Apply: clampFloat(penaltyMin, penaltyMax)},
		{Stage: StagePostLimits, Apply: forceZeroPenalty()},
	}},
	{Name: "presence_penalty", Rules: []StagedRule{
		{Stage: StagePostLimits, Apply: clampFloat(penaltyMin, penaltyMax)},
		{Stage: StagePostLimits, Apply: forceZeroPenalty()},
	}},
	spec("logit_bias", StagePostLimits, capFloatMap(logitBiasMinValue, logitBiasMaxValue, logitBiasMaxEntries)),
	spec("skip_special_tokens", StagePreValidation, requireBool()),
	spec("detokenize", StagePreValidation, requireBool()),
	spec("parallel_tool_calls", StagePreValidation, requireBool()),
	spec("user", StagePreValidation, requireString(userMaxLen)),
	spec("logprobs", StagePostLimits, forceLiteral(true)),
	spec("top_logprobs", StagePostLimits, forceLiteral(topLogprobsForcedValue)),
	spec("return_token_ids", StagePostLimits, forceLiteral(true)),
	spec("service_tier", StagePreValidation, stripParameter()),
	spec("store", StagePreValidation, stripParameter()),
	spec("provider", StagePreValidation, stripParameter()),
	spec("plugins", StagePreValidation, stripParameter()),
	spec("prompt_cache_key", StagePreValidation, stripParameter()),
	spec("cache_key", StagePreValidation, stripParameter()),
	spec("extra_headers", StagePreValidation, stripParameter()),
	spec("thinking_config", StagePreValidation, stripParameter()),
	spec("think", StagePreValidation, stripParameter()),
	{Name: "stop", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: requireStringElements()},
		{Stage: StagePreValidation, Apply: capListLength(stopMaxEntries, stopMaxEntryLen)},
	}},
	// Cap runs before the element-type check here (reversed vs "stop" above): an
	// oversized array is rejected on size alone before any element is inspected.
	{Name: "stop_token_ids", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: capListLength(stopTokenIdsMaxEntries, 0)},
		{Stage: StagePreValidation, Apply: requireUintElements()},
	}},
	{Name: "bad_words", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: requireStringElements()},
		{Stage: StagePreValidation, Apply: dropBlankStringListElements()},
		{Stage: StagePreValidation, Apply: capListLength(badWordsMaxEntries, badWordsMaxEntryLen)},
	}},
	spec("metadata", StagePreValidation, validMetadata(metadataMaxKeys, metadataMaxKeyLen, metadataMaxValueLen)),
	spec("stream_options", StagePreValidation, validStreamOptions()),
	// min_tokens strips when stop_token_ids is present (Pre), then clamps to max_tokens
	// once the output-token stage has resolved it (Post).
	{Name: "min_tokens", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: requireUint()},
		{Stage: StagePreValidation, Apply: stripWhenFieldPresent("stop_token_ids")},
		{Stage: StagePostLimits, Apply: clampUintToField("max_tokens")},
	}},
	// tools must precede tool_choice: it coerces/defaults tool_choice before
	// tool_choice's own shape check runs.
	spec("tools", StagePreValidation, validTools(SchemaBounds{
		MaxDepth:      toolsMaxDepth,
		MaxNodes:      toolsMaxNodes,
		MaxSizeBytes:  toolsMaxSizeBytes,
		MaxBranch:     toolsMaxBranch,
		MaxEnum:       toolsMaxEnum,
		MaxPatternLen: toolsMaxPatternLen,
	}, "auto")),
	spec("tool_choice", StagePreValidation, validToolChoice(toolChoiceMaxNameLen)),
	spec("response_format", StagePreValidation, validResponseFormat(SchemaBounds{
		MaxDepth:      responseFormatMaxDepth,
		MaxNodes:      responseFormatMaxNodes,
		MaxSizeBytes:  responseFormatMaxSizeBytes,
		MaxBranch:     responseFormatMaxBranch,
		MaxEnum:       responseFormatMaxEnum,
		MaxPatternLen: responseFormatMaxPatternLen,
	}, responseFormatMaxNameLen)),
	spec("structured_outputs", StagePreValidation, validStructuredOutputs(structuredOutputsBounds{
		SchemaBounds: SchemaBounds{
			MaxDepth:      structuredOutputsMaxDepth,
			MaxNodes:      structuredOutputsMaxNodes,
			MaxSizeBytes:  structuredOutputsMaxSizeBytes,
			MaxBranch:     structuredOutputsMaxBranch,
			MaxEnum:       structuredOutputsMaxEnum,
			MaxPatternLen: structuredOutputsMaxPatternLen,
		},
		MaxChoiceEntries:    structuredOutputsMaxChoiceEntries,
		MaxChoiceEntryLen:   structuredOutputsMaxChoiceEntryLen,
		MaxGrammarLen:       structuredOutputsMaxGrammarLen,
		MaxGrammarNesting:   structuredOutputsMaxGrammarNesting,
		MaxStructuralTagLen: structuredOutputsMaxStructuralTagLen,
	})),
	{Name: "reasoning", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: reasoningWrapper()},
	}},
	{Name: "reasoning_effort", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: reasoningEffortValidate()},
		{Stage: StagePreValidation, Apply: stripParameter()},
	}},
	{Name: "enable_thinking", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: enableThinking()},
	}},
	// thinking must run before chat_template_kwargs below: its mirror path writes into
	// chat_template_kwargs, and that object's own bounds must validate the merged result.
	{Name: "thinking", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: thinking()},
	}},
	{Name: "thinking_token_budget", Rules: []StagedRule{
		{Stage: StagePreValidation, Apply: thinkingTokenBudgetStrip()},
		{Stage: StagePostLimits, Apply: thinkingTokenBudgetResolve()},
	}},
	spec("safety_identifier", StagePreValidation, safetyIdentifier()),
	spec("reasoning_split", StagePreValidation, reasoningSplit()),
	spec("chat_template_kwargs", StagePreValidation, validChatTemplateKwargs(ObjectBounds{
		MaxDepth:     chatTemplateKwargsMaxDepth,
		MaxNodes:     chatTemplateKwargsMaxNodes,
		MaxSizeBytes: chatTemplateKwargsMaxSizeBytes,
	})),
}

// knownParameterSet is the whitelist, derived from parameterTable once at package init.
var knownParameterSet = buildKnownParameterSet()

func buildKnownParameterSet() map[string]struct{} {
	known := make(map[string]struct{}, len(parameterTable))
	for _, spec := range parameterTable {
		known[spec.Name] = struct{}{}
	}
	return known
}

// forcedParameterNames lists request fields a forceLiteral rule always overwrites; each must
// have a clientStrippedFields response-side counterpart.
var forcedParameterNames = []string{"logprobs", "top_logprobs", "return_token_ids"}

// unwrapExtraBody flattens an extra_body envelope into top-level fields before the whitelist
// runs; an existing top-level key wins on conflict, the envelope key is dropped either way.
func unwrapExtraBody(document *Document) {
	envelope, exists := document.Get("extra_body")
	if !exists {
		return
	}
	document.Delete("extra_body")
	inner, ok := envelope.(map[string]any)
	if !ok {
		return
	}
	for key, value := range inner {
		if key == "extra_body" || document.Has(key) {
			continue
		}
		document.Set(key, value)
	}
}

// rejectUnknownParameters is the whitelist gate: any key absent from parameterTable rejects
// the request. Unknown keys are sorted so the reported violation is deterministic.
func rejectUnknownParameters(document *Document) error {
	unknown := make([]string, 0)
	for key := range document.Raw() {
		if _, ok := knownParameterSet[key]; !ok {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	name := unknown[0]
	if name == "" {
		return Reject("request body contains a field with an empty name")
	}
	return Reject("%s", unsupportedParameterMessage(name))
}
