package filters

import (
	"fmt"
	"strings"
)

const (
	// Message role wire values; the only roles validateMessages accepts.
	roleDeveloper = "developer"
	roleSystem    = "system"
	roleUser      = "user"
	roleAssistant = "assistant"
	roleTool      = "tool"
	roleFunction  = "function"

	// emptyToolResultContent fills an empty role:"tool" message -- vLLM chat templates require text in every tool turn.
	emptyToolResultContent = "<empty tool result>"
)

var (
	// disallowedAgenticFields are fields a plain-text role (no tool/function agency) must never carry.
	disallowedAgenticFields = []string{"tool_calls", "tool_call_id", "function_call"}

	// messageRolePolicies is keyed by wire role value; a role missing from this map is rejected.
	messageRolePolicies = map[string]messageRolePolicy{
		roleDeveloper: {disallowedFields: disallowedAgenticFields},
		roleSystem:    {disallowedFields: disallowedAgenticFields},
		roleUser:      {disallowedFields: disallowedAgenticFields},
		roleAssistant: {disallowedFields: []string{"tool_call_id"}},
		roleTool:      {disallowedFields: []string{"tool_calls", "function_call"}, requireToolCallID: true},
		roleFunction:  {disallowedFields: disallowedAgenticFields, requireName: true},
	}

	// messageNormalizerChain is fixed and order-sensitive. See README.md, "Message hygiene".
	messageNormalizerChain = []messageNormalizer{
		dropOrphanToolMessages,
		dropEmptyAssistantTurns,
		normalizeEmptyMessageContent,
		stripLegacyToolName,
		flattenMessageTextParts,
	}
)

type messageRolePolicy struct {
	disallowedFields  []string
	requireName       bool
	requireToolCallID bool
}

// messageNormalizer rewrites the messages array before validateMessages, reporting whether anything changed.
type messageNormalizer func(messages []any) ([]any, bool, error)

func normalizeMessages(document *Document) error {
	messages, ok := document.Array("messages")
	if !ok {
		return nil
	}
	changed := false
	for _, normalize := range messageNormalizerChain {
		rewritten, wasChanged, err := normalize(messages)
		if err != nil {
			return WrapReject(err)
		}
		if wasChanged {
			messages = rewritten
			changed = true
		}
	}
	if changed {
		document.Set("messages", messages)
	}
	return nil
}

// dropOrphanToolMessages removes role:"tool" entries whose tool_call_id has no matching prior assistant tool_call.
func dropOrphanToolMessages(messages []any) ([]any, bool, error) {
	pending := map[string]struct{}{}
	filtered := make([]any, 0, len(messages))
	dropped := false
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			filtered = append(filtered, raw)
			continue
		}
		role, _ := message["role"].(string)
		switch role {
		case roleAssistant:
			if calls, ok := message["tool_calls"].([]any); ok {
				for _, rawCall := range calls {
					call, ok := rawCall.(map[string]any)
					if !ok {
						continue
					}
					if id, ok := call["id"].(string); ok && id != "" {
						pending[id] = struct{}{}
					}
				}
			}
		case roleTool:
			if id, ok := message["tool_call_id"].(string); ok && id != "" {
				if _, matched := pending[id]; !matched {
					dropped = true
					continue
				}
				delete(pending, id)
			}
		}
		filtered = append(filtered, raw)
	}
	return filtered, dropped, nil
}

// dropEmptyAssistantTurns removes assistant messages with no content and no call -- placeholders some clients resend.
func dropEmptyAssistantTurns(messages []any) ([]any, bool, error) {
	filtered := make([]any, 0, len(messages))
	dropped := false
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			filtered = append(filtered, raw)
			continue
		}
		if role, _ := message["role"].(string); role == roleAssistant && isAssistantTurnEmpty(message) {
			dropped = true
			continue
		}
		filtered = append(filtered, raw)
	}
	return filtered, dropped, nil
}

// normalizeEmptyMessageContent fills empty tool content with the sentinel, and nullifies empty assistant content carrying a call.
func normalizeEmptyMessageContent(messages []any) ([]any, bool, error) {
	changed := false
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		role, _ := message["role"].(string)
		content, exists := message["content"]
		switch {
		case !exists, content == nil:
			if role == roleTool {
				message["content"] = emptyToolResultContent
				changed = true
			}
		case isEmptyContent(content):
			switch role {
			case roleAssistant:
				_, hasToolCalls := message["tool_calls"]
				_, hasFunctionCall := message["function_call"]
				if hasToolCalls || hasFunctionCall {
					message["content"] = nil
					changed = true
				}
			case roleTool:
				message["content"] = emptyToolResultContent
				changed = true
			}
		}
	}
	return messages, changed, nil
}

// stripLegacyToolName drops the `name` field from role:"tool" messages, left over from the retired role:"function".
func stripLegacyToolName(messages []any) ([]any, bool, error) {
	changed := false
	for _, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if role, _ := message["role"].(string); role != roleTool {
			continue
		}
		if _, exists := message["name"]; !exists {
			continue
		}
		delete(message, "name")
		changed = true
	}
	return messages, changed, nil
}

// flattenMessageTextParts joins {type:"text",text} parts into one string; other shapes are left for validateMessages.
func flattenMessageTextParts(messages []any) ([]any, bool, error) {
	changed := false
	for index, raw := range messages {
		message, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		content, exists := message["content"]
		if !exists || content == nil {
			continue
		}
		parts, ok := content.([]any)
		if !ok {
			continue
		}
		combined, err := combineTextContentParts(parts)
		if err != nil {
			return nil, false, fmt.Errorf("messages[%d].content%w", index, err)
		}
		if combined != "" {
			message["content"] = combined
			changed = true
		}
	}
	return messages, changed, nil
}

func validateMessages(document *Document) error {
	rawMessages, exists := document.Array("messages")
	if !exists {
		return Reject("messages is required")
	}
	if len(rawMessages) == 0 {
		return Reject("messages must not be empty")
	}
	pendingToolCalls := map[string]struct{}{}
	for index, rawMessage := range rawMessages {
		message, ok := rawMessage.(map[string]any)
		if !ok {
			return Reject("messages[%d] must be an object", index)
		}
		role, err := requiredNonEmptyStringField(message, "role")
		if err != nil {
			return Reject("messages[%d].role: %v", index, err)
		}
		policy, known := messageRolePolicies[role]
		if !known {
			return Reject("messages[%d].role has unsupported value %q", index, role)
		}
		if err := ensureFieldsAbsent(message, policy.disallowedFields...); err != nil {
			return Reject("messages[%d]: %v", index, err)
		}
		if err := validateMessageRoleFields(message, index, role, policy, pendingToolCalls); err != nil {
			return err
		}
	}
	return nil
}

// validateMessageRoleFields checks one role's own fields; content is required unless assistant carries a call.
func validateMessageRoleFields(message map[string]any, index int, role string, policy messageRolePolicy, pendingToolCalls map[string]struct{}) error {
	switch role {
	case roleDeveloper, roleSystem, roleUser:
		if err := validateRequiredContentField(message); err != nil {
			return Reject("messages[%d].content: %v", index, err)
		}
	case roleAssistant:
		toolCallIDs, hasToolCalls, err := validateToolCallsField(message)
		if err != nil {
			return Reject("messages[%d].%v", index, err)
		}
		hasFunctionCall, err := validateFunctionCallField(message)
		if err != nil {
			return Reject("messages[%d].%v", index, err)
		}
		if err := validateAssistantContentField(message, hasToolCalls || hasFunctionCall); err != nil {
			return Reject("messages[%d].content: %v", index, err)
		}
		for _, id := range toolCallIDs {
			pendingToolCalls[id] = struct{}{}
		}
	case roleTool:
		if policy.requireToolCallID {
			toolCallID, err := requiredNonEmptyStringField(message, "tool_call_id")
			if err != nil {
				return Reject("messages[%d].tool_call_id: %v", index, err)
			}
			if _, pending := pendingToolCalls[toolCallID]; !pending {
				return Reject("messages[%d].tool_call_id does not match any previous assistant tool_calls", index)
			}
			delete(pendingToolCalls, toolCallID)
		}
		if err := validateRequiredContentField(message); err != nil {
			return Reject("messages[%d].content: %v", index, err)
		}
	case roleFunction:
		if policy.requireName {
			if _, err := requiredNonEmptyStringField(message, "name"); err != nil {
				return Reject("messages[%d].name: %v", index, err)
			}
		}
		if err := validateRequiredContentField(message); err != nil {
			return Reject("messages[%d].content: %v", index, err)
		}
	}
	return nil
}

// requiredNonEmptyStringField returns the trimmed-nonblank string, or why it isn't; the caller adds the positional prefix.
func requiredNonEmptyStringField(fields map[string]any, key string) (string, error) {
	rawValue, exists := fields[key]
	if !exists || rawValue == nil {
		return "", fmt.Errorf("is required")
	}
	value, ok := rawValue.(string)
	if !ok {
		return "", fmt.Errorf("must be a string")
	}
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("must not be empty")
	}
	return value, nil
}

// optionalStringField rejects fields[key] only when present, non-null, and not a string.
func optionalStringField(fields map[string]any, key string) error {
	rawValue, exists := fields[key]
	if !exists || rawValue == nil {
		return nil
	}
	if _, ok := rawValue.(string); !ok {
		return fmt.Errorf("must be a string")
	}
	return nil
}

// ensureFieldsAbsent rejects the first disallowed field present; an explicit null still counts as present.
func ensureFieldsAbsent(fields map[string]any, disallowed ...string) error {
	for _, key := range disallowed {
		if _, exists := fields[key]; exists {
			return fmt.Errorf("%s is not allowed for this role", key)
		}
	}
	return nil
}

// validateNonEmptyContent accepts a non-blank string, or a non-empty array of {type:"text",text} parts.
func validateNonEmptyContent(content any) error {
	switch value := content.(type) {
	case string:
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("must not be empty")
		}
		return nil
	case []any:
		if len(value) == 0 {
			return fmt.Errorf("must not be empty")
		}
		for index, rawPart := range value {
			part, ok := rawPart.(map[string]any)
			if !ok {
				return fmt.Errorf("[%d] must be an object", index)
			}
			text, err := requiredTextContentPart(part, index)
			if err != nil {
				return err
			}
			if strings.TrimSpace(text) == "" {
				return fmt.Errorf("[%d].text must not be empty", index)
			}
		}
		return nil
	default:
		return fmt.Errorf("must be a string or an array of typed content parts")
	}
}

func validateRequiredContentField(message map[string]any) error {
	content, exists := message["content"]
	if !exists || content == nil {
		return fmt.Errorf("is required")
	}
	return validateNonEmptyContent(content)
}

// validateAssistantContentField allows a missing content when canBeEmpty; a present field is always shape-checked.
func validateAssistantContentField(message map[string]any, canBeEmpty bool) error {
	content, exists := message["content"]
	if !exists || content == nil {
		if canBeEmpty {
			return nil
		}
		return fmt.Errorf("is required unless tool_calls or function_call is provided")
	}
	return validateNonEmptyContent(content)
}

func requiredTextContentPart(part map[string]any, partIndex int) (string, error) {
	partType, err := requiredNonEmptyStringField(part, "type")
	if err != nil {
		return "", fmt.Errorf("[%d].type: %w", partIndex, err)
	}
	if partType != "text" {
		return "", fmt.Errorf("[%d].type has unsupported value %q", partIndex, partType)
	}
	text, err := requiredNonEmptyStringField(part, "text")
	if err != nil {
		return "", fmt.Errorf("[%d].text: %w", partIndex, err)
	}
	return text, nil
}

func combineTextContentParts(parts []any) (string, error) {
	texts := make([]string, 0, len(parts))
	for partIndex, rawPart := range parts {
		part, ok := rawPart.(map[string]any)
		if !ok {
			return "", fmt.Errorf("[%d] must be an object", partIndex)
		}
		text, err := requiredTextContentPart(part, partIndex)
		if err != nil {
			return "", err
		}
		texts = append(texts, text)
	}
	if len(texts) == 0 {
		return "", nil
	}
	return strings.Join(texts, "\n"), nil
}

// isEmptyContent reports a blank string or an empty parts array; nil means "missing" rather than "empty".
func isEmptyContent(content any) bool {
	switch value := content.(type) {
	case string:
		return strings.TrimSpace(value) == ""
	case []any:
		return len(value) == 0
	default:
		return false
	}
}

func isAssistantTurnEmpty(message map[string]any) bool {
	if raw, exists := message["tool_calls"]; exists && raw != nil {
		if calls, ok := raw.([]any); ok && len(calls) > 0 {
			return false
		}
	}
	if raw, exists := message["function_call"]; exists && raw != nil {
		if functionCall, ok := raw.(map[string]any); ok && len(functionCall) > 0 {
			return false
		}
	}
	content, exists := message["content"]
	if !exists || content == nil {
		return true
	}
	return isEmptyContent(content)
}

// validateToolCallsField validates tool_calls and returns its ids; a null is removed as absent (some SDKs serialize empty slots so).
func validateToolCallsField(message map[string]any) ([]string, bool, error) {
	rawToolCalls, exists := message["tool_calls"]
	if !exists {
		return nil, false, nil
	}
	if rawToolCalls == nil {
		delete(message, "tool_calls")
		return nil, false, nil
	}
	toolCalls, ok := rawToolCalls.([]any)
	if !ok {
		return nil, true, fmt.Errorf("tool_calls must be an array")
	}
	if len(toolCalls) == 0 {
		return nil, true, fmt.Errorf("tool_calls must not be empty")
	}
	seen := map[string]struct{}{}
	ids := make([]string, 0, len(toolCalls))
	for callIndex, rawCall := range toolCalls {
		call, ok := rawCall.(map[string]any)
		if !ok {
			return nil, true, fmt.Errorf("tool_calls[%d] must be an object", callIndex)
		}
		id, err := requiredNonEmptyStringField(call, "id")
		if err != nil {
			return nil, true, fmt.Errorf("tool_calls[%d].id: %w", callIndex, err)
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, true, fmt.Errorf("tool_calls[%d].id is duplicated", callIndex)
		}
		seen[id] = struct{}{}
		callType, err := requiredNonEmptyStringField(call, "type")
		if err != nil {
			return nil, true, fmt.Errorf("tool_calls[%d].type: %w", callIndex, err)
		}
		if callType != "function" {
			return nil, true, fmt.Errorf("tool_calls[%d].type must be \"function\"", callIndex)
		}
		function, ok := call["function"].(map[string]any)
		if !ok {
			return nil, true, fmt.Errorf("tool_calls[%d].function must be an object", callIndex)
		}
		if _, err := requiredNonEmptyStringField(function, "name"); err != nil {
			return nil, true, fmt.Errorf("tool_calls[%d].function.name: %w", callIndex, err)
		}
		if err := optionalStringField(function, "arguments"); err != nil {
			return nil, true, fmt.Errorf("tool_calls[%d].function.arguments: %w", callIndex, err)
		}
		ids = append(ids, id)
	}
	return ids, true, nil
}

// validateFunctionCallField validates the legacy function_call; a null value is removed as absent.
func validateFunctionCallField(message map[string]any) (bool, error) {
	rawFunctionCall, exists := message["function_call"]
	if !exists {
		return false, nil
	}
	if rawFunctionCall == nil {
		delete(message, "function_call")
		return false, nil
	}
	functionCall, ok := rawFunctionCall.(map[string]any)
	if !ok {
		return true, fmt.Errorf("function_call must be an object")
	}
	if _, err := requiredNonEmptyStringField(functionCall, "name"); err != nil {
		return true, fmt.Errorf("function_call.name: %w", err)
	}
	if err := optionalStringField(functionCall, "arguments"); err != nil {
		return true, fmt.Errorf("function_call.arguments: %w", err)
	}
	return true, nil
}
