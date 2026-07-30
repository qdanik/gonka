package engine

// sseClassifier joins one attempt's bounded reassembly to the SSE verdicts, so an event split across
// chunk boundaries is classified once, when its final line arrives.
type sseClassifier struct {
	model    string
	carry    *carryBuffer
	overflow func()
}

func newSSEClassifier(budget *carryBudget, participant, model string, overflow func()) *sseClassifier {
	return &sseClassifier{
		model:    model,
		carry:    newCarryBuffer(budget, participant),
		overflow: overflow,
	}
}

func (c *sseClassifier) Classify(chunk []byte) chunkFacts {
	events, firstDrop := c.carry.Take(chunk)
	if firstDrop && c.overflow != nil {
		c.overflow()
	}
	return c.facts(classifyChunk(events))
}

// Flush classifies the unterminated final event, which reads as nothing at all until it does.
func (c *sseClassifier) Flush() chunkFacts { return c.facts(classifyChunk(c.carry.Tail())) }

func (c *sseClassifier) Release() { c.carry.Release() }

// facts keeps a capability refusal out of Error: another host can still serve it, so it must neither
// count as a chunk nor end the race, while its message still reaches the perf recorder.
func (c *sseClassifier) facts(signal chunkSignal) chunkFacts {
	facts := chunkFacts{
		Content:               signal.crownsWinner(),
		ContentSource:         signal.ContentSource,
		UsageCompletionTokens: signal.UsageCompletionTokens,
		TokensBurned:          signal.UsageCompletionTokens > 0 && thinkingBudgetRoute(c.model),
	}
	if !signal.Error.present() {
		return facts
	}
	capability := ParseCapabilityError(signal.Error.Message)
	facts.Error = !capability.Retriable()
	facts.CapabilityRefused = capability.Retriable()
	facts.ErrorSource = signal.Error.Source
	facts.ErrorCode = signal.Error.Code
	facts.ErrorType = signal.Error.Type
	facts.ErrorMessage = signal.Error.Message
	facts.ErrorPayload = signal.Error.Payload
	facts.ContextLimit = capability.ContextLimit
	facts.ToolsUnsupported = capability.ToolsUnsupported
	return facts
}
