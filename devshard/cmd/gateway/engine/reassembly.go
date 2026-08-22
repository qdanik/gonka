package engine

// sseClassifier joins one attempt's bounded reassembly to the SSE verdicts. See
// gateway-speculative-race.md, "Classification and reassembly".
type sseClassifier struct {
	thinkingBudget bool
	carry          *carryBuffer
	overflow       func()
}

func newSSEClassifier(budget *carryBudget, participant, model string, overflow func()) *sseClassifier {
	return &sseClassifier{
		thinkingBudget: thinkingBudgetRoute(model),
		carry:          newCarryBuffer(budget, participant),
		overflow:       overflow,
	}
}

func (c *sseClassifier) Classify(chunk []byte) chunkFacts {
	events, firstDrop := c.carry.Take(chunk)
	if firstDrop && c.overflow != nil {
		c.overflow()
	}
	return c.facts(classifyChunk(events, c.thinkingBudget))
}

// Flush classifies the unterminated final event, which reads as nothing at all until it does.
func (c *sseClassifier) Flush() chunkFacts {
	tail := c.carry.Tail()
	return c.facts(classifyChunk(tail, c.thinkingBudget))
}

func (c *sseClassifier) Release() { c.carry.Release() }

// facts keeps a capability refusal out of Error, while its message still reaches the perf recorder. See
// gateway-speculative-race.md, "An SSE error event counts as a chunk but never crowns".
func (c *sseClassifier) facts(signal chunkSignal) chunkFacts {
	facts := chunkFacts{
		Content:               signal.crownsWinner(),
		ContentSource:         signal.ContentSource,
		UsageCompletionTokens: signal.UsageCompletionTokens,
		TokensBurned:          signal.UsageCompletionTokens > 0 && c.thinkingBudget,
		LogprobsDecoded:       signal.LogprobsDecoded,
	}
	if !signal.Error.present() {
		return facts
	}
	capability := ParseCapabilityError(signal.Error.Message)
	facts.Error = !capability.Retriable()
	facts.CapabilityRefused = capability.Retriable()
	facts.Capability = capability
	facts.ErrorSource = signal.Error.Source
	facts.ErrorCode = signal.Error.Code
	facts.ErrorType = signal.Error.Type
	facts.ErrorMessage = signal.Error.Message
	facts.ErrorPayload = signal.Error.Payload
	return facts
}
