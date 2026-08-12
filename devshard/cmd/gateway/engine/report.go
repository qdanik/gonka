package engine

import (
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

func (c *raceCoordinator) apply(event AttemptEvent) {
	attempt := c.byNonce[event.Nonce]
	if attempt == nil {
		return
	}
	switch event.Kind {
	case AttemptDispatched:
		attempt.sendTime = event.At
	case AttemptReceipt:
		attempt.receiptTime = event.At
	case AttemptFirstToken:
		attempt.firstToken, attempt.lastChunk = event.At, event.At
	case AttemptContent:
		attempt.firstContent, attempt.lastChunk = event.At, event.At
	case AttemptChunk:
		attempt.lastChunk = event.At
	case AttemptDone:
		c.complete(attempt, event)
	}
}

// attemptDeliveryFields reads the same for a winner and a loser; every duration is from the dispatch.
func attemptDeliveryFields(outcome AttemptOutcome) []any {
	fields := []any{
		logkey.ContentChunks, outcome.ContentChunks,
		logkey.StreamChunks, outcome.StreamChunks,
		logkey.OutputBytes, outcome.OutputBytes,
		logkey.UsageTokens, outcome.UsageCompletionTokens,
	}
	if outcome.MaxChunkGap > 0 {
		fields = append(fields,
			logkey.MaxGapMS, outcome.MaxChunkGap.Milliseconds(),
			logkey.MaxGapAtChunk, outcome.MaxChunkGapAt,
			logkey.MeanGapMS, outcome.MeanChunkGap.Milliseconds())
	}
	for _, span := range []struct {
		name  string
		until time.Time
	}{
		{"receipt_ms", outcome.ReceiptTime},
		{"first_token_ms", outcome.FirstToken},
		{"first_content_ms", outcome.FirstContent},
		{"attempt_ms", outcome.Completed},
	} {
		if outcome.SendTime.IsZero() || span.until.IsZero() {
			continue
		}
		fields = append(fields, span.name, span.until.Sub(outcome.SendTime).Milliseconds())
	}
	return fields
}
func (c *raceCoordinator) complete(attempt *liveAttempt, event AttemptEvent) {
	attempt.done, attempt.completed = true, event.At
	attempt.outcome, attempt.lifecycle = event.Outcome, event.Lifecycle
	attempt.nonceFinished = c.target.NonceFinished(attempt.nonce)
	c.retire(attempt)

	if attempt.outcome == nil {
		logging.Info("attempt finished with no outcome",
			logkey.Request, c.request.RequestID, logkey.Escrow, c.escrowID, logkey.Nonce, attempt.nonce,
			logkey.Participant, attempt.participant, logkey.NonceFinished, attempt.nonceFinished)
		return
	}
	// The phase abort is the difference between a host that answered nothing and one the PoC transition
	// cut off, and every other reader already tells them apart. See gateway-capacity-and-health.md.
	fields := []any{
		logkey.Request, c.request.RequestID, logkey.Escrow, c.escrowID, logkey.Nonce, attempt.nonce,
		logkey.Participant, attempt.participant,
		logkey.Terminal, c.racedTerminal(attempt, *attempt.outcome).String(),
		logkey.NonceFinished, attempt.nonceFinished, logkey.StateDivergent, attempt.outcome.StateDivergent,
	}
	fields = append(fields, attemptDeliveryFields(*attempt.outcome)...)
	if c.phaseAborted(attempt, *attempt.outcome) {
		fields = append(fields, logkey.PhaseAborted, true)
	}
	logging.Info("attempt finished", fields...)
	if signal := CapabilityOf(*attempt.outcome); signal.Retriable() {
		RecordCapability(c.deps.Perf, attempt.participant, signal)
		c.contextHint = GrowContextHint(c.contextHint, signal)
		c.exclude(attempt.participant)
	}
	if attempt.outcome.StateDivergent {
		logging.Warn("host blocked for state divergence",
			logkey.Request, c.request.RequestID, logkey.Escrow, c.escrowID,
			logkey.Nonce, attempt.nonce, logkey.Participant, attempt.participant)
		c.deps.Picker.BlockHost(c.escrowID, attempt.participant)
		c.exclude(attempt.participant)
	}
}

// unreportedOutcome stands in for an attempt whose goroutine never reported: the race stopped listening
// before it finished. Dropping it left a committed nonce with no log line, no ledger row and no timeout
// vote -- a nonce spent on chain that nothing downstream could see. See gateway-invariants.md, "1".
func (c *raceCoordinator) unreportedOutcome(attempt *liveAttempt) AttemptOutcome {
	hostLabel := ""
	if c.target != nil {
		hostLabel = c.target.HostLabel(attempt.hostIdx)
	}
	return AttemptOutcome{
		Nonce:       attempt.nonce,
		Participant: attempt.participant,
		HostIdx:     attempt.hostIdx,
		HostLabel:   hostLabel,
		SendTime:    attempt.sendTime,
		ReceiptTime: attempt.receiptTime,
		Terminal:    TerminalUnclassified,
	}
}

// racedTerminal is the attempt's terminal as the race sees it: an attempt goroutine knows only its own
// cancellation, not the crown, the silence or the backstop. Every reader promotes through here.
func (c *raceCoordinator) racedTerminal(attempt *liveAttempt, outcome AttemptOutcome) Terminal {
	terminal := outcome.Terminal
	if terminal == TerminalClientCancelled && attempt.backstopped {
		terminal = TerminalHardTimeout
	}
	if terminal == TerminalClientCancelled && (attempt.stalled && outcome.ContentChunks > 0 || c.abandonedByHosts()) {
		terminal = TerminalStalled
	}
	if attempt == c.winner && terminal == TerminalLost {
		return TerminalWon
	}
	return terminal
}
func (c *raceCoordinator) retire(attempt *liveAttempt) {
	c.pending--
	attempt.cancel()
	c.deps.Perf.Release(attempt.participant)
}

// report closes the byte path's escape hatch and hands the race's one outcome to its consumers.
func (c *raceCoordinator) report() RaceOutcome {
	close(c.done)
	outcome := c.outcome()
	if c.deps.Crown != nil {
		for _, attempt := range outcome.Attempts {
			c.deps.Crown.Observe(attempt.Participant, outcome.Model, outcome.DeniesCrowning(attempt))
		}
	}
	if c.deps.Report != nil {
		c.deps.Report(outcome)
	}
	return outcome
}
func (c *raceCoordinator) outcome() RaceOutcome {
	outcome := RaceOutcome{
		RequestID:       c.request.RequestID,
		EscrowID:        c.escrowID,
		Model:           c.request.Model,
		InputTokens:     c.request.InputTokens,
		ClientStream:    c.request.ClientStream,
		Decision:        c.decision,
		PoCBypassActive: c.pocBypass,
		Lifecycle:       Lifecycle{BalanceExhausted: c.balanceExhausted, ClientGone: !c.clientGoneAt.IsZero()},
		Attempts:        make([]AttemptOutcome, 0, len(c.attempts)),
	}
	for _, attempt := range c.attempts {
		record := c.unreportedOutcome(attempt)
		if attempt.outcome != nil {
			record = *attempt.outcome
		}
		record.StartedAt = c.started
		record.NonceFinished = attempt.nonceFinished
		record.FailureRateExceeded = c.deps.Perf.Ejected(attempt.participant, c.request.Model)
		record.PhaseTransitionAborted = c.phaseAborted(attempt, record)
		record.Terminal = c.racedTerminal(attempt, record)
		if attempt == c.winner {
			outcome.WinnerNonce = attempt.nonce
			outcome.Succeeded = record.Terminal == TerminalWon
		}
		outcome.Lifecycle.EscrowMissing = outcome.Lifecycle.EscrowMissing || attempt.lifecycle.EscrowMissing
		outcome.Lifecycle.BalanceExhausted = outcome.Lifecycle.BalanceExhausted || attempt.lifecycle.BalanceExhausted
		outcome.Attempts = append(outcome.Attempts, record)
	}
	return outcome
}

// pocBypassActive reports the gateway serving through a chain phase that refuses new inferences.
func pocBypassActive(snapshot chain.PhaseSnapshot, modes config.Modes) bool {
	return modes.PoCMode == config.PoCModeRelaxed && snapshot.RequestsBlocked
}

// pocGenerating is the narrower phase that takes a host away from inference mid-attempt. Only the
// bypass has an attempt running inside it, so strict mode never reports it.
func pocGenerating(snapshot chain.PhaseSnapshot, modes config.Modes) bool {
	if modes.PoCMode != config.PoCModeRelaxed {
		return false
	}
	return snapshot.EpochPhase == chain.EpochPhasePoCGenerate ||
		snapshot.ConfirmationPoCPhase == chain.ConfirmationPoCGeneration
}

// phaseAborted asks the shared rule with the phase the coordinator sees, so the log line and the
// ladders cannot disagree about who ended the attempt.
func (c *raceCoordinator) phaseAborted(attempt *liveAttempt, outcome AttemptOutcome) bool {
	return phaseAborted(outcome, attempt.inInference, pocGenerating(c.deps.Snapshots.Snapshot(), c.deps.Modes))
}

// phaseAborted reports an attempt the phase transition ended rather than the host. A no-receipt
// attempt never reached the host's queue, so the transition cannot be what ended it.
func phaseAborted(a AttemptOutcome, startedInInference, pocGenerating bool) bool {
	if !startedInInference || !pocGenerating {
		return false
	}
	return !a.NonceFinished && a.ErrorSource == "" && a.Terminal != TerminalNoReceipt
}
