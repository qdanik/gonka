package accounting

// Thresholds are constants rather than configuration: a per-deployment one would let two gateways
// report the same host differently, and neverCritical is the ceiling of a finding that stops at
// warning, since no rate exceeds 1.
const (
	findingMinimumVolume = 20

	executionTimeoutWarning  = 0.01
	executionTimeoutCritical = 0.05
	refusalWarning           = 0.05
	refusalCritical          = 0.20
	unusedAnswerWarning      = 0.20
	gatewayThrottleWarning   = 0.10
	stateDivergedWarning     = 0.01
	chainMissWarning         = 0.01
	chainMissCritical        = 0.05
	chainInvalidWarning      = 0.01
	chainInvalidCritical     = 0.05
	undecidedTimeoutWarning  = 0.10
	undecidedTimeoutCritical = 0.50
	unknownReasonWarning     = 0.05
	slowReceiptWarning       = 0.10
	slowChunkWarning         = 0.10
	clockDriftWarning        = 0.05
	slowDecodeWarning        = 0.10
	decodedLogprobsWarning   = 0.001
	decodedLogprobsCritical  = 0.01
	neverCritical            = 2.0
)

const (
	TimeoutReasonLongResponse    = "long_response_after_content"
	TimeoutReasonPhaseAborted    = "phase_transition_aborted"
	TimeoutReasonCollectionError = "timeout_collection_error"
	TimeoutReasonNotApplied      = "timeout_not_applied"
	TimeoutReasonNoPoster        = "no_poster"
)

const (
	FindingExecutionTimeouts    = "execution_timeouts"
	FindingRefusals             = "refusals"
	FindingUnusedAnswers        = "answers_unused"
	FindingGatewayThrottled     = "throttled_by_gateway"
	FindingChainDisagreement    = "ledger_disagrees_with_chain"
	FindingLedgerOvercounted    = "ledger_overcounted"
	FindingChainMisses          = "chain_recorded_misses"
	FindingChainInvalid         = "chain_recorded_invalid"
	FindingUnresolvedChallenges = "challenges_unresolved"
	FindingUndecidedTimeouts    = "timeouts_undecided"
	FindingUnknownReasons       = "reasons_unknown"
	FindingStateDiverged        = "blocked_by_state_divergence"
	FindingDecodedLogprobs      = "logprobs_not_token_ids"
	FindingFailureTerminals     = "failure_terminals"
	FindingSlowReceipts         = "slow_receipts"
	FindingSlowChunks           = "slow_chunks"
	FindingClockDrift           = "clock_drift"
	FindingSlowDecode           = "slow_decode"
)

// findingCodes is every code this gateway can emit, pinned so a rename has to be deliberate.
var findingCodes = []string{
	FindingExecutionTimeouts, FindingRefusals, FindingUnusedAnswers, FindingGatewayThrottled,
	FindingStateDiverged, FindingChainMisses, FindingChainInvalid, FindingUnresolvedChallenges,
	FindingUndecidedTimeouts, FindingUnknownReasons, FindingChainDisagreement, FindingLedgerOvercounted,
	FindingFailureTerminals, FindingSlowReceipts, FindingSlowChunks, FindingClockDrift, FindingSlowDecode,
	FindingDecodedLogprobs,
}

type Severity string

const (
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Finding names a condition and the numbers it was flagged on, nothing more. What each code means and
// what to check lives in docs/accounting-findings.md, so an explanation is written once instead of
// crossing the network with every response.
type Finding struct {
	Code     string   `json:"code"`
	Severity Severity `json:"severity"`
	Part     uint64   `json:"part"`
	Whole    uint64   `json:"whole,omitempty"`
}

// Nonces that never reached the host are excluded from every rate: a burn is this gateway's own
// decision, and counting it against the host would report our throttling as its failure.
func findingsFor(record ParticipantRecord) []Finding {
	probes := countersWhere(record, servedNoUser)
	delivered := without(record.Dispositions[DispositionFinishedUsed]+
		record.Dispositions[DispositionFinishedUnused]+
		record.Dispositions[DispositionFinishedUsageUnknown], probes)
	refused := without(record.Dispositions[DispositionUnfinishedRefused],
		countersWhere(record, both(is(DispositionUnfinishedRefused), excused)))
	unfinished := without(record.Dispositions[DispositionUnfinishedExecution],
		countersWhere(record, both(is(DispositionUnfinishedExecution), excused)))
	reached := delivered + refused + unfinished

	deliveredNormally := countersWhere(record, both(outsidePoC, wasDelivered))
	acknowledgedNormally := countersWhere(record, both(outsidePoC, wasAcknowledged))

	findings := make([]Finding, 0, 4)
	add := func(finding Finding, flagged bool) {
		if flagged {
			findings = append(findings, finding)
		}
	}
	add(ratio(unfinished, reached, executionTimeoutWarning, executionTimeoutCritical,
		FindingExecutionTimeouts))
	add(ratio(refused, reached, refusalWarning, refusalCritical,
		FindingRefusals))
	add(ratio(without(record.Dispositions[DispositionFinishedUnused], probes), delivered,
		unusedAnswerWarning, neverCritical, FindingUnusedAnswers))
	add(ratio(ghostsBecause(record, "participant_throttled_no_send"), record.Assigned, gatewayThrottleWarning, neverCritical,
		FindingGatewayThrottled))
	add(ratio(ghostsBecause(record, "participant_state_diverged_no_send"), record.Assigned, stateDivergedWarning, neverCritical,
		FindingStateDiverged))
	add(ratio(uint64(record.ChainMissed), record.Assigned, chainMissWarning, chainMissCritical,
		FindingChainMisses))
	add(ratio(uint64(record.ChainInvalid), record.Assigned, chainInvalidWarning, chainInvalidCritical,
		FindingChainInvalid))
	add(ratio(record.UnresolvedChallenges, record.Assigned, chainInvalidWarning, chainInvalidCritical,
		FindingUnresolvedChallenges))
	add(ratio(undecidedTimeouts(record), timeoutRoundsVoted(record), undecidedTimeoutWarning, undecidedTimeoutCritical,
		FindingUndecidedTimeouts))
	add(ratio(record.UnknownReasonTotal, record.Assigned, unknownReasonWarning, neverCritical,
		FindingUnknownReasons))
	add(ratio(countersWhere(record, both(outsidePoC, receiptWasSlow)), acknowledgedNormally, slowReceiptWarning, neverCritical,
		FindingSlowReceipts))
	add(ratio(countersWhere(record, both(outsidePoC, chunkWasSlow)), deliveredNormally, slowChunkWarning, neverCritical,
		FindingSlowChunks))
	add(ratio(countersWhere(record, both(outsidePoC, decodeWasSlow)), deliveredNormally, slowDecodeWarning, neverCritical,
		FindingSlowDecode))
	add(ratio(countersWhere(record, logprobsWereDecoded), delivered, decodedLogprobsWarning, decodedLogprobsCritical,
		FindingDecodedLogprobs))
	add(ratio(countersWhere(record, clockHadDrifted), delivered+record.Dispositions[DispositionUnfinishedExecution], clockDriftWarning, neverCritical,
		FindingClockDrift))

	if total := countersWhere(record, failedWithoutAnswer); total > 0 && reached >= findingMinimumVolume {
		findings = append(findings, Finding{
			Code: FindingFailureTerminals, Severity: SeverityWarning, Part: total, Whole: reached,
		})
	}
	if record.Overcounted > 0 {
		findings = append(findings, Finding{
			Code: FindingLedgerOvercounted, Severity: SeverityWarning,
			Part: record.Overcounted, Whole: record.Assigned,
		})
	}
	if drift := record.CrossChecks.ErrorCount - record.Overcounted; drift > 0 && record.Assigned >= findingMinimumVolume {
		findings = append(findings, Finding{
			Code: FindingChainDisagreement, Severity: SeverityWarning,
			Part: drift, Whole: record.Assigned,
		})
	}
	return findings
}

func undecidedTimeouts(record ParticipantRecord) uint64 {
	return record.TimeoutOutcomes[TimeoutVoteCollectionFailed] + record.TimeoutOutcomes[TimeoutInsufficientVotes]
}

// A skipped round never asked anyone, so it cannot be undecided.
func timeoutRoundsVoted(record ParticipantRecord) uint64 {
	var total uint64
	for outcome, count := range record.TimeoutOutcomes {
		if outcome != TimeoutSkipped {
			total += count
		}
	}
	return total
}

// ratio takes the denominator once, so the rate that decides and the numbers reported beside it cannot
// disagree about what they were measured against.
func ratio(part, whole uint64, warning, critical float64, code string) (Finding, bool) {
	severity, flagged := rate(part, whole, warning, critical)
	if !flagged {
		return Finding{}, false
	}
	return Finding{Code: code, Severity: severity, Part: part, Whole: whole}, true
}

func rate(part, whole uint64, warning, critical float64) (Severity, bool) {
	if whole < findingMinimumVolume || part == 0 {
		return "", false
	}
	measured := float64(part) / float64(whole)
	switch {
	case measured >= critical:
		return SeverityCritical, true
	case measured >= warning:
		return SeverityWarning, true
	}
	return "", false
}

// failedWithoutAnswer is the pair of dispositions that reached the host and produced nothing; which
// terminal it was is already in the counter beside it.
func failedWithoutAnswer(key CounterKey) bool {
	return key.Disposition == DispositionUnfinishedRefused || key.Disposition == DispositionUnfinishedExecution
}

func receiptWasSlow(key CounterKey) bool  { return key.SlowReceipt }
func chunkWasSlow(key CounterKey) bool    { return key.SlowChunk }
func clockHadDrifted(key CounterKey) bool { return key.ClockDrifted }
func decodeWasSlow(key CounterKey) bool   { return key.SlowDecode }

func logprobsWereDecoded(key CounterKey) bool { return key.LogprobsDecoded }

func countersWhere(record ParticipantRecord, match func(CounterKey) bool) uint64 {
	var total uint64
	for _, counter := range record.Counters {
		if match(counter.CounterKey) {
			total += counter.Count
		}
	}
	return total
}

func is(disposition Disposition) func(CounterKey) bool {
	return func(key CounterKey) bool { return key.Disposition == disposition }
}

func both(first, second func(CounterKey) bool) func(CounterKey) bool {
	return func(key CounterKey) bool { return first(key) && second(key) }
}

func outsidePoC(key CounterKey) bool { return key.Phase != PhasePoC }

func wasDelivered(key CounterKey) bool {
	return key.Disposition == DispositionFinishedUsed ||
		key.Disposition == DispositionFinishedUnused ||
		key.Disposition == DispositionFinishedUsageUnknown
}

func wasAcknowledged(key CounterKey) bool {
	return wasDelivered(key) || key.Disposition == DispositionUnfinishedExecution
}

func servedNoUser(key CounterKey) bool { return key.Terminal == TerminalWarmupProbe }

// excused names a failure the host did not cause: the client stopped waiting, or this gateway's own
// policy ended the attempt. Slowness that drove a client away is measured on its own, by the receipt,
// chunk and decode findings, so excusing the abort here does not hide a slow host.
func excused(key CounterKey) bool {
	if key.Terminal == TerminalClientCancelled {
		return true
	}
	switch key.TimeoutReason {
	case TimeoutReasonLongResponse, TimeoutReasonPhaseAborted,
		TimeoutReasonCollectionError, TimeoutReasonNotApplied, TimeoutReasonNoPoster:
		return true
	}
	return false
}

func without(total, part uint64) uint64 {
	if part > total {
		return 0
	}
	return total - part
}

func ghostsBecause(record ParticipantRecord, reason string) uint64 {
	var total uint64
	for _, counter := range record.Counters {
		if counter.Disposition == DispositionGhost && counter.GhostReason == reason {
			total += counter.Count
		}
	}
	return total
}
