package accounting

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// recordWith builds the totals a finding reads, so a test states the shape of a participant's epoch
// rather than the sequence of facts that would produce it.
func recordWith(assigned uint64, dispositions map[Disposition]uint64) ParticipantRecord {
	return ParticipantRecord{Assigned: assigned, Dispositions: dispositions}
}

// troubledBook drives a book through the facts a struggling host produces, so the tests above the
// derivation are joined by one that proves the derivation is reached at all. A group of one puts
// every nonce on the same participant.
func troubledBook(t *testing.T) *Book {
	t.Helper()
	const total, unfinished = 100, 10
	book := newTestBook(t, 1)
	// Nonce numbering starts at one, as the chain counts it: starting at zero would classify a nonce
	// outside the assigned range and raise a disagreement finding this fixture does not mean to raise.
	if err := book.ObserveLatestNonce(testEscrow, total); err != nil {
		t.Fatalf("ObserveLatestNonce(): %v", err)
	}
	attempts := make([]Attempt, 0, total)
	for nonce := uint64(1); nonce <= total; nonce++ {
		attempts = append(attempts, Attempt{
			Nonce: nonce, Sent: true, Acknowledged: true,
			Finished: nonce > unfinished, Usage: UsageWinner,
		})
	}
	if err := book.RecordRace(testEscrow, attempts); err != nil {
		t.Fatalf("RecordRace(): %v", err)
	}
	for nonce := uint64(1); nonce <= unfinished; nonce++ {
		if err := book.RecordTimeout(testEscrow, nonce, "execution", "completed", "none"); err != nil {
			t.Fatalf("RecordTimeout(%d): %v", nonce, err)
		}
	}
	return book
}

// TestQueryAttachesFindingsToTheRecord is the join: the derivation is tested above on totals alone,
// and this is what proves a reader of the ledger ever reaches it.
func TestQueryAttachesFindingsToTheRecord(t *testing.T) {
	records := troubledBook(t).Query(QueryFilter{})

	if len(records) != 1 {
		t.Fatalf("got %d records, want the single participant of a group of one", len(records))
	}
	findingWithCode(t, records[0].Findings, FindingExecutionTimeouts)
}

func codesOf(findings []Finding) []string {
	codes := make([]string, 0, len(findings))
	for _, finding := range findings {
		codes = append(codes, finding.Code)
	}
	return codes
}

func findingWithCode(t *testing.T, findings []Finding, code string) Finding {
	t.Helper()
	for _, finding := range findings {
		if finding.Code == code {
			return finding
		}
	}
	t.Fatalf("findings %v carry no %s", codesOf(findings), code)
	return Finding{}
}

func TestAcknowledgedButUnfinishedNoncesAreReported(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{
		DispositionFinishedUsed:        90,
		DispositionUnfinishedExecution: 10,
	})

	finding := findingWithCode(t, findingsFor(record), FindingExecutionTimeouts)

	if finding.Severity != SeverityCritical {
		t.Fatalf("severity = %s, want critical at 10%%", finding.Severity)
	}
	if finding.Part != 10 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want 10 of 100", finding.Part, finding.Whole)
	}
}

// A rate is only worth reading once the sample is worth reading. Without the floor, a participant the
// gateway barely used would be reported as its worst host.
func TestARateOverTooFewNoncesIsNotReported(t *testing.T) {
	record := recordWith(5, map[Disposition]uint64{
		DispositionFinishedUsed:        1,
		DispositionUnfinishedExecution: 4,
	})

	if findings := findingsFor(record); len(findings) != 0 {
		t.Fatalf("findings = %v, want none below the volume floor", codesOf(findings))
	}
}

func TestACleanParticipantIsReportedAsClean(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{DispositionFinishedUsed: 100})

	if findings := findingsFor(record); len(findings) != 0 {
		t.Fatalf("findings = %v, want none for a participant that finished everything", codesOf(findings))
	}
}

// Burns are the gateway's own decision. Counting them against the host would report our throttling as
// its failure, which is the one reading a host operator cannot act on. The assertion names the
// denominator rather than the severity, so it pins which nonces the rate is taken over.
func TestBurnedNoncesDoNotCountAgainstTheHostsFailureRates(t *testing.T) {
	record := recordWith(500, map[Disposition]uint64{
		DispositionFinishedUsed:        95,
		DispositionUnfinishedExecution: 5,
		DispositionGhost:               400,
	})

	finding := findingWithCode(t, findingsFor(record), FindingExecutionTimeouts)

	if finding.Part != 5 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want 5 of 100", finding.Part, finding.Whole)
	}
}

func TestGatewaySideThrottlingIsReportedAsTheGatewaysOwn(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{DispositionFinishedUsed: 60, DispositionGhost: 40})
	record.Counters = []CounterRecord{{
		CounterKey: CounterKey{Disposition: DispositionGhost, GhostReason: "participant_throttled_no_send"},
		Count:      40,
	}}

	finding := findingWithCode(t, findingsFor(record), FindingGatewayThrottled)

	if finding.Part != 40 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want the burns measured against assigned nonces", finding.Part, finding.Whole)
	}
}

// Counting more than the chain assigned is this gateway's own bug, so it is reported at any volume.
func TestLedgerOvercountingIsAlwaysReported(t *testing.T) {
	record := recordWith(10, map[Disposition]uint64{DispositionFinishedUsed: 10})
	record.Overcounted = 3

	finding := findingWithCode(t, findingsFor(record), FindingLedgerOvercounted)

	if finding.Severity != SeverityWarning {
		t.Fatalf("severity = %s, want warning: no host behaviour produces this", finding.Severity)
	}
}

// Overcounting has its own code, so the disagreement code must report what is left after it: an
// operator alerting on drift would otherwise be reading the overcount bug under a name that means
// something else, and would never see real drift at all.
func TestChainDisagreementReportsTheDriftBesideTheOvercount(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{DispositionFinishedUsed: 100})
	record.Overcounted = 2
	record.CrossChecks.ErrorCount = 7

	finding := findingWithCode(t, findingsFor(record), FindingChainDisagreement)

	if finding.Part != 5 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want the 5 drifting nonces the overcount does not explain",
			finding.Part, finding.Whole)
	}
}

func TestUnusedAnswersReadAgainstWhatWasDelivered(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{
		DispositionFinishedUsed:   70,
		DispositionFinishedUnused: 30,
	})

	finding := findingWithCode(t, findingsFor(record), FindingUnusedAnswers)

	if finding.Part != 30 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want 30 of 100", finding.Part, finding.Whole)
	}
}

func recordCrossing(assigned uint64, delivered uint64, key CounterKey, crossings uint64) ParticipantRecord {
	record := recordWith(assigned, map[Disposition]uint64{DispositionFinishedUsed: delivered})
	record.Counters = []CounterRecord{
		{CounterKey: key, Count: crossings},
		{CounterKey: CounterKey{Disposition: DispositionFinishedUsed}, Count: delivered - crossings},
	}
	return record
}

func TestSlowChunksAreReportedAgainstDeliveredAnswers(t *testing.T) {
	record := recordCrossing(200, 100,
		CounterKey{Disposition: DispositionFinishedUsed, SlowChunk: true}, 20)

	finding := findingWithCode(t, findingsFor(record), FindingSlowChunks)

	if finding.Part != 20 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want 20 of 100", finding.Part, finding.Whole)
	}
}

func TestDriftedClocksAreReported(t *testing.T) {
	record := recordCrossing(200, 100,
		CounterKey{Disposition: DispositionFinishedUsed, ClockDrifted: true}, 10)

	finding := findingWithCode(t, findingsFor(record), FindingClockDrift)

	if finding.Part != 10 {
		t.Fatalf("finding = %d drifted, want 10", finding.Part)
	}
}

// The breakdown lives in the counters, which carry the terminal and the phase; the finding carries only
// how many failures reached the host at all.
func TestFailureTerminalsCountEveryFailureThatReachedTheHost(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{
		DispositionFinishedUsed:        80,
		DispositionUnfinishedExecution: 20,
	})
	record.Counters = []CounterRecord{
		{CounterKey: CounterKey{Disposition: DispositionUnfinishedExecution, Terminal: "empty_stream", Phase: PhasePoC}, Count: 5},
		{CounterKey: CounterKey{Disposition: DispositionUnfinishedExecution, Terminal: "empty_stream"}, Count: 15},
	}

	finding := findingWithCode(t, findingsFor(record), FindingFailureTerminals)

	if finding.Part != 20 {
		t.Fatalf("finding = %d failures, want both the PoC and the normal one", finding.Part)
	}
	if counted := countersWhere(record, both(outsidePoC, failedWithoutAnswer)); counted != 15 {
		t.Fatalf("counters outside PoC = %d, want 15: the split lives there now", counted)
	}
}

func TestStallsDuringPoCAreNotChargedToTheHost(t *testing.T) {
	record := recordWith(200, map[Disposition]uint64{DispositionFinishedUsed: 100})
	record.Counters = []CounterRecord{
		{CounterKey: CounterKey{Disposition: DispositionFinishedUsed, SlowChunk: true, Phase: PhasePoC}, Count: 40},
		{CounterKey: CounterKey{Disposition: DispositionFinishedUsed, SlowChunk: true}, Count: 10},
		{CounterKey: CounterKey{Disposition: DispositionFinishedUsed}, Count: 50},
	}

	finding := findingWithCode(t, findingsFor(record), FindingSlowChunks)

	if finding.Part != 10 || finding.Whole != 60 {
		t.Fatalf("finding = %d of %d, want 10 of 60", finding.Part, finding.Whole)
	}
}

// The settlement already declined to vote this nonce timed out, so counting it as the host's failure
// would charge it for a judgement the gateway itself refused to make.
func TestALongResponseTheGatewayExcusedIsNotAFailure(t *testing.T) {
	record := recordWith(200, map[Disposition]uint64{
		DispositionFinishedUsed:        100,
		DispositionUnfinishedExecution: 10,
	})
	record.Counters = []CounterRecord{{
		CounterKey: CounterKey{
			Disposition:   DispositionUnfinishedExecution,
			TimeoutReason: TimeoutReasonLongResponse,
		},
		Count: 10,
	}}

	if codes := codesOf(findingsFor(record)); slices.Contains(codes, FindingExecutionTimeouts) {
		t.Fatalf("findings = %v, want no execution-timeout finding for nonces the gateway excused", codes)
	}
}

// Two hosts held 39.5% of a model's traffic at a sixth of their peers' rate and nothing named it: the
// answers arrived and were correct, they just took six times as long to write.
func TestSlowDecodersAreReportedAgainstDeliveredAnswers(t *testing.T) {
	record := recordCrossing(200, 100,
		CounterKey{Disposition: DispositionFinishedUsed, SlowDecode: true}, 20)

	finding := findingWithCode(t, findingsFor(record), FindingSlowDecode)

	if finding.Part != 20 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want 20 of 100", finding.Part, finding.Whole)
	}
}

// A host serving under PoC is proving computation, not serving, so its rate says nothing about how it
// writes for a client.
func TestASlowDecodeDuringPoCIsNotChargedToTheHost(t *testing.T) {
	record := recordCrossing(200, 100,
		CounterKey{Disposition: DispositionFinishedUsed, SlowDecode: true, Phase: PhasePoC}, 40)

	for _, finding := range findingsFor(record) {
		if finding.Code == FindingSlowDecode {
			t.Fatalf("finding = %+v, want none: a PoC answer is not a serving answer", finding)
		}
	}
}

// A dashboard and an operator's alert select on these strings, and the ledger they were written
// against is the one this gateway replaces. A rename is a silent alert that stops firing.
func TestTheFindingVocabularyKeepsTheNamesOperatorsAlertOn(t *testing.T) {
	for _, code := range []string{
		"execution_timeouts", "refusals", "answers_unused", "throttled_by_gateway",
		"blocked_by_capability", "chain_recorded_misses", "chain_recorded_invalid",
		"challenges_unresolved", "timeouts_undecided", "reasons_unknown",
		"ledger_disagrees_with_chain", "ledger_overcounted", "logprobs_not_token_ids",
		"slow_receipts", "slow_chunks", "clock_drift", "slow_decode",
	} {
		if !slices.Contains(findingCodes, code) {
			t.Errorf("%q is no longer a code this gateway can emit", code)
		}
	}
}

// A skipped round asked nobody, so counting it as decided would hide a host whose every real round
// ends without a verdict behind the rounds that were never raised.
func TestUndecidedTimeoutsAreMeasuredAgainstTheRoundsThatVoted(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{DispositionFinishedUsed: 100})
	record.TimeoutOutcomes = map[TimeoutOutcome]uint64{
		TimeoutSkipped:              500,
		TimeoutApplied:              15,
		TimeoutInsufficientVotes:    3,
		TimeoutVoteCollectionFailed: 2,
	}

	finding := findingWithCode(t, findingsFor(record), FindingUndecidedTimeouts)

	if finding.Part != 5 || finding.Whole != 20 {
		t.Fatalf("finding = %d of %d, want 5 undecided of the 20 rounds that voted", finding.Part, finding.Whole)
	}
}

// A host blocked by capability burns every nonce it is assigned; reading that as ordinary throttling
// hides a defect only its operator can fix.
func TestCapabilityBurnsAreReportedApartFromThrottling(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{DispositionFinishedUsed: 100})
	record.Counters = []CounterRecord{{
		CounterKey: CounterKey{Disposition: DispositionGhost, GhostReason: "participant_capability_no_send"},
		Count:      30,
	}}

	findings := findingsFor(record)

	blocked := findingWithCode(t, findings, FindingCapabilityBlocked)
	if blocked.Part != 30 {
		t.Fatalf("blocked = %d, want the 30 burned nonces", blocked.Part)
	}
	if slices.Contains(codesOf(findings), FindingGatewayThrottled) {
		t.Error("a capability burn was also counted as gateway throttling: one cause, two accusations")
	}
}

// The threshold is a tenth of a percent because a single unreplayable answer is already a lost reward,
// and a host that does it once does it for every answer with logprobs.
func TestDecodedLogprobsAreReportedAtOnce(t *testing.T) {
	record := recordWith(100, map[Disposition]uint64{DispositionFinishedUsed: 100})
	record.Counters = []CounterRecord{{
		CounterKey: CounterKey{Disposition: DispositionFinishedUsed, LogprobsDecoded: true},
		Count:      1,
	}}

	finding := findingWithCode(t, findingsFor(record), FindingDecodedLogprobs)

	if finding.Part != 1 || finding.Whole != 100 {
		t.Fatalf("finding = %d of %d, want the one unreplayable answer against what was delivered",
			finding.Part, finding.Whole)
	}
}

// The doc is what an operator reads when a finding fires, so a code that exists in neither place is
// unexplainable and one that exists only in the doc is a promise this gateway does not keep.
func TestEveryFindingCodeIsDocumented(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("..", "docs", "accounting-findings.md"))
	if err != nil {
		t.Fatalf("reading accounting-findings.md: %v", err)
	}
	for _, code := range findingCodes {
		if !bytes.Contains(doc, []byte("`"+code+"`")) {
			t.Errorf("finding %q is not explained in accounting-findings.md", code)
		}
	}
}
