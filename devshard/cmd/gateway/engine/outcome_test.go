package engine

import (
	"context"
	"testing"
	"time"

	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/transport"
)

const (
	testParticipant = "gonka1host"
	testModel       = "Qwen/Qwen3-235B"
)

var testEpoch = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

func limiterConfig(tripThreshold int64) limits.ParticipantConfig {
	return limits.ParticipantConfig{
		InitialWindow: 4,
		MaxWindow:     10,
		TripThreshold: tripThreshold,
		BaseOpen:      time.Minute,
		MaxOpen:       time.Hour,
	}
}

// observedWindow measures the AIMD window through a real limiter instead of asserting against a
// second copy of limits' own rules: with a trip threshold no single verdict can reach, the only
// thing that can change the admitted count is the window itself.
func observedWindow(verdict limits.Verdict, recorded bool) int {
	limiter := limits.NewParticipantLimiter(limiterConfig(1000), func() time.Time { return testEpoch })
	limiter.Acquire(testParticipant, testModel)
	limiter.Acquire(testParticipant, testModel)
	if recorded {
		limiter.OnResult(testParticipant, testModel, verdict)
	}
	limiter.Release(testParticipant, testModel)
	limiter.Release(testParticipant, testModel)

	admitted := 0
	for limiter.Acquire(testParticipant, testModel) {
		admitted++
	}
	return admitted
}

func observedBreakerOpen(verdict limits.Verdict, recorded bool) bool {
	limiter := limits.NewParticipantLimiter(limiterConfig(1), func() time.Time { return testEpoch })
	if recorded {
		limiter.OnResult(testParticipant, testModel, verdict)
	}
	return !limiter.Available(testParticipant, testModel)
}

func cleanAttempt() AttemptOutcome {
	return AttemptOutcome{
		Participant:   testParticipant,
		Nonce:         7,
		Role:          "primary",
		StartedAt:     testEpoch,
		SendTime:      testEpoch,
		ReceiptTime:   testEpoch.Add(200 * time.Millisecond),
		FirstToken:    testEpoch.Add(900 * time.Millisecond),
		Completed:     testEpoch.Add(3 * time.Second),
		ContentChunks: 12,
		Terminal:      TerminalWon,
		Confirmed:     true,
		NonceFinished: true,
	}
}

func failedAttempt(terminal Terminal) AttemptOutcome {
	attempt := cleanAttempt()
	attempt.Terminal = terminal
	attempt.Confirmed = false
	attempt.NonceFinished = false
	attempt.ContentChunks = 0
	return attempt
}

func race(attempt AttemptOutcome) RaceOutcome {
	return RaceOutcome{
		RequestID:   "req-1",
		EscrowID:    "escrow-1",
		Model:       testModel,
		InputTokens: 1024,
		WinnerNonce: 7,
		Succeeded:   true,
		Attempts:    []AttemptOutcome{attempt},
	}
}

// Every upstream condition in the verdict specification, asserting the verdict, whether it is
// reported at all, and what a real limiter does with it.
func TestVerdictTable(t *testing.T) {
	stalledOverThreshold := failedAttempt(TerminalStalled)
	stalledOverThreshold.ContentChunks = 5
	stalledOverThreshold.FailureRateExceeded = true

	stalledUnderThreshold := failedAttempt(TerminalStalled)
	stalledUnderThreshold.ContentChunks = 5

	// ContentSource is what makes this a long response rather than a long silence: ContentChunks
	// counts error events too, and an error-only attempt must keep its timeout vote.
	longResponse := cleanAttempt()
	longResponse.NonceFinished = false
	longResponse.ContentSource = "delta.content"
	longResponse.Completed = testEpoch.Add(longResponseExemption)

	unfinished := cleanAttempt()
	unfinished.NonceFinished = false

	tests := []struct {
		name         string
		outcome      RaceOutcome
		attempt      AttemptOutcome
		wantVerdict  limits.Verdict
		wantRecorded bool
		wantWindow   int
		wantBreaker  bool
	}{
		{"clean finish, nonce finished, content present", race(cleanAttempt()), cleanAttempt(), limits.Success, true, 5, false},
		{"clean finish by a loser", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.Terminal = TerminalLost
			attempt.Nonce = 8
			return attempt
		}(), limits.Success, true, 5, false},
		{"http 429", race(cleanAttempt()), failedAttempt(TerminalThrottled), limits.Overload, true, 2, false},
		{"http 503", race(cleanAttempt()), failedAttempt(TerminalUnavailable), limits.Overload, true, 2, false},
		{"http 404 on the inference path", race(cleanAttempt()), failedAttempt(TerminalNotFound), limits.TransportFault, true, 4, true},
		{"http 403 on the inference path", race(cleanAttempt()), failedAttempt(TerminalForbidden), limits.TransportFault, true, 4, true},
		{"http 401 with timestamp drift", race(cleanAttempt()), failedAttempt(TerminalTimestampDrift), limits.TransportFault, true, 4, true},
		{"http 400 / 500 / any other status", race(cleanAttempt()), failedAttempt(TerminalRejected), limits.ModelOutcome, false, 4, false},
		{"dial error, connection reset, TLS failure", race(cleanAttempt()), failedAttempt(TerminalDialFailure), limits.TransportFault, true, 4, true},
		{"unexpected EOF", race(cleanAttempt()), failedAttempt(TerminalUnexpectedEOF), limits.TransportFault, true, 4, true},
		{"truncated SSE stream", race(cleanAttempt()), failedAttempt(TerminalStreamTruncated), limits.TransportFault, true, 4, true},
		{"failure on a non-inference path", race(cleanAttempt()), failedAttempt(TerminalOffPath), limits.ModelOutcome, false, 4, false},
		{"empty stream", race(failedAttempt(TerminalEmptyStream)), failedAttempt(TerminalEmptyStream), limits.ModelOutcome, true, 4, false},
		{"empty stream with completion tokens burned", race(cleanAttempt()), failedAttempt(TerminalBurnEmpty), limits.ModelOutcome, true, 4, false},
		{"error event inside the SSE stream", race(cleanAttempt()), failedAttempt(TerminalErrorStream), limits.ModelOutcome, true, 4, false},
		{"capability refusal another host can serve", race(cleanAttempt()), failedAttempt(TerminalCapabilityRefused), limits.ModelOutcome, true, 4, false},
		{"winner stalled after content, failure rate exceeded", race(stalledOverThreshold), stalledOverThreshold, limits.TransportFault, true, 4, true},
		{"winner stalled after content, failure rate not exceeded", race(stalledUnderThreshold), stalledUnderThreshold, limits.ModelOutcome, false, 4, false},
		{"content produced, past the exemption, nonce unfinished", race(longResponse), longResponse, limits.ModelOutcome, false, 4, false},
		{"empty stream while the PoC bypass is active", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalEmptyStream))
			outcome.PoCBypassActive = true
			return outcome
		}(), failedAttempt(TerminalEmptyStream), limits.ModelOutcome, false, 4, false},
		{"attempt aborted by a phase transition", race(cleanAttempt()), func() AttemptOutcome {
			attempt := failedAttempt(TerminalEmptyStream)
			attempt.PhaseTransitionAborted = true
			return attempt
		}(), limits.ModelOutcome, false, 4, false},
		{"escrow not found", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalRejected))
			outcome.Lifecycle.EscrowMissing = true
			return outcome
		}(), failedAttempt(TerminalRejected), limits.ModelOutcome, false, 4, false},
		{"post-state-root divergence", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.StateDivergent = true
			return attempt
		}(), limits.ModelOutcome, false, 4, false},
		{"clean finish with the nonce still unfinished", race(unfinished), unfinished, limits.ModelOutcome, false, 4, false},
		{"client cancelled", race(cleanAttempt()), failedAttempt(TerminalClientCancelled), limits.ModelOutcome, false, 4, false},
		{"send returned without a receipt", race(cleanAttempt()), failedAttempt(TerminalNoReceipt), limits.ModelOutcome, false, 4, false},
		{"unclassified", race(cleanAttempt()), failedAttempt(TerminalUnclassified), limits.ModelOutcome, false, 4, false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verdict, recorded := testCase.outcome.Verdict(testCase.attempt)
			if verdict != testCase.wantVerdict || recorded != testCase.wantRecorded {
				t.Fatalf("Verdict() = (%v, %v), want (%v, %v)", verdict, recorded, testCase.wantVerdict, testCase.wantRecorded)
			}
			if window := observedWindow(verdict, recorded); window != testCase.wantWindow {
				t.Errorf("window after the verdict = %d, want %d", window, testCase.wantWindow)
			}
			if open := observedBreakerOpen(verdict, recorded); open != testCase.wantBreaker {
				t.Errorf("breaker open after the verdict = %v, want %v", open, testCase.wantBreaker)
			}
		})
	}
}

func TestSampleExemptionLadderRungs(t *testing.T) {
	// ContentSource is what makes this a long response rather than a long silence: ContentChunks
	// counts error events too, and an error-only attempt must keep its timeout vote.
	longResponse := cleanAttempt()
	longResponse.NonceFinished = false
	longResponse.ContentSource = "delta.content"
	longResponse.Completed = testEpoch.Add(longResponseExemption)

	tests := []struct {
		name    string
		outcome RaceOutcome
		attempt AttemptOutcome
		want    SampleExemption
	}{
		{"healthy attempt", race(cleanAttempt()), cleanAttempt(), SampleRecorded},
		{"phase transition abort", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.PhaseTransitionAborted = true
			return attempt
		}(), ExemptPhaseAborted},
		{"error stream", race(cleanAttempt()), failedAttempt(TerminalErrorStream), ExemptErrorStream},
		{"capability refusal", race(cleanAttempt()), failedAttempt(TerminalCapabilityRefused), ExemptErrorStream},
		{"state-root divergence", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.StateDivergent = true
			return attempt
		}(), ExemptStateDivergent},
		{"long response", race(longResponse), longResponse, ExemptLongResponse},
		{"empty stream under the PoC bypass", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalEmptyStream))
			outcome.PoCBypassActive = true
			return outcome
		}(), failedAttempt(TerminalEmptyStream), ExemptPoCSuppressed},
		{"empty stream in a race nobody won", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalEmptyStream))
			outcome.Succeeded = false
			return outcome
		}(), failedAttempt(TerminalEmptyStream), ExemptEmptyStreamNoWinner},
		{"empty stream in a race someone else won", race(failedAttempt(TerminalEmptyStream)), failedAttempt(TerminalEmptyStream), SampleRecorded},
		{"a loser the race itself cancelled", race(cleanAttempt()), failedAttempt(TerminalClientCancelled), ExemptClientCancelled},
		{"an assignment no attempt could spend", race(cleanAttempt()), AttemptOutcome{Participant: testParticipant, Terminal: TerminalNoReceipt}, ExemptNeverDispatched},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.outcome.sampleExemption(testCase.attempt); got != testCase.want {
				t.Fatalf("sampleExemption() = %v, want %v", got, testCase.want)
			}
			_, exemption := testCase.outcome.Sample(testCase.attempt)
			if (exemption == SampleRecorded) != (testCase.want == SampleRecorded) {
				t.Fatalf("Sample() exemption = %v, want a sample: %v", exemption, testCase.want == SampleRecorded)
			}
		})
	}
}

// Earlier rungs must win: each combination is exempt for two reasons at once and the ladder has to
// report the first, or the order it claims to have is not the order it applies.
func TestSampleExemptionLadderOrder(t *testing.T) {
	abortedAndDivergent := failedAttempt(TerminalEmptyStream)
	abortedAndDivergent.PhaseTransitionAborted = true
	abortedAndDivergent.StateDivergent = true

	divergentAndPoCSuppressed := failedAttempt(TerminalEmptyStream)
	divergentAndPoCSuppressed.StateDivergent = true

	pocSuppressedRace := race(divergentAndPoCSuppressed)
	pocSuppressedRace.PoCBypassActive = true

	tests := []struct {
		name    string
		outcome RaceOutcome
		attempt AttemptOutcome
		want    SampleExemption
	}{
		{"phase abort before state divergence", race(abortedAndDivergent), abortedAndDivergent, ExemptPhaseAborted},
		{"state divergence before the PoC bypass", pocSuppressedRace, divergentAndPoCSuppressed, ExemptStateDivergent},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.outcome.sampleExemption(testCase.attempt); got != testCase.want {
				t.Fatalf("sampleExemption() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestSampleFields(t *testing.T) {
	t.Parallel()

	outcome := race(cleanAttempt())
	sample, exemption := outcome.Sample(cleanAttempt())
	if exemption != SampleRecorded {
		t.Fatalf("Sample() exemption = %v, want SampleRecorded", exemption)
	}

	want := perf.Sample{
		ParticipantKey: testParticipant,
		Model:          testModel,
		Responsive:     true,
	}
	if sample != want {
		t.Fatalf("Sample() = %+v, want %+v", sample, want)
	}
}

func TestSampleResponsive(t *testing.T) {
	longNonStreamEmpty := failedAttempt(TerminalEmptyStream)
	longNonStreamEmpty.Completed = testEpoch.Add(longResponseExemption)

	tests := []struct {
		name    string
		outcome RaceOutcome
		attempt AttemptOutcome
		want    bool
	}{
		{"clean finish", race(cleanAttempt()), cleanAttempt(), true},
		{"nonce unfinished", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.NonceFinished = false
			return attempt
		}(), false},
		{"receipt unconfirmed", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.Confirmed = false
			return attempt
		}(), false},
		{"empty stream", race(failedAttempt(TerminalEmptyStream)), func() AttemptOutcome {
			attempt := failedAttempt(TerminalEmptyStream)
			attempt.Confirmed = true
			attempt.NonceFinished = true
			return attempt
		}(), false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			sample, exemption := testCase.outcome.Sample(testCase.attempt)
			if exemption != SampleRecorded {
				t.Fatalf("Sample() exemption = %v, want SampleRecorded", exemption)
			}
			if sample.Responsive != testCase.want {
				t.Fatalf("Sample().Responsive = %v, want %v", sample.Responsive, testCase.want)
			}
		})
	}
}

func TestEmptyStreamDeniesCrowningWithoutMovingTheWindow(t *testing.T) {
	t.Parallel()

	attempt := failedAttempt(TerminalEmptyStream)
	outcome := race(attempt)

	verdict, recorded := outcome.Verdict(attempt)
	if verdict != limits.ModelOutcome || !recorded {
		t.Fatalf("Verdict() = (%v, %v), want (ModelOutcome, true)", verdict, recorded)
	}
	if window := observedWindow(verdict, recorded); window != 4 {
		t.Fatalf("window after an empty stream = %d, want 4 (untouched)", window)
	}
	if !outcome.DeniesCrowning(attempt) {
		t.Fatal("DeniesCrowning() = false, want true")
	}
}

func TestDeniesCrowning(t *testing.T) {
	longNonStreamEmpty := failedAttempt(TerminalEmptyStream)
	longNonStreamEmpty.Completed = testEpoch.Add(longResponseExemption)

	tests := []struct {
		name    string
		outcome RaceOutcome
		attempt AttemptOutcome
		want    bool
	}{
		{"empty stream", race(failedAttempt(TerminalEmptyStream)), failedAttempt(TerminalEmptyStream), true},
		{"burned completion tokens", race(failedAttempt(TerminalBurnEmpty)), failedAttempt(TerminalBurnEmpty), false},
		{"clean finish", race(cleanAttempt()), cleanAttempt(), false},
		{"phase transition abort", race(cleanAttempt()), func() AttemptOutcome {
			attempt := failedAttempt(TerminalEmptyStream)
			attempt.PhaseTransitionAborted = true
			return attempt
		}(), false},
		{"PoC bypass active", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalEmptyStream))
			outcome.PoCBypassActive = true
			return outcome
		}(), failedAttempt(TerminalEmptyStream), false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.outcome.DeniesCrowning(testCase.attempt); got != testCase.want {
				t.Fatalf("DeniesCrowning() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestLabels(t *testing.T) {
	loser := cleanAttempt()
	loser.Terminal = TerminalLost
	loser.Nonce = 8

	suspicious := cleanAttempt()
	suspicious.Suspicious = true
	suspicious.Terminal = TerminalLost
	suspicious.Nonce = 8

	suspiciousWinner := cleanAttempt()
	suspiciousWinner.Suspicious = true

	unfinished := cleanAttempt()
	unfinished.NonceFinished = false

	aborted := failedAttempt(TerminalEmptyStream)
	aborted.PhaseTransitionAborted = true

	roleless := cleanAttempt()
	roleless.Role = ""

	tests := []struct {
		name       string
		outcome    RaceOutcome
		attempt    AttemptOutcome
		wantLabels AttemptLabels
	}{
		{"winner", race(cleanAttempt()), cleanAttempt(), AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "success", Visibility: "user_visible_winner",
		}},
		{"suppressed loser", race(cleanAttempt()), loser, AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "success", Visibility: "suppressed_loser",
		}},
		{"shadow-quarantined host another attempt served for", race(cleanAttempt()), suspicious, AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "success", Visibility: "no_winner", Reason: reasonCrownDenied,
		}},
		{"shadow-quarantined host that served alone", race(suspiciousWinner), suspiciousWinner, AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "success", Visibility: "user_visible_winner",
		}},
		{"missing role defaults to primary", race(roleless), roleless, AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "success", Visibility: "user_visible_winner",
		}},
		{"nonce unfinished", race(unfinished), unfinished, AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "failed", Visibility: "failed_not_finished", Reason: "not_finished",
		}},
		{"http 429", race(cleanAttempt()), failedAttempt(TerminalThrottled), AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "failed", Visibility: "failed_not_finished", Reason: "http_429",
		}},
		{"truncated stream", race(cleanAttempt()), failedAttempt(TerminalStreamTruncated), AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "failed", Visibility: "failed_not_finished", Reason: "sse_truncated",
		}},
		{"phase abort outranks the terminal reason", race(aborted), aborted, AttemptLabels{
			Participant: testParticipant, Model: testModel, Role: "primary",
			Outcome: "failed", Visibility: "failed_not_finished", Reason: "phase_transition_aborted",
		}},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			labels := testCase.outcome.Labels(testCase.attempt)
			if labels != testCase.wantLabels {
				t.Fatalf("Labels() = %+v, want %+v", labels, testCase.wantLabels)
			}
		})
	}
}

func TestTerminalFailureReasons(t *testing.T) {
	tests := []struct {
		terminal Terminal
		want     string
	}{
		{TerminalUnclassified, "unknown"},
		{TerminalWon, ""},
		{TerminalLost, ""},
		{TerminalThrottled, "http_429"},
		{TerminalUnavailable, "http_503"},
		{TerminalForbidden, "http_forbidden"},
		{TerminalNotFound, "http_not_found"},
		{TerminalTimestampDrift, "http_timestamp_drift"},
		{TerminalRejected, "http_error"},
		{TerminalOffPath, "off_path"},
		{TerminalDialFailure, "transport_error"},
		{TerminalStreamTruncated, "sse_truncated"},
		{TerminalUnexpectedEOF, "eof_transport"},
		{TerminalClientCancelled, "client_cancelled"},
		{TerminalNoReceipt, "no_receipt"},
		{TerminalEmptyStream, "empty_stream"},
		{TerminalBurnEmpty, "empty_stream"},
		{TerminalErrorStream, "error_stream"},
		{TerminalCapabilityRefused, "error_stream"},
		{TerminalStalled, "stalled"},
	}

	for _, testCase := range tests {
		if got := testCase.terminal.reason(); got != testCase.want {
			t.Errorf("Terminal(%d).reason() = %q, want %q", testCase.terminal, got, testCase.want)
		}
	}
}

// The engine reports lifecycle signals and does nothing with them: no verdict, sample, label or
// crowning decision may read them, or the engine would be acting on escrow state it cannot own.
func TestLifecycleSignalsAreInert(t *testing.T) {
	t.Parallel()

	attempt := cleanAttempt()
	quiet := race(attempt)
	signalled := race(attempt)
	signalled.Lifecycle = Lifecycle{EscrowMissing: true, BalanceExhausted: true}

	quietVerdict, quietRecorded := quiet.Verdict(attempt)
	signalledVerdict, signalledRecorded := signalled.Verdict(attempt)
	if quietVerdict != signalledVerdict || quietRecorded != signalledRecorded {
		t.Errorf("Verdict() changed with lifecycle signals: (%v, %v) vs (%v, %v)", quietVerdict, quietRecorded, signalledVerdict, signalledRecorded)
	}

	quietSample, quietExemption := quiet.Sample(attempt)
	signalledSample, signalledExemption := signalled.Sample(attempt)
	if quietSample != signalledSample || quietExemption != signalledExemption {
		t.Errorf("Sample() changed with lifecycle signals: %+v/%v vs %+v/%v", quietSample, quietExemption, signalledSample, signalledExemption)
	}

	quietLabels := quiet.Labels(attempt)
	signalledLabels := signalled.Labels(attempt)
	if quietLabels != signalledLabels {
		t.Errorf("Labels() changed with lifecycle signals: %+v vs %+v", quietLabels, signalledLabels)
	}
	if quiet.DeniesCrowning(attempt) != signalled.DeniesCrowning(attempt) {
		t.Error("DeniesCrowning() changed with lifecycle signals")
	}
}

// classifyDispatchError and metrics read one table in opposite directions. A status that classifies
// to one terminal and reports back as a different one mislabels the transport-error metric with a
// status no host returned, and nothing else would notice.
func TestEveryRecoveredStatusRoundTripsThroughItsTerminal(t *testing.T) {
	for terminal, status := range terminalStatuses {
		body := ""
		if terminal == TerminalTimestampDrift {
			body = "timestamp drift detected"
		}
		err := &transport.UpstreamStatusError{Path: "/v1/chat/completions", StatusCode: status, Body: body}

		classified := classifyDispatchError(context.Background(), err)

		if classified != terminal {
			t.Errorf("status %d classified as %v, want %v", status, classified, terminal)
		}
		if recovered, ok := StatusFor(classified); !ok || recovered != status {
			t.Errorf("StatusFor(%v) = %d/%v, want %d/true", classified, recovered, ok, status)
		}
	}
}

// The log field is called terminal, so it carries the terminal's name. reason() answers why an attempt
// failed and is empty for the two outcomes that are not failures -- an empty log field reads as missing
// data, and failureReason depends on that same emptiness to fall through.
func TestEveryTerminalHasAName(t *testing.T) {
	for terminal := TerminalUnclassified; terminal <= TerminalStalled; terminal++ {
		if terminal.String() == "" {
			t.Fatalf("terminal %d has no name", terminal)
		}
		if terminal.String() == "unnamed" {
			t.Fatalf("terminal %d fell through to the placeholder, so a new terminal was added without one", terminal)
		}
	}
	if TerminalWon.reason() != "" || TerminalLost.reason() != "" {
		t.Fatal("a win or a loss reported a failure reason, which failureReason would report as the cause")
	}
}

// A committed nonce whose attempt never reported used to be dropped from the outcome entirely: no log
// line, no ledger row, and -- because TimeoutPlan reads only the outcome -- no timeout vote for a nonce
// already spent on chain. Production logs showed 8 of 255 nonces leaving no trace at all.
func TestAnAttemptThatNeverReportedStillOwesAVote(t *testing.T) {
	outcome := RaceOutcome{
		Model: testModel,
		Attempts: []AttemptOutcome{{
			Nonce:       557,
			Participant: testParticipant,
			SendTime:    raceStart,
			Terminal:    TerminalUnclassified,
		}},
	}

	plan := outcome.TimeoutPlan()

	if len(plan) != 1 || plan[0].Nonce != 557 {
		t.Fatalf("TimeoutPlan() = %+v, want one step for nonce 557", plan)
	}
	if !plan[0].Post {
		t.Fatalf("step = %+v, want the vote posted: the nonce was spent and never finished", plan[0])
	}
	if _, exemption := outcome.Sample(outcome.Attempts[0]); exemption != ExemptNeverReported {
		t.Fatalf("exemption = %v, want the host judged for nothing it never answered", exemption)
	}
}
