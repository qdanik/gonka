package engine

import (
	"context"
	"math"
	"testing"
	"time"

	"devshard/bridge"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/transport"
	"devshard/types"
)

const (
	testParticipant = "gonka1host"
	testModel       = "Qwen/Qwen3-235B"
)

var testEpoch = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)

// The probe prices a request at 16 tokens on both sides and starts each window 16 requests wide, so a
// window starts at 256 tokens. One attempt fills the window, which is the peak a healthy answer needs
// before it widens; nothing has narrowed the window yet, so that answer widens it by every token it carried.
const (
	probeRequestTokens    = 16
	probeStartingRequests = 16
	probeStartingWindow   = probeStartingRequests * probeRequestTokens
)

var probeAttempt = limits.TokenCost{Input: probeStartingWindow, Output: probeStartingWindow}

func limiterConfig(tripThreshold int64) limits.ParticipantConfig {
	return limits.ParticipantConfig{
		Pricing: limits.WindowPricing{
			Input:                 limits.RequestBounds{Min: 1, Initial: probeStartingRequests},
			Output:                limits.RequestBounds{Min: 1, Initial: probeStartingRequests},
			FallbackContextTokens: probeRequestTokens,
			FallbackOutputTokens:  probeRequestTokens,
		},
		Factors:       limits.CongestionFactors{Soft: 0.85, Hard: 0.70, Severe: 0.50, Cross: 0.90},
		AfterFailures: tripThreshold,
		BaseOpen:      time.Minute,
		MaxOpen:       time.Hour,
	}
}

// observedWindows drives one verdict through a real limiter instead of asserting against a second copy of
// limits' own rules, and reads both windows back off the snapshot metrics and /v1/admin/hosts are served from.
func observedWindows(t *testing.T, verdict limits.Verdict, recorded bool) (input, output float64) {
	t.Helper()

	limiter := limits.NewParticipantLimiter(limiterConfig(1000), func() time.Time { return testEpoch })
	release, _ := limiter.Acquire(testParticipant, testModel, probeAttempt)
	if recorded {
		limiter.OnResult(probeResult(verdict))
	}
	release()

	return trackedWindows(t, limiter)
}

func trackedWindows(t *testing.T, limiter *limits.ParticipantLimiter) (input, output float64) {
	t.Helper()

	tracked := limiter.Snapshot()
	if len(tracked) != 1 {
		t.Fatalf("limiter tracks %d pairs, want the one host the test drove it with", len(tracked))
	}
	return tracked[0].InputWindowTokens, tracked[0].OutputWindowTokens
}

func observedCutoffOpen(verdict limits.Verdict, recorded bool) bool {
	limiter := limits.NewParticipantLimiter(limiterConfig(1), func() time.Time { return testEpoch })
	if recorded {
		limiter.OnResult(probeResult(verdict))
	}
	return !limiter.Available(testParticipant, testModel)
}

func probeResult(verdict limits.Verdict) limits.Result {
	return limits.Result{
		Participant: testParticipant,
		Model:       testModel,
		Verdict:     verdict,
		Carried:     probeAttempt,
	}
}

// A window is a float, and the ladder's factors do not land on round token counts; anything the ladder
// actually moves is wider apart than this.
const windowTolerance = 0.5

func assertWindowsAndCutoff(t *testing.T, verdict limits.Verdict, recorded bool, wantInput, wantOutput float64, wantCutoff bool) {
	t.Helper()

	input, output := observedWindows(t, verdict, recorded)
	if math.Abs(input-wantInput) > windowTolerance {
		t.Errorf("input window = %v, want %v: the tier this verdict blames prefill with, or the cross factor when it blames decode", input, wantInput)
	}
	if math.Abs(output-wantOutput) > windowTolerance {
		t.Errorf("output window = %v, want %v: the tier this verdict blames decode with, or the cross factor when it blames prefill", output, wantOutput)
	}
	if open := observedCutoffOpen(verdict, recorded); open != wantCutoff {
		t.Errorf("cutoff open = %v, want %v: only a fault the host never answered for counts towards cutting it off", open, wantCutoff)
	}
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
	stalled := failedAttempt(TerminalStalled)
	stalled.ContentChunks = 5

	// ContentSource is what makes this a long response rather than a long silence: ContentChunks
	// counts error events too, and an error-only attempt must keep its timeout vote.
	longResponse := cleanAttempt()
	longResponse.NonceFinished = false
	longResponse.ContentSource = "delta.content"
	longResponse.Completed = testEpoch.Add(longResponseExemption)

	unfinished := cleanAttempt()
	unfinished.NonceFinished = false

	heldEmpty := failedAttempt(TerminalEmptyStream)
	heldEmpty.Completed = testEpoch.Add(emptyStreamHeldTooLong)

	// A host that closed its nonce answered, even with nothing in it; one that did not took the work
	// and left its reserve parked until the timeout vote.
	finishedEmpty := failedAttempt(TerminalEmptyStream)
	finishedEmpty.NonceFinished = true

	heldFinishedEmpty := finishedEmpty
	heldFinishedEmpty.Completed = testEpoch.Add(emptyStreamHeldTooLong)

	heldBurnEmpty := failedAttempt(TerminalBurnEmpty)
	heldBurnEmpty.Completed = testEpoch.Add(15 * time.Minute)

	briefBurnEmpty := failedAttempt(TerminalBurnEmpty)
	briefBurnEmpty.Completed = testEpoch.Add(emptyStreamHeldTooLong - time.Millisecond)

	lateWinner := cleanAttempt()
	lateWinner.FirstTokenDeadlineMissed = true

	lateLoser := cleanAttempt()
	lateLoser.Terminal = TerminalLost
	lateLoser.Nonce = 8
	lateLoser.ReceiptDeadlineMissed = true

	lateThrottled := failedAttempt(TerminalThrottled)
	lateThrottled.ReceiptDeadlineMissed = true

	tests := []struct {
		name             string
		outcome          RaceOutcome
		attempt          AttemptOutcome
		wantVerdict      limits.Verdict
		wantRecorded     bool
		wantInputWindow  float64
		wantOutputWindow float64
		wantCutoff       bool
	}{
		{"clean finish, nonce finished, content present", race(cleanAttempt()), cleanAttempt(), limits.Success, true, 272, 272, false},
		{"clean finish by a loser", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.Terminal = TerminalLost
			attempt.Nonce = 8
			return attempt
		}(), limits.Success, true, 272, 272, false},
		{"clean finish after missing the first-token deadline", race(lateWinner), lateWinner, limits.LateSuccess, true, 256, 256, false},
		{"clean finish by a loser after missing the receipt deadline", race(cleanAttempt()), lateLoser, limits.LateSuccess, true, 256, 256, false},
		{"http 429 after missing the receipt deadline", race(cleanAttempt()), lateThrottled, limits.Overload, true, 217.6, 217.6, false},
		{"http 429", race(cleanAttempt()), failedAttempt(TerminalThrottled), limits.Overload, true, 217.6, 217.6, false},
		{"http 503", race(cleanAttempt()), failedAttempt(TerminalUnavailable), limits.Overload, true, 217.6, 217.6, false},
		{"http 404 on the inference path", race(cleanAttempt()), failedAttempt(TerminalNotFound), limits.TransportFault, true, 256, 256, true},
		{"http 403 on the inference path", race(cleanAttempt()), failedAttempt(TerminalForbidden), limits.TransportFault, true, 256, 256, true},
		{"http 401 with timestamp drift", race(cleanAttempt()), failedAttempt(TerminalTimestampDrift), limits.TransportFault, true, 256, 256, true},
		{"http 400 / 500 / any other status", race(cleanAttempt()), failedAttempt(TerminalRejected), limits.ModelOutcome, false, 256, 256, false},
		{"dial error, connection reset, TLS failure", race(cleanAttempt()), failedAttempt(TerminalDialFailure), limits.TransportFault, true, 256, 256, true},
		{"unexpected EOF", race(cleanAttempt()), failedAttempt(TerminalUnexpectedEOF), limits.TransportFault, true, 256, 256, true},
		{"truncated SSE stream", race(cleanAttempt()), failedAttempt(TerminalStreamTruncated), limits.TransportFault, true, 256, 256, true},
		{"failure on a non-inference path", race(cleanAttempt()), failedAttempt(TerminalOffPath), limits.ModelOutcome, false, 256, 256, false},
		{"empty stream that never finished its nonce", race(failedAttempt(TerminalEmptyStream)), failedAttempt(TerminalEmptyStream), limits.EmptyAnswerLeftOpen, true, 128, 128, true},
		{"empty stream that finished its nonce", race(finishedEmpty), finishedEmpty, limits.EmptyAnswer, true, 179.2, 179.2, false},
		{"empty stream with completion tokens burned", race(cleanAttempt()), failedAttempt(TerminalBurnEmpty), limits.ModelOutcome, true, 256, 256, false},
		{"error event inside the SSE stream", race(cleanAttempt()), failedAttempt(TerminalErrorStream), limits.ModelOutcome, true, 256, 256, false},
		{"capability refusal another host can serve", race(cleanAttempt()), failedAttempt(TerminalCapabilityRefused), limits.ModelOutcome, true, 256, 256, false},
		{"winner stalled after content", race(stalled), stalled, limits.DecodeStalled, true, 230.4, 128, false},
		{"content produced, past the exemption, nonce unfinished", race(longResponse), longResponse, limits.ModelOutcome, false, 256, 256, false},
		{"empty stream that held the request past the refusal point without finishing its nonce", race(heldEmpty), heldEmpty, limits.EmptyAnswerLeftOpen, true, 128, 128, true},
		{"empty stream that finished its nonce and held the request past the refusal point", race(heldFinishedEmpty), heldFinishedEmpty, limits.EmptyAnswer, true, 179.2, 179.2, false},
		{"empty stream that burned tokens and held the request", race(heldBurnEmpty), heldBurnEmpty, limits.Overload, true, 217.6, 217.6, false},
		{"empty stream that burned tokens one millisecond inside the refusal point", race(briefBurnEmpty), briefBurnEmpty, limits.ModelOutcome, true, 256, 256, false},
		{"empty stream that held the request while the PoC bypass is active", func() RaceOutcome {
			outcome := race(heldEmpty)
			outcome.PoCBypassActive = true
			return outcome
		}(), heldEmpty, limits.ModelOutcome, false, 256, 256, false},
		{"empty stream while the PoC bypass is active", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalEmptyStream))
			outcome.PoCBypassActive = true
			return outcome
		}(), failedAttempt(TerminalEmptyStream), limits.ModelOutcome, false, 256, 256, false},
		{"attempt aborted by a phase transition", race(cleanAttempt()), func() AttemptOutcome {
			attempt := failedAttempt(TerminalEmptyStream)
			attempt.PhaseTransitionAborted = true
			return attempt
		}(), limits.ModelOutcome, false, 256, 256, false},
		{"escrow not found", func() RaceOutcome {
			outcome := race(failedAttempt(TerminalRejected))
			outcome.Lifecycle.EscrowMissing = true
			return outcome
		}(), failedAttempt(TerminalRejected), limits.ModelOutcome, false, 256, 256, false},
		{"post-state-root divergence", race(cleanAttempt()), func() AttemptOutcome {
			attempt := cleanAttempt()
			attempt.StateDivergent = true
			return attempt
		}(), limits.ModelOutcome, false, 256, 256, false},
		{"clean finish with the nonce still unfinished", race(unfinished), unfinished, limits.ModelOutcome, false, 256, 256, false},
		{"client cancelled", race(cleanAttempt()), failedAttempt(TerminalClientCancelled), limits.ModelOutcome, false, 256, 256, false},
		{"send returned without a receipt", race(cleanAttempt()), failedAttempt(TerminalNoReceipt), limits.ModelOutcome, false, 256, 256, false},
		{"unclassified", race(cleanAttempt()), failedAttempt(TerminalUnclassified), limits.ModelOutcome, false, 256, 256, false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verdict, recorded := testCase.outcome.Verdict(testCase.attempt)
			if verdict != testCase.wantVerdict || recorded != testCase.wantRecorded {
				t.Fatalf("Verdict() = (%v, %v), want (%v, %v)", verdict, recorded, testCase.wantVerdict, testCase.wantRecorded)
			}
			assertWindowsAndCutoff(t, verdict, recorded, testCase.wantInputWindow, testCase.wantOutputWindow, testCase.wantCutoff)
		})
	}
}

// A deadline is judged while the attempt still runs, so these two verdicts reach the limiter with no
// upstream condition to classify: the stage the deadline belongs to is the whole of what they carry.
func TestMissedDeadlineVerdictTable(t *testing.T) {
	tests := []struct {
		name             string
		stage            EscalationStage
		wantVerdict      limits.Verdict
		wantInputWindow  float64
		wantOutputWindow float64
		wantCutoff       bool
	}{
		{"receipt deadline missed", StageReceiptTimeout, limits.MissedReceiptDeadline, 128, 128, false},
		{"first-token deadline missed", StageFirstToken, limits.MissedFirstTokenDeadline, 128, 230.4, false},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			verdict := missedDeadlineVerdict(testCase.stage)
			if verdict != testCase.wantVerdict {
				t.Fatalf("missedDeadlineVerdict() = %v, want %v", verdict, testCase.wantVerdict)
			}
			assertWindowsAndCutoff(t, verdict, true, testCase.wantInputWindow, testCase.wantOutputWindow, testCase.wantCutoff)
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

// The two penalties are independent: crown denial keeps the host off the client's answer, the cutoff
// keeps it off the escrow's nonces, and neither moves the congestion windows.
func TestEmptyStreamWithAnUnfinishedNonceDeniesCrowningAndOpensTheCutoff(t *testing.T) {
	t.Parallel()

	attempt := failedAttempt(TerminalEmptyStream)
	outcome := race(attempt)

	verdict, recorded := outcome.Verdict(attempt)
	if verdict != limits.EmptyAnswerLeftOpen || !recorded {
		t.Fatalf("Verdict() = (%v, %v), want (EmptyAnswerLeftOpen, true)", verdict, recorded)
	}
	input, output := observedWindows(t, verdict, recorded)
	if math.Abs(input-128) > windowTolerance || math.Abs(output-128) > windowTolerance {
		t.Fatalf("windows after an empty stream = %v/%v, want 128/128: a nonce left open narrows both severely", input, output)
	}
	if !observedCutoffOpen(verdict, recorded) {
		t.Fatal("cutoff after an empty stream = closed, want open")
	}
	if !outcome.DeniesCrowning(attempt) {
		t.Fatal("DeniesCrowning() = false, want true")
	}
}

func TestDeniesCrowning(t *testing.T) {
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
			Outcome: "success", Visibility: "no_winner",
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
	for terminal := TerminalUnclassified; terminal <= TerminalHardTimeout; terminal++ {
		if terminal.String() == "" {
			t.Fatalf("terminal %d has no name", terminal)
		}
		if terminal.String() == TerminalNameUnnamed || terminal.String() == ReasonUnknown {
			t.Fatalf("terminal %d fell through to a placeholder, so a new terminal was added without a name", terminal)
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

// Prefill must not be charged to decode speed; first-content already measures it.
func TestTimePerOutputTokenMeasuresTheDecodeWindowAlone(t *testing.T) {
	t.Parallel()
	attempt := cleanAttempt()
	attempt.FirstContent = testEpoch.Add(5 * time.Second)
	attempt.LastChunk = testEpoch.Add(15 * time.Second)
	attempt.UsageCompletionTokens = 100

	if got, want := TimePerOutputToken(attempt), 100*time.Millisecond; got != want {
		t.Fatalf("TimePerOutputToken() = %v, want %v", got, want)
	}
}

// A missing input leaves the measure unreported rather than wrong.
func TestTimePerOutputTokenIsUnreportedWithoutAMeasurableWindow(t *testing.T) {
	t.Parallel()
	measurable := cleanAttempt()
	measurable.FirstContent = testEpoch.Add(5 * time.Second)
	measurable.LastChunk = testEpoch.Add(15 * time.Second)
	measurable.UsageCompletionTokens = 100

	cases := []struct {
		name   string
		mutate func(*AttemptOutcome)
	}{
		{"the host reported no completion tokens", func(a *AttemptOutcome) { a.UsageCompletionTokens = 0 }},
		{"content never arrived", func(a *AttemptOutcome) { a.FirstContent = time.Time{} }},
		{"the answer was one chunk wide", func(a *AttemptOutcome) { a.LastChunk = a.FirstContent }},
		{"the last chunk predates the first", func(a *AttemptOutcome) { a.LastChunk = testEpoch }},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			attempt := measurable
			testCase.mutate(&attempt)

			if got := TimePerOutputToken(attempt); got != 0 {
				t.Fatalf("TimePerOutputToken() = %v, want 0", got)
			}
		})
	}
}

// The sample is the only route from a finished attempt to the tracker.
func TestSampleCarriesTheDecodeMeasure(t *testing.T) {
	t.Parallel()
	attempt := cleanAttempt()
	attempt.FirstContent = testEpoch.Add(time.Second)
	attempt.LastChunk = testEpoch.Add(3 * time.Second)
	attempt.UsageCompletionTokens = 40

	sample, exemption := race(attempt).Sample(attempt)

	if exemption != SampleRecorded {
		t.Fatalf("Sample() exemption = %v, want it recorded", exemption)
	}
	if got, want := sample.TimePerOutputToken, 50*time.Millisecond; got != want {
		t.Fatalf("sample.TimePerOutputToken = %v, want %v", got, want)
	}
}

// A strike is cleared by a host that answered, not by one that never got the chance. Judging every
// terminal lets a host alternate empty streams with dial failures and never reach the threshold.
func TestOnlyAnAnswerOrAnEmptyStreamJudgesCrowning(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name    string
		attempt AttemptOutcome
		want    bool
	}{
		{name: "answered with content", attempt: cleanAttempt(), want: true},
		{name: "claimed to serve and produced none", attempt: failedAttempt(TerminalEmptyStream), want: true},
		{name: "never reached the host", attempt: failedAttempt(TerminalNoReceipt), want: false},
		{name: "the client left", attempt: failedAttempt(TerminalClientCancelled), want: false},
		{name: "won but never confirmed", attempt: failedAttempt(TerminalWon), want: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if got := race(testCase.attempt).JudgesCrowning(testCase.attempt); got != testCase.want {
				t.Fatalf("JudgesCrowning() = %v, want %v", got, testCase.want)
			}
		})
	}
}

// A host that never answered must keep the strikes it earned. Reporting every attempt clears them on a
// dial failure, so a host alternating empty streams with failures never reaches the threshold.
func TestARaceTheHostNeverAnsweredLeavesItsStrikesAlone(t *testing.T) {
	t.Parallel()
	crown := newCrownStrikes(nil)
	for range crownDenialStrikes - 1 {
		crown.Observe(testParticipant, testModel, true)
	}

	race(failedAttempt(TerminalNoReceipt)).observeCrowning(crown)
	crown.Observe(testParticipant, testModel, true)

	if !crown.Denied(testParticipant, testModel) {
		t.Fatal("the dial failure cleared the strikes the host had already earned")
	}
}

// The oversize cap is the gateway's own buffer limit, so an attempt it kills says nothing about the
// host's transport and must not open its cutoff.
func TestResponseTooLargeIsNotChargedToTheHostAsATransportFault(t *testing.T) {
	t.Parallel()

	attempt := failedAttempt(TerminalResponseTooLarge)
	outcome := race(attempt)

	verdict, recorded := outcome.Verdict(attempt)
	if verdict != limits.ModelOutcome {
		t.Fatalf("Verdict() = (%v, %v), want (ModelOutcome, _)", verdict, recorded)
	}
}

// A 5xx is the host's upstream failing while the host itself answers, so the window should narrow for it.
// A 4xx, and a 5xx that names a fault in what the gateway sent, must not move the host's window at all.
func TestUpstreamServerErrorsNarrowTheWindowAndClientErrorsDoNot(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name         string
		status       int
		body         string
		wantTerminal Terminal
		wantVerdict  limits.Verdict
		wantMoves    bool
	}{
		{name: "500", status: 500, wantTerminal: TerminalUpstreamServerError, wantVerdict: limits.UpstreamFault, wantMoves: true},
		{name: "502", status: 502, wantTerminal: TerminalUpstreamServerError, wantVerdict: limits.UpstreamFault, wantMoves: true},
		{name: "504", status: 504, wantTerminal: TerminalUpstreamServerError, wantVerdict: limits.UpstreamFault, wantMoves: true},
		{name: "400", status: 400, wantTerminal: TerminalRejected, wantVerdict: limits.ModelOutcome, wantMoves: false},
		{
			name: "500 on an escrow we never opened", status: 500, body: bridge.ErrEscrowNotFound.Error(),
			wantTerminal: TerminalRejected, wantVerdict: limits.ModelOutcome,
		},
		{
			name: "500 on a nonce we allocated past the cap", status: 500,
			body:         types.ErrNonceLimitExceeded.Error() + ": nonce 21 exceeds active cap 20",
			wantTerminal: TerminalRejected, wantVerdict: limits.ModelOutcome,
		},
		{
			name: "500 on a payload we mismatched", status: 500, body: types.ErrPromptHashMismatch.Error(),
			wantTerminal: TerminalRejected, wantVerdict: limits.ModelOutcome,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := &transport.UpstreamStatusError{Path: "/v1/chat/completions", StatusCode: testCase.status, Body: testCase.body}

			terminal := classifyDispatchError(context.Background(), err)
			if terminal != testCase.wantTerminal {
				t.Fatalf("status %d classified as %v, want %v", testCase.status, terminal, testCase.wantTerminal)
			}

			attempt := failedAttempt(terminal)
			verdict, moves := race(attempt).Verdict(attempt)
			if verdict != testCase.wantVerdict || moves != testCase.wantMoves {
				t.Fatalf("Verdict() = (%v, %v), want (%v, %v)",
					verdict, moves, testCase.wantVerdict, testCase.wantMoves)
			}
		})
	}
}
