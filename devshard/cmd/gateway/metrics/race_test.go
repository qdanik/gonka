package metrics

import (
	"testing"
	"time"

	"devshard/cmd/gateway/engine"
)

var raceStart = time.Unix(1700000000, 0)

func at(offset time.Duration) time.Time { return raceStart.Add(offset) }

func winningAttempt() engine.AttemptOutcome {
	return engine.AttemptOutcome{
		Participant:   "gonka1winner",
		Nonce:         11,
		Role:          "primary",
		StartReason:   "primary",
		SendTime:      at(0),
		ReceiptTime:   at(200 * time.Millisecond),
		FirstToken:    at(500 * time.Millisecond),
		FirstContent:  at(600 * time.Millisecond),
		Completed:     at(2 * time.Second),
		ContentChunks: 4,
		Terminal:      engine.TerminalWon,
		Confirmed:     true,
		NonceFinished: true,
	}
}

func TestAWonRaceEmitsTheWinnerFamiliesWithTheirValues(t *testing.T) {
	telemetry := New()
	recorder := NewRaceRecorder(telemetry)

	recorder.RecordRace(engine.RaceOutcome{
		Model:       "qwen",
		InputTokens: 100,
		Decision:    "primary",
		WinnerNonce: 11,
		Succeeded:   true,
		Attempts:    []engine.AttemptOutcome{winningAttempt()},
	})

	expectCounter(t, telemetry, "devshard_gateway_attempts_started_total",
		labels{"participant_key": "gonka1winner", "model": "qwen", "role": "primary", "reason": "primary"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_attempts_terminal_total",
		labels{"participant_key": "gonka1winner", "model": "qwen", "role": "primary", "outcome": "success", "visibility": "user_visible_winner"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_user_visible_wins_total",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_requests_total",
		labels{"model": "qwen", "outcome": "success", "reason": "none"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_escalation_decisions_total", labels{"reason": "primary"}, 1)
	expectAbsent(t, telemetry, "devshard_gateway_attempt_failures_total")
	expectAbsent(t, telemetry, "devshard_gateway_critical_user_failures_total")
	expectAbsent(t, telemetry, "devshard_gateway_user_requests_with_hidden_failure_total")

	expectHistogram(t, telemetry, "devshard_gateway_participant_receipt_seconds",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 0.2)
	expectHistogram(t, telemetry, "devshard_gateway_participant_first_content_seconds",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 0.6)
	expectHistogram(t, telemetry, "devshard_gateway_participant_prefill_seconds_per_input_token",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 0.004)
	expectHistogram(t, telemetry, "devshard_gateway_participant_total_attempt_seconds",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 2)
}

// A role-only chunk is a token, not content: charging its arrival to the content metrics reports a
// prefill no client ever waited for.
func TestAContentlessStreamLeavesTheContentLatenciesUnobserved(t *testing.T) {
	telemetry := New()
	recorder := NewRaceRecorder(telemetry)

	attempt := winningAttempt()
	attempt.FirstContent = time.Time{}
	recorder.RecordRace(engine.RaceOutcome{
		Model: "qwen", InputTokens: 100, Decision: "primary", WinnerNonce: 11,
		Succeeded: true, Attempts: []engine.AttemptOutcome{attempt},
	})

	expectAbsent(t, telemetry, "devshard_gateway_participant_first_content_seconds")
	expectAbsent(t, telemetry, "devshard_gateway_participant_prefill_seconds_per_input_token")
	expectHistogram(t, telemetry, "devshard_gateway_participant_receipt_seconds",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 0.2)
}

// A stalled host and a slow one carry the same chunk count; the longest silence is what parts them.
func TestAStalledAttemptReportsItsLongestSilence(t *testing.T) {
	telemetry := New()
	recorder := NewRaceRecorder(telemetry)

	attempt := winningAttempt()
	attempt.MaxChunkGap, attempt.MeanChunkGap = 55*time.Second, 40*time.Millisecond
	recorder.RecordRace(engine.RaceOutcome{
		Model: "qwen", InputTokens: 100, Decision: "primary", WinnerNonce: 11,
		Succeeded: true, Attempts: []engine.AttemptOutcome{attempt},
	})

	expectHistogram(t, telemetry, "devshard_gateway_participant_max_inter_chunk_seconds",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 55)
	expectHistogram(t, telemetry, "devshard_gateway_participant_inter_chunk_seconds",
		labels{"participant_key": "gonka1winner", "model": "qwen"}, 1, 0.04)
}

func TestAHiddenLoserFailureIsCountedAgainstASuccessfulRequest(t *testing.T) {
	telemetry := New()
	recorder := NewRaceRecorder(telemetry)

	recorder.RecordRace(engine.RaceOutcome{
		Model:       "qwen",
		InputTokens: 100,
		Decision:    "primary_slow",
		WinnerNonce: 11,
		Succeeded:   true,
		Attempts: []engine.AttemptOutcome{
			winningAttempt(),
			{
				Participant: "gonka1loser",
				Nonce:       12,
				Role:        "extra",
				StartReason: "primary_slow",
				SendTime:    at(300 * time.Millisecond),
				Completed:   at(1 * time.Second),
				Terminal:    engine.TerminalUnavailable,
			},
		},
	})

	expectCounter(t, telemetry, "devshard_gateway_attempt_failures_total",
		labels{"participant_key": "gonka1loser", "model": "qwen", "role": "extra", "reason": "http_503", "visibility": "failed_not_finished"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_participant_transport_errors_total",
		labels{"participant_key": "gonka1loser", "model": "qwen", "path_kind": "inference", "status": "503"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_user_requests_with_hidden_failure_total",
		labels{"model": "qwen", "severity": "protected", "reason": "http_503"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_requests_total",
		labels{"model": "qwen", "outcome": "success", "reason": "none"}, 1)
	expectAbsent(t, telemetry, "devshard_gateway_critical_user_failures_total")
}

func TestASuspiciousAttemptIsCountedAsNoWinner(t *testing.T) {
	telemetry := New()
	recorder := NewRaceRecorder(telemetry)

	recorder.RecordRace(engine.RaceOutcome{
		Model:    "qwen",
		Decision: "primary",
		Attempts: []engine.AttemptOutcome{{
			Participant:   "gonka1quiet",
			Nonce:         21,
			Role:          "primary",
			SendTime:      at(0),
			Completed:     at(time.Second),
			Suspicious:    true,
			Terminal:      engine.TerminalEmptyStream,
			Confirmed:     true,
			NonceFinished: true,
		}},
	})

	expectCounter(t, telemetry, "devshard_gateway_no_winner_attempts_total",
		labels{"participant_key": "gonka1quiet", "model": "qwen", "reason": "empty_stream"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_requests_total",
		labels{"model": "qwen", "outcome": "failure", "reason": "empty_stream"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_critical_user_failures_total",
		labels{"model": "qwen", "reason": "empty_stream"}, 1)
	expectAbsent(t, telemetry, "devshard_gateway_user_visible_wins_total")
	expectAbsent(t, telemetry, "devshard_gateway_participant_transport_errors_total")
}

func TestALifecycleFailureNamesItselfRatherThanAnAttempt(t *testing.T) {
	testCases := []struct {
		name     string
		outcome  engine.RaceOutcome
		expected string
	}{
		{
			name:     "no attempt was ever dispatched",
			outcome:  engine.RaceOutcome{Model: "qwen", Decision: "primary"},
			expected: "no_attempts",
		},
		{
			name:     "the escrow disappeared",
			outcome:  engine.RaceOutcome{Model: "qwen", Decision: "primary", Lifecycle: engine.Lifecycle{EscrowMissing: true}},
			expected: "escrow_missing",
		},
		{
			name:     "the escrow ran out of funds",
			outcome:  engine.RaceOutcome{Model: "qwen", Decision: "primary", Lifecycle: engine.Lifecycle{BalanceExhausted: true}},
			expected: "balance_exhausted",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			telemetry := New()
			NewRaceRecorder(telemetry).RecordRace(testCase.outcome)

			expectCounter(t, telemetry, "devshard_gateway_requests_total",
				labels{"model": "qwen", "outcome": "failure", "reason": testCase.expected}, 1)
			expectCounter(t, telemetry, "devshard_gateway_critical_user_failures_total",
				labels{"model": "qwen", "reason": testCase.expected}, 1)
		})
	}
}

func TestAnUndispatchedAttemptIsNotCountedAsStarted(t *testing.T) {
	telemetry := New()
	NewRaceRecorder(telemetry).RecordRace(engine.RaceOutcome{
		Model:    "qwen",
		Decision: "primary",
		Attempts: []engine.AttemptOutcome{{Participant: "gonka1ghost", Role: "extra", Terminal: engine.TerminalOffPath}},
	})

	expectAbsent(t, telemetry, "devshard_gateway_attempts_started_total")
	expectCounter(t, telemetry, "devshard_gateway_attempts_terminal_total",
		labels{"participant_key": "gonka1ghost", "model": "qwen", "role": "extra", "outcome": "failed", "visibility": "failed_not_finished"}, 1)
}

func TestEmptyLabelsFallBackRatherThanShippingBlank(t *testing.T) {
	telemetry := New()
	NewRaceRecorder(telemetry).RecordRace(engine.RaceOutcome{
		Attempts: []engine.AttemptOutcome{{SendTime: at(0), Terminal: engine.TerminalDialFailure}},
	})

	expectCounter(t, telemetry, "devshard_gateway_attempts_started_total",
		labels{"participant_key": "unknown", "model": "unknown", "role": "primary", "reason": "primary"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_escalation_decisions_total", labels{"reason": "unknown"}, 1)
	expectCounter(t, telemetry, "devshard_gateway_participant_transport_errors_total",
		labels{"participant_key": "unknown", "model": "unknown", "path_kind": "inference", "status": "0"}, 1)
}

func TestATimeoutVoteIsCountedOnceItIsResolved(t *testing.T) {
	testCases := []struct {
		name             string
		event            engine.TimeoutEvent
		expectedTimeouts float64
	}{
		{
			name:             "a posted vote",
			event:            engine.TimeoutEvent{Participant: "gonka1host", Model: "qwen", Kind: "refused", Action: "completed", Reason: "refused"},
			expectedTimeouts: 1,
		},
		{
			name:             "a failed vote",
			event:            engine.TimeoutEvent{Participant: "gonka1host", Model: "qwen", Kind: "execution", Action: "failed", Reason: "timeout_collection_error"},
			expectedTimeouts: 1,
		},
		{
			name:             "a vote that was never attempted",
			event:            engine.TimeoutEvent{Participant: "gonka1host", Model: "qwen", Kind: "refused", Action: "skipped", Reason: "nonce_already_finished"},
			expectedTimeouts: 0,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			telemetry := New()
			NewRaceRecorder(telemetry).RecordTimeout(testCase.event)

			expectCounter(t, telemetry, "devshard_gateway_timeout_actions_total", labels{
				"participant_key": testCase.event.Participant,
				"model":           testCase.event.Model,
				"kind":            testCase.event.Kind,
				"action":          testCase.event.Action,
				"reason":          testCase.event.Reason,
			}, 1)
			if testCase.expectedTimeouts == 0 {
				expectAbsent(t, telemetry, "devshard_inference_timeouts_total")
				return
			}
			expectCounter(t, telemetry, "devshard_inference_timeouts_total", labels{"reason": testCase.event.Reason}, testCase.expectedTimeouts)
		})
	}
}

func TestAClassifyOverflowIsAttributedToItsHost(t *testing.T) {
	telemetry := New()
	NewRaceRecorder(telemetry).RecordClassifyOverflow("gonka1host", "qwen")

	expectCounter(t, telemetry, "devshard_gateway_stream_carry_overflow_total",
		labels{"participant_key": "gonka1host", "model": "qwen"}, 1)
}

// The engine's hook is satisfied structurally; a signature drift must fail the build, not a scrape.
var _ interface {
	RecordRace(engine.RaceOutcome)
	RecordTimeout(engine.TimeoutEvent)
	RecordClassifyOverflow(participant, model string)
} = (*RaceRecorder)(nil)

// The label values below reach dashboards verbatim. Changing one empties the panel that reads it
// without failing a build or a query, so the wire strings are pinned here rather than inferred.
func TestEmittedLabelValuesMatchTheirWireStrings(t *testing.T) {
	pinned := []struct{ emitted, want string }{
		{engine.AttemptOutcomeSuccess, "success"},
		{engine.AttemptOutcomeFailed, "failed"},
		{engine.VisibilityWinner, "user_visible_winner"},
		{engine.VisibilityNoWinner, "no_winner"},
		{engine.VisibilitySuppressedLoser, "suppressed_loser"},
		{engine.VisibilityFailedNotFinished, "failed_not_finished"},
		{engine.RolePrimary, "primary"},
		{engine.RoleSpeculative, "speculative"},
		{engine.TimeoutActionSkipped, "skipped"},
		{engine.TimeoutActionStarted, "started"},
		{engine.TimeoutActionCompleted, "completed"},
		{engine.TimeoutActionFailed, "failed"},
		{outcomeFailure, "failure"},
	}
	for _, label := range pinned {
		if label.emitted != label.want {
			t.Errorf("label value %q no longer matches the string dashboards read, %q", label.emitted, label.want)
		}
	}
}
