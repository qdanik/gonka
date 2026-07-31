package engine

import "testing"

type contextLimitCall struct {
	participant string
	maxTokens   uint64
}

type stubCapabilityRecorder struct {
	contextLimits    []contextLimitCall
	toolsUnsupported []string
}

func (r *stubCapabilityRecorder) RecordContextLimit(participant string, maxTokens uint64) {
	r.contextLimits = append(r.contextLimits, contextLimitCall{participant: participant, maxTokens: maxTokens})
}

func (r *stubCapabilityRecorder) RecordToolUnsupported(participant string) {
	r.toolsUnsupported = append(r.toolsUnsupported, participant)
}

const (
	vllmContextLengthMessage = "This model's maximum context length is 131072 tokens. However, you requested 140000 tokens (12000 in the messages, 128000 in the completion). Please reduce the length of the messages or completion."
	vllmContextTotalMessage  = "This model's maximum context length is 40960 tokens. However, you requested 41200 tokens (1200 in the messages, 40000 in the completion), for a total of at least 41200 tokens."
	vllmToolChoiceMessage    = "tool choice requires --enable-auto-tool-choice and --tool-call-parser to be set"
)

// The host controls this text, and U+212A lowers to a one-byte k: a phrase index taken from the
// lowered message lands two bytes early in the original.
func TestParseCapabilityError(t *testing.T) {
	testCases := []struct {
		name    string
		message string
		want    CapabilitySignal
	}{
		{
			name:    "maximum_context_length",
			message: vllmContextLengthMessage,
			want:    CapabilitySignal{ContextLimit: 131072},
		},
		{
			name:    "total_of_at_least",
			message: vllmContextTotalMessage,
			want:    CapabilitySignal{ContextLimit: 40960, ContextRequested: 41200},
		},
		{
			name:    "tool_choice_unsupported",
			message: vllmToolChoiceMessage,
			want:    CapabilitySignal{ToolsUnsupported: true},
		},
		{
			name:    "a_rune_that_shrinks_when_lowercased_does_not_shift_the_digits",
			message: "Kontext: maximum context length is 131072 tokens",
			want:    CapabilitySignal{ContextLimit: 131072},
		},
		{
			name:    "limit_at_end_of_message",
			message: "This model's maximum context length is 8192",
			want:    CapabilitySignal{ContextLimit: 8192},
		},
		{
			name:    "case_insensitive_phrase",
			message: "Maximum Context Length Is 4096 tokens",
			want:    CapabilitySignal{ContextLimit: 4096},
		},
		{
			name:    "lookalike_without_a_number",
			message: "This model's maximum context length is exceeded by your request",
			want:    CapabilitySignal{},
		},
		{
			name:    "lookalike_total_without_a_number",
			message: "you requested too much, for a total of at least several thousand tokens",
			want:    CapabilitySignal{},
		},
		{
			name:    "unrelated_error",
			message: "the model is currently loading",
			want:    CapabilitySignal{},
		},
		{
			name:    "empty_message",
			message: "",
			want:    CapabilitySignal{},
		},
		{
			name:    "overflowing_number",
			message: "maximum context length is 99999999999999999999999 tokens",
			want:    CapabilitySignal{},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := ParseCapabilityError(testCase.message); got != testCase.want {
				t.Fatalf("signal = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestRetriableCapabilitySignal(t *testing.T) {
	testCases := []struct {
		name   string
		signal CapabilitySignal
		want   bool
	}{
		{name: "context_limit", signal: CapabilitySignal{ContextLimit: 8192}, want: true},
		{name: "tools_unsupported", signal: CapabilitySignal{ToolsUnsupported: true}, want: true},
		{name: "requested_total_alone", signal: CapabilitySignal{ContextRequested: 41200}},
		{name: "nothing_parsed", signal: CapabilitySignal{}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := testCase.signal.Retriable(); got != testCase.want {
				t.Fatalf("retriable = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestRecordCapability(t *testing.T) {
	t.Run("context_limit_is_recorded_with_its_parsed_value", func(t *testing.T) {
		t.Parallel()
		recorder := &stubCapabilityRecorder{}

		RecordCapability(recorder, testParticipant, ParseCapabilityError(vllmContextLengthMessage))

		if len(recorder.contextLimits) != 1 {
			t.Fatalf("context limits = %+v, want one", recorder.contextLimits)
		}
		if recorder.contextLimits[0] != (contextLimitCall{participant: testParticipant, maxTokens: 131072}) {
			t.Fatalf("context limit = %+v, want 131072 for %s", recorder.contextLimits[0], testParticipant)
		}
		if len(recorder.toolsUnsupported) != 0 {
			t.Fatalf("tools unsupported = %+v, want none", recorder.toolsUnsupported)
		}
	})

	t.Run("tool_choice_is_recorded_without_a_context_limit", func(t *testing.T) {
		t.Parallel()
		recorder := &stubCapabilityRecorder{}

		RecordCapability(recorder, testParticipant, ParseCapabilityError(vllmToolChoiceMessage))

		if len(recorder.toolsUnsupported) != 1 || recorder.toolsUnsupported[0] != testParticipant {
			t.Fatalf("tools unsupported = %+v, want %s", recorder.toolsUnsupported, testParticipant)
		}
		if len(recorder.contextLimits) != 0 {
			t.Fatalf("context limits = %+v, want none", recorder.contextLimits)
		}
	})

	t.Run("a_lookalike_phrase_records_nothing", func(t *testing.T) {
		t.Parallel()
		recorder := &stubCapabilityRecorder{}

		RecordCapability(recorder, testParticipant, ParseCapabilityError("this model's maximum context length is exceeded"))

		if len(recorder.contextLimits) != 0 || len(recorder.toolsUnsupported) != 0 {
			t.Fatalf("recorder = %+v, want nothing recorded", recorder)
		}
	})

	t.Run("an_unnamed_participant_records_nothing", func(t *testing.T) {
		t.Parallel()
		recorder := &stubCapabilityRecorder{}

		RecordCapability(recorder, "", ParseCapabilityError(vllmContextLengthMessage))

		if len(recorder.contextLimits) != 0 {
			t.Fatalf("context limits = %+v, want none", recorder.contextLimits)
		}
	})
}

func TestCapabilityOfAttempt(t *testing.T) {
	errorStream := failedAttempt(TerminalCapabilityRefused)
	errorStream.ErrorSource = "sse_error"
	errorStream.ErrorMessage = vllmContextLengthMessage

	noErrorSource := errorStream
	noErrorSource.ErrorSource = ""

	testCases := []struct {
		name    string
		attempt AttemptOutcome
		want    CapabilitySignal
	}{
		{name: "error_stream_attempt", attempt: errorStream, want: CapabilitySignal{ContextLimit: 131072}},
		{name: "attempt_without_an_error_event_is_ignored", attempt: noErrorSource},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := CapabilityOf(testCase.attempt); got != testCase.want {
				t.Fatalf("signal = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

// The hint the next Pick screens hosts against grows only from what the host said the request
// needs, never from what the host said it can serve.
func TestContextHintGrowsAcrossARetry(t *testing.T) {
	firstAttempt := failedAttempt(TerminalCapabilityRefused)
	firstAttempt.ErrorSource = "sse_error"
	firstAttempt.ErrorMessage = vllmContextTotalMessage

	hint := uint64(1024)
	hint = GrowContextHint(hint, CapabilityOf(firstAttempt))
	if hint != 41200 {
		t.Fatalf("hint after the first refusal = %d, want 41200", hint)
	}

	secondAttempt := firstAttempt
	secondAttempt.ErrorMessage = "This model's maximum context length is 8192 tokens. However, you requested 41200 tokens, for a total of at least 9000 tokens."
	hint = GrowContextHint(hint, CapabilityOf(secondAttempt))
	if hint != 41200 {
		t.Fatalf("hint after a smaller refusal = %d, want it held at 41200", hint)
	}

	thirdAttempt := firstAttempt
	thirdAttempt.ErrorMessage = "for a total of at least 60000 tokens"
	if hint = GrowContextHint(hint, CapabilityOf(thirdAttempt)); hint != 60000 {
		t.Fatalf("hint after a larger refusal = %d, want 60000", hint)
	}
}

func TestContextHintIgnoresANonCapabilityFailure(t *testing.T) {
	attempt := failedAttempt(TerminalDialFailure)
	attempt.ErrorMessage = "connection reset by peer"

	if hint := GrowContextHint(4096, CapabilityOf(attempt)); hint != 4096 {
		t.Fatalf("hint = %d, want it unchanged at 4096", hint)
	}
}
