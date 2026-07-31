package engine

import (
	"testing"
	"time"

	"devshard/cmd/gateway/config"
)

var (
	raceStart    = time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	testPolicy   = EscalationPolicyFromConfig(config.Defaults().Engine)
	streaming    = EscalationRequest{InputTokens: 1_000, Stream: true}
	nonStreaming = EscalationRequest{InputTokens: 1_000, Stream: false}

	nonStreamingWithReducedTokens = EscalationRequest{InputTokens: 1_000, Stream: false, ReducedTokensStillOffered: true}
)

func dispatched(offset time.Duration) EscalationAttempt {
	return EscalationAttempt{SendTime: raceStart.Add(offset)}
}

func TestEscalationPolicyFromConfigConvertsEveryTunable(t *testing.T) {
	policy := EscalationPolicyFromConfig(config.Engine{
		ReceiptTimeoutMS:           1_500,
		FirstTokenFloorMS:          250,
		InterChunkStallMS:          7_000,
		LoserGraceMS:               90_000,
		NonStreamResponseFloorMS:   11_000,
		PerInputTokenResponseLagMS: 13,
		MaxSpeculativeAttempts:     4,
	})
	want := EscalationPolicy{
		ReceiptTimeout:           1_500 * time.Millisecond,
		FirstTokenFloor:          250 * time.Millisecond,
		InterChunkStall:          7 * time.Second,
		LoserGrace:               90 * time.Second,
		NonStreamResponseFloor:   11 * time.Second,
		PerInputTokenResponseLag: 13 * time.Millisecond,
		MaxSpeculativeAttempts:   4,
	}
	if policy != want {
		t.Fatalf("EscalationPolicyFromConfig = %+v, want %+v", policy, want)
	}
}

func TestDecideStartsOneAttemptUnlessThePrimaryIsDistrusted(t *testing.T) {
	testCases := []struct {
		name              string
		budget            int
		primarySuspicious bool
		primaryDegraded   bool
		want              StartPlan
	}{
		{
			name:   "healthy primary starts alone and waits for the ladder",
			budget: 4,
			want:   StartPlan{ImmediateAttempts: 1, Reason: StartReceiptTimeout},
		},
		{
			name:              "suspicious primary starts a second attempt at once",
			budget:            2,
			primarySuspicious: true,
			want:              StartPlan{ImmediateAttempts: 2, Reason: StartPrimarySuspicious},
		},
		{
			name:              "suspicious primary with no budget for a second attempt still waits",
			budget:            1,
			primarySuspicious: true,
			want:              StartPlan{ImmediateAttempts: 1, Reason: StartReceiptTimeout},
		},
		{
			name:            "degraded primary starts a second attempt at once",
			budget:          2,
			primaryDegraded: true,
			want:            StartPlan{ImmediateAttempts: 2, Reason: StartPrimaryDegraded},
		},
		{
			name:            "degraded primary with no budget for a second attempt still waits",
			budget:          1,
			primaryDegraded: true,
			want:            StartPlan{ImmediateAttempts: 1, Reason: StartReceiptTimeout},
		},
		{
			name:              "a primary that is both reports the crown denial it was pinned for",
			budget:            2,
			primarySuspicious: true,
			primaryDegraded:   true,
			want:              StartPlan{ImmediateAttempts: 2, Reason: StartPrimarySuspicious},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := testPolicy.Decide(testCase.budget, testCase.primarySuspicious, testCase.primaryDegraded)
			if got != testCase.want {
				t.Fatalf("Decide = %+v, want %+v", got, testCase.want)
			}
		})
	}
}

func TestAttemptBudgetClampsToOneWhenNoncesAreScarce(t *testing.T) {
	testCases := []struct {
		name           string
		maxSpeculative int
		hostCount      int
		nonceScarce    bool
		want           int
	}{
		{name: "unset knob is bounded by the host group", maxSpeculative: 0, hostCount: 5, want: 5},
		{name: "knob below the host count wins", maxSpeculative: 3, hostCount: 5, want: 3},
		{name: "knob equal to the host count", maxSpeculative: 5, hostCount: 5, want: 5},
		{name: "knob above the host count is clamped", maxSpeculative: 9, hostCount: 5, want: 5},
		{name: "single host allows a single attempt", maxSpeculative: 3, hostCount: 1, want: 1},
		{name: "empty host group still yields one", maxSpeculative: 3, hostCount: 0, want: 1},
		{name: "scarce nonces force one attempt", maxSpeculative: 3, hostCount: 5, nonceScarce: true, want: 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			policy := EscalationPolicy{MaxSpeculativeAttempts: testCase.maxSpeculative}
			got := policy.AttemptBudget(testCase.hostCount, testCase.nonceScarce)
			if got != testCase.want {
				t.Fatalf("AttemptBudget(%d, %t) = %d, want %d",
					testCase.hostCount, testCase.nonceScarce, got, testCase.want)
			}
		})
	}
}

func TestLadderRuleInIsolation(t *testing.T) {
	receiptedNoToken := EscalationAttempt{SendTime: raceStart, ReceiptTime: raceStart.Add(time.Second)}
	testCases := []struct {
		name         string
		attempt      EscalationAttempt
		request      EscalationRequest
		wantStage    EscalationStage
		wantDeadline time.Time
	}{
		{
			name:      "rule 0: an attempt that already escalated never triggers again",
			attempt:   EscalationAttempt{Escalated: true, Suspicious: true, SendTime: raceStart},
			request:   streaming,
			wantStage: StageNone,
		},
		{
			name:         "rule 1: a suspicious attempt escalates immediately",
			attempt:      EscalationAttempt{Suspicious: true, SendTime: raceStart},
			request:      streaming,
			wantStage:    StageSuspicious,
			wantDeadline: raceStart,
		},
		{
			name:      "rule 3: a finished attempt needs no successor",
			attempt:   EscalationAttempt{Done: true, NonceFinished: true, SendTime: raceStart},
			request:   streaming,
			wantStage: StageNone,
		},
		{
			name:      "rule 4: a failed non-streaming attempt waits for the request-level timer",
			attempt:   EscalationAttempt{Done: true, SendTime: raceStart},
			request:   nonStreaming,
			wantStage: StageNone,
		},
		{
			name:         "rule 5: a failed streaming attempt escalates immediately",
			attempt:      EscalationAttempt{Done: true, SendTime: raceStart},
			request:      streaming,
			wantStage:    StageAttemptFailed,
			wantDeadline: raceStart,
		},
		{
			name:      "rule 6: an undispatched attempt has no deadline to measure from",
			attempt:   EscalationAttempt{},
			request:   streaming,
			wantStage: StageNone,
		},
		{
			name:         "rule 7: a dispatched attempt without a receipt waits the receipt timeout",
			attempt:      dispatched(0),
			request:      streaming,
			wantStage:    StageReceiptTimeout,
			wantDeadline: raceStart.Add(5 * time.Second),
		},
		{
			name:         "rule 7 applies to non-streaming requests too",
			attempt:      dispatched(0),
			request:      nonStreaming,
			wantStage:    StageReceiptTimeout,
			wantDeadline: raceStart.Add(5 * time.Second),
		},
		{
			name:         "rule 7 is not shadowed: a non-streaming attempt still owes its receipt first",
			attempt:      dispatched(0),
			request:      nonStreamingWithReducedTokens,
			wantStage:    StageReceiptTimeout,
			wantDeadline: raceStart.Add(5 * time.Second),
		},
		{
			name:      "rule 8: a receipted non-streaming attempt has no first-token stage",
			attempt:   receiptedNoToken,
			request:   nonStreaming,
			wantStage: StageNone,
		},
		{
			name:         "rule 8: a spent fallback is what leaves the non-streaming attempt alone",
			attempt:      receiptedNoToken,
			request:      nonStreamingWithReducedTokens,
			wantStage:    StageReducedMaxTokens,
			wantDeadline: raceStart.Add(20 * time.Second),
		},
		{
			name:      "rule 8: an answer that has started arriving is owed no shorter retry",
			attempt:   EscalationAttempt{SendTime: raceStart, ReceiptTime: raceStart.Add(time.Second), FirstContent: raceStart.Add(2 * time.Second)},
			request:   nonStreamingWithReducedTokens,
			wantStage: StageNone,
		},
		{
			name:      "rule 9: a streaming attempt that produced a token is on its own",
			attempt:   EscalationAttempt{SendTime: raceStart, ReceiptTime: raceStart.Add(time.Second), FirstToken: raceStart.Add(2 * time.Second)},
			request:   streaming,
			wantStage: StageNone,
		},
		{
			name:         "rule 10: a receipted streaming attempt waits the first-token deadline",
			attempt:      receiptedNoToken,
			request:      streaming,
			wantStage:    StageFirstToken,
			wantDeadline: raceStart.Add(1_730_500 * time.Microsecond),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			armed, ok := testPolicy.triggerFor(testCase.attempt, testCase.request, raceStart)
			if testCase.wantStage == StageNone {
				if ok {
					t.Fatalf("triggerFor = %+v, want no trigger", armed)
				}
				return
			}
			if !ok {
				t.Fatalf("triggerFor: want stage %q, got no trigger", testCase.wantStage)
			}
			if armed.Stage != testCase.wantStage {
				t.Fatalf("stage = %q, want %q", armed.Stage, testCase.wantStage)
			}
			if !armed.Deadline.Equal(testCase.wantDeadline) {
				t.Fatalf("deadline = %v, want %v", armed.Deadline, testCase.wantDeadline)
			}
		})
	}
}

func TestReceiptTimeoutDoublesOnlyAboveTheLargeInputBoundary(t *testing.T) {
	testCases := []struct {
		name        string
		inputTokens uint64
		want        time.Duration
	}{
		{"just under the boundary", 99_999, 5 * time.Second},
		{"exactly at the boundary", 100_000, 5 * time.Second},
		{"just over the boundary", 100_001, 10 * time.Second},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testPolicy.receiptTimeout(testCase.inputTokens); got != testCase.want {
				t.Fatalf("receiptTimeout(%d) = %v, want %v", testCase.inputTokens, got, testCase.want)
			}
		})
	}
}

func TestFirstTokenTimeoutHoldsTheFloorAndGrowsWithInput(t *testing.T) {
	floored := EscalationPolicy{FirstTokenFloor: 3_988 * time.Millisecond}
	testCases := []struct {
		name        string
		policy      EscalationPolicy
		inputTokens uint64
		want        time.Duration
	}{
		{"curve just under the floor", floored, 43_000, 3_988 * time.Millisecond},
		{"curve exactly on the floor", floored, 44_000, 3_988 * time.Millisecond},
		{"curve just over the floor", floored, 44_001, 3_988_074 * time.Microsecond},
		{"empty prompt sits on the curve", testPolicy, 0, 1_700 * time.Millisecond},
		{"linear term grows the wait", testPolicy, 1_000, 1_730_500 * time.Microsecond},
		{"quadratic term dominates at a million tokens", testPolicy, 1_000_000, 531_700 * time.Millisecond},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.policy.firstTokenTimeout(testCase.inputTokens); got != testCase.want {
				t.Fatalf("firstTokenTimeout(%d) = %v, want %v", testCase.inputTokens, got, testCase.want)
			}
		})
	}
}

func TestNonStreamResponseTimeoutHoldsTheFloorAndGrowsWithInput(t *testing.T) {
	testCases := []struct {
		name        string
		policy      EscalationPolicy
		inputTokens uint64
		want        time.Duration
	}{
		{"a short prompt sits on the floor", testPolicy, 100, 20 * time.Second},
		{"exactly on the floor", testPolicy, 1_000, 20 * time.Second},
		{"a large prompt grows past the floor", testPolicy, 30_000, 600 * time.Second},
		{"a zero lag leaves the floor alone", EscalationPolicy{NonStreamResponseFloor: 7 * time.Second}, 1_000_000, 7 * time.Second},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.policy.nonStreamResponseTimeout(testCase.inputTokens); got != testCase.want {
				t.Fatalf("nonStreamResponseTimeout(%d) = %v, want %v", testCase.inputTokens, got, testCase.want)
			}
		})
	}
}

func TestNextEscalationPicksTheEarliestDeadline(t *testing.T) {
	attempts := []EscalationAttempt{
		dispatched(0),
		{Escalated: true, SendTime: raceStart.Add(-time.Hour)},
		dispatched(-2 * time.Second),
		{},
	}

	armed, ok := testPolicy.NextEscalation(raceStart, attempts, streaming)

	if !ok {
		t.Fatal("NextEscalation: want a trigger, got none")
	}
	if armed.Attempt != 2 {
		t.Fatalf("Attempt = %d, want 2 (the earliest live deadline)", armed.Attempt)
	}
	if want := raceStart.Add(3 * time.Second); !armed.Deadline.Equal(want) {
		t.Fatalf("Deadline = %v, want %v", armed.Deadline, want)
	}
}

func TestNextEscalationReportsNothingWhenNoAttemptQualifies(t *testing.T) {
	attempts := []EscalationAttempt{
		{Done: true, NonceFinished: true, SendTime: raceStart},
		{Escalated: true, SendTime: raceStart},
	}

	if armed, ok := testPolicy.NextEscalation(raceStart, attempts, streaming); ok {
		t.Fatalf("NextEscalation = %+v, want no trigger", armed)
	}
}

// A timer fires on the state the race is in then, not the state it was armed in: without the
// re-check every primary that receipts under the receipt timeout starts a needless second attempt.
func TestConfirmRejectsAnEscalationWhoseConditionCleared(t *testing.T) {
	attempts := []EscalationAttempt{dispatched(0)}
	armed, ok := testPolicy.NextEscalation(raceStart, attempts, streaming)
	if !ok {
		t.Fatal("NextEscalation on a dispatched attempt: want a trigger, got none")
	}
	fireTime := armed.Deadline

	receipted := []EscalationAttempt{{
		SendTime:    raceStart,
		ReceiptTime: raceStart.Add(400 * time.Millisecond),
		FirstToken:  raceStart.Add(900 * time.Millisecond),
	}}
	if confirmed, ok := testPolicy.Confirm(armed, fireTime, receipted, streaming); ok {
		t.Fatalf("Confirm on a receipted, streaming attempt = %+v, want rejection", confirmed)
	}

	stillSilent := []EscalationAttempt{{SendTime: raceStart, ReceiptTime: raceStart.Add(400 * time.Millisecond)}}
	if confirmed, ok := testPolicy.Confirm(armed, fireTime, stillSilent, streaming); ok {
		t.Fatalf("Confirm after the stage advanced to first-token = %+v, want rejection", confirmed)
	}

	confirmed, ok := testPolicy.Confirm(armed, fireTime, attempts, streaming)
	if !ok {
		t.Fatal("Confirm on an attempt that never receipted: want the escalation, got rejection")
	}
	if confirmed != (ConfirmedEscalation{Attempt: 0, Stage: StageReceiptTimeout}) {
		t.Fatalf("Confirm = %+v, want attempt 0 at %q", confirmed, StageReceiptTimeout)
	}
}

func TestConfirmHonoursTheDeadlineBoundary(t *testing.T) {
	attempts := []EscalationAttempt{dispatched(0)}
	armed, ok := testPolicy.NextEscalation(raceStart, attempts, streaming)
	if !ok {
		t.Fatal("NextEscalation on a dispatched attempt: want a trigger, got none")
	}
	testCases := []struct {
		name     string
		fireTime time.Time
		want     bool
	}{
		{"one nanosecond early", armed.Deadline.Add(-time.Nanosecond), false},
		{"exactly on the deadline", armed.Deadline, true},
		{"one nanosecond late", armed.Deadline.Add(time.Nanosecond), true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, ok := testPolicy.Confirm(armed, testCase.fireTime, attempts, streaming); ok != testCase.want {
				t.Fatalf("Confirm at %v = %t, want %t", testCase.fireTime, ok, testCase.want)
			}
		})
	}
}

func TestConfirmRejectsAnAttemptIndexOutsideTheRace(t *testing.T) {
	attempts := []EscalationAttempt{dispatched(0)}
	testCases := []struct {
		name  string
		armed ArmedEscalation
	}{
		{"negative index", ArmedEscalation{Attempt: -1, Stage: StageReceiptTimeout, Deadline: raceStart}},
		{"index past the last attempt", ArmedEscalation{Attempt: 1, Stage: StageReceiptTimeout, Deadline: raceStart}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, ok := testPolicy.Confirm(testCase.armed, raceStart.Add(time.Minute), attempts, streaming); ok {
				t.Fatal("Confirm on an out-of-range attempt: want rejection")
			}
		})
	}
}

func TestConfirmSpendsOnlyTheArmedAttemptsEscalation(t *testing.T) {
	attempts := []EscalationAttempt{dispatched(0), dispatched(0)}
	armed, ok := testPolicy.NextEscalation(raceStart, attempts, streaming)
	if !ok {
		t.Fatal("NextEscalation with two dispatched attempts: want a trigger, got none")
	}

	attempts[armed.Attempt].Escalated = true

	if confirmed, ok := testPolicy.Confirm(armed, armed.Deadline, attempts, streaming); ok {
		t.Fatalf("Confirm on an already-escalated attempt = %+v, want rejection", confirmed)
	}
}

func TestStageReasonLabelsEveryTrigger(t *testing.T) {
	testCases := []struct {
		stage EscalationStage
		want  string
	}{
		{StageSuspicious, "suspicious_host"},
		{StageAttemptFailed, "attempt_failed"},
		{StageReceiptTimeout, "receipt_timeout"},
		{StageFirstToken, "first_token_timeout"},
		{StageReducedMaxTokens, "response_timeout_reduced_max_tokens"},
		{StageNone, ""},
	}
	for _, testCase := range testCases {
		t.Run(string(testCase.stage), func(t *testing.T) {
			if got := testCase.stage.Reason(); got != testCase.want {
				t.Fatalf("%q.Reason() = %q, want %q", testCase.stage, got, testCase.want)
			}
		})
	}
}
