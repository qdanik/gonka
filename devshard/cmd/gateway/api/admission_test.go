package api

import (
	"net/http"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

func TestAdmissionFoldsRelaxedModeOverTheRawChainState(t *testing.T) {
	testCases := []struct {
		name        string
		snapshot    chain.PhaseSnapshot
		mode        string
		wantBlocked bool
		wantMessage string
	}{
		{
			name:     "inference phase admits",
			snapshot: chain.PhaseSnapshot{EpochPhase: chain.EpochPhaseInference},
			mode:     config.PoCModeOff,
		},
		{
			name:        "poc generation blocks",
			snapshot:    chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonPoC, EpochPhase: chain.EpochPhasePoCGenerate},
			mode:        config.PoCModeOff,
			wantBlocked: true,
			wantMessage: "devshard temporarily unavailable during PoC generation",
		},
		{
			name:        "poc validation blocks",
			snapshot:    chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonPoC, EpochPhase: chain.EpochPhasePoCValidate},
			mode:        config.PoCModeOff,
			wantBlocked: true,
			wantMessage: "devshard temporarily unavailable during PoC validation",
		},
		{
			name:        "confirmation poc blocks",
			snapshot:    chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonConfirmationPoC, ConfirmationPoCPhase: chain.ConfirmationPoCGeneration},
			mode:        config.PoCModeOff,
			wantBlocked: true,
			wantMessage: "devshard temporarily unavailable during confirmation PoC generation",
		},
		{
			name:     "relaxed mode admits a blocked poc generation",
			snapshot: chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonPoC, EpochPhase: chain.EpochPhasePoCGenerate},
			mode:     config.PoCModeRelaxed,
		},
		{
			name:     "relaxed mode admits a blocked confirmation poc",
			snapshot: chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonConfirmationPoC, ConfirmationPoCPhase: chain.ConfirmationPoCValidation},
			mode:     config.PoCModeRelaxed,
		},
		{
			name:        "an unnamed reason still blocks",
			snapshot:    chain.PhaseSnapshot{RequestsBlocked: true},
			mode:        config.PoCModeOff,
			wantBlocked: true,
			wantMessage: "devshard temporarily unavailable during chain admission controls",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := admission(testCase.snapshot, config.Modes{PoCMode: testCase.mode})
			if testCase.wantBlocked != (err != nil) {
				t.Fatalf("blocked: got %v, want %v", err != nil, testCase.wantBlocked)
			}
			if err == nil {
				return
			}
			if err.Error() != testCase.wantMessage {
				t.Fatalf("message: got %q, want %q", err.Error(), testCase.wantMessage)
			}
			if got := statusForError(err); got != http.StatusServiceUnavailable {
				t.Fatalf("status: got %d, want 503", got)
			}
		})
	}
}

func TestChatIsRejectedDuringPoCAndServedUnderRelaxedMode(t *testing.T) {
	blocked := chain.PhaseSnapshot{RequestsBlocked: true, BlockReason: chain.BlockReasonPoC, EpochPhase: chain.EpochPhasePoCGenerate}

	rejecting := newHarness(t)
	rejecting.snapshots.snapshot = blocked
	recorder := rejecting.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("blocked chat: got %d, want 503", recorder.Code)
	}
	if got := rejecting.limiter.acquires.Load(); got != 0 {
		t.Fatalf("a blocked request took %d limiter slots; admission must run before the limiter", got)
	}
	if got := rejecting.inference.runs.Load(); got != 0 {
		t.Fatalf("a blocked request started %d races", got)
	}

	relaxed := newHarness(t)
	relaxed.snapshots.snapshot = blocked
	relaxed.swapConfig(func(next *config.Config) { next.Modes.PoCMode = config.PoCModeRelaxed })
	served := relaxed.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if served.Code != http.StatusOK {
		t.Fatalf("relaxed mode must serve the same snapshot: got %d (%s)", served.Code, served.Body.String())
	}
	if got := relaxed.inference.runs.Load(); got != 1 {
		t.Fatalf("relaxed mode races: got %d, want 1", got)
	}
}
