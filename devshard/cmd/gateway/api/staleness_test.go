package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

var staleTestEpoch = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

func TestAdmissionRefusesAnUnrefreshedSnapshot(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name       string
		age        time.Duration
		maxAge     int64
		relaxed    bool
		blocked    bool
		wantRefuse bool
	}{
		{name: "fresh", age: 5 * time.Second, maxAge: 30},
		{name: "one second inside the limit", age: 29 * time.Second, maxAge: 30},
		{name: "on the limit", age: 30 * time.Second, maxAge: 30, wantRefuse: true},
		{name: "far past it", age: 10 * time.Minute, maxAge: 30, wantRefuse: true},
		{name: "the limit switched off", age: 10 * time.Minute, maxAge: 0},
		{name: "relaxed mode still answers to the clock", age: 10 * time.Minute, maxAge: 30, relaxed: true, blocked: true, wantRefuse: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			snapshot := chain.PhaseSnapshot{
				LastHealthyAt:   staleTestEpoch.Add(-testCase.age),
				RequestsBlocked: testCase.blocked,
				BlockReason:     chain.BlockReasonPoC,
			}

			modes := config.Modes{}
			if testCase.relaxed {
				modes.PoCMode = config.PoCModeRelaxed
			}
			err := admission(snapshot, modes, staleTestEpoch, testCase.maxAge)

			var stale *ChainStaleError
			if errors.As(err, &stale) != testCase.wantRefuse {
				t.Fatalf("admission() = %v, refuse = %v, want %v", err, err != nil, testCase.wantRefuse)
			}
			if testCase.wantRefuse && statusForError(err) != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503: a shard that cannot read the chain has no room, it has no quota", statusForError(err))
			}
		})
	}
}

func TestAStaleSnapshotRefusalCarriesRetryAfter(t *testing.T) {
	t.Parallel()
	recorder := httptest.NewRecorder()

	writeErrorFor(recorder, &ChainStaleError{Age: time.Minute})

	if got, want := recorder.Header().Get("Retry-After"), "5"; got != want {
		t.Fatalf("Retry-After = %q, want %q: one poll interval, not the one-second default", got, want)
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", recorder.Code)
	}
}

func TestAdmissionRefusesASnapshotThatWasNeverPublished(t *testing.T) {
	t.Parallel()
	err := admission(chain.PhaseSnapshot{}, config.Modes{}, staleTestEpoch, 30)

	var stale *ChainStaleError
	if !errors.As(err, &stale) {
		t.Fatalf("admission() = %v, want a staleness refusal", err)
	}
}

func TestABlockedPhaseOutranksStaleness(t *testing.T) {
	t.Parallel()
	snapshot := chain.PhaseSnapshot{
		RequestsBlocked: true,
		BlockReason:     "poc_generating",
		LastHealthyAt:   staleTestEpoch.Add(-time.Hour),
	}

	err := admission(snapshot, config.Modes{}, staleTestEpoch, 30)

	var blocked *BlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("admission() = %v, want the blocked-phase answer", err)
	}
}
