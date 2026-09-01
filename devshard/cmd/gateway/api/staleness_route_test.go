package api

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
)

func TestChatIsRefusedWhenTheChainSnapshotIsTooOld(t *testing.T) {
	harness := newHarness(t, func(configuration *config.Config) {
		configuration.Chain.SnapshotMaxAgeSeconds = 30
	})
	harness.snapshots.snapshot = chain.PhaseSnapshot{
		LastHealthyAt: harnessClock.Add(-time.Hour),
	}

	recorder := harness.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"qwen","messages":[{"role":"user","content":"hi"}]}`,
		map[string]string{"Authorization": "Bearer " + clientKey})

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: %s", recorder.Code, recorder.Body)
	}
	if got := recorder.Header().Get("Retry-After"); got == "" {
		t.Errorf("Retry-After = %q, want the observer's poll interval", got)
	}
}

func TestStatusNamesAStaleSnapshotAsTheReasonItIsBlocked(t *testing.T) {
	harness := newHarness(t, func(configuration *config.Config) {
		configuration.Chain.SnapshotMaxAgeSeconds = 30
	})
	harness.snapshots.snapshot = chain.PhaseSnapshot{
		LastHealthyAt: harnessClock.Add(-time.Hour),
	}

	recorder := harness.request(t, http.MethodGet, "/v1/status", "",
		map[string]string{"Authorization": "Bearer " + clientKey})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body)
	}
	body := recorder.Body.String()
	for _, want := range []string{`"requests_blocked":true`, string(chain.BlockReasonSnapshotStale)} {
		if !strings.Contains(body, want) {
			t.Errorf("status body does not carry %s: %s", want, body)
		}
	}
}
