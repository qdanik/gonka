package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"devshard/cmd/gateway/config"
)

// ---- P0: ingest bounds ----

func TestAnOversizedAdminBodyIsRejectedAndTheConnectionIsClosed(t *testing.T) {
	live := newHarness(t)
	upstream := httptest.NewServer(live.server.Handler())
	t.Cleanup(upstream.Close)

	oversized := `{"disabled_message":"` + strings.Repeat("A", adminIngestLimit+1) + `"}`
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPut, upstream.URL+"/v1/admin/settings", strings.NewReader(oversized))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+adminKey)
	response, err := upstream.Client().Do(request)
	if err != nil {
		t.Fatalf("sending request: %v", err)
	}
	t.Cleanup(func() { _ = response.Body.Close() })

	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d, want 413", response.StatusCode)
	}
	if !response.Close {
		t.Fatal("the server kept the connection open after refusing an oversized body")
	}
	if got := len(live.operations.calls); got != 0 {
		t.Fatalf("an oversized body reached %d operations", got)
	}
}

func TestAnAdminBodyInsideTheBoundIsAccepted(t *testing.T) {
	live := newHarness(t)
	body := `{"disabled_message":"` + strings.Repeat("A", 1024) + `"}`
	recorder := live.request(t, http.MethodPut, "/v1/admin/settings", body, adminHeaders())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s), want 200", recorder.Code, recorder.Body.String())
	}
}

func TestAnOversizedChatBodyIsRejectedBeforeAnythingRuns(t *testing.T) {
	live := newHarness(t)
	prompt := strings.Repeat("A", chatIngestLimit)
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"qwen","messages":[{"role":"user","content":"`+prompt+`"}]}`, nil)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status: got %d, want 413", recorder.Code)
	}
	if got := live.limiter.acquires.Load(); got != 0 {
		t.Fatalf("an oversized body took %d limiter slots", got)
	}
	if got := live.inference.runs.Load(); got != 0 {
		t.Fatalf("an oversized body started %d races", got)
	}
}

// ---- P0: the admin key comparison ----

func TestThePublicRoutesNeverReachTheKeyComparison(t *testing.T) {
	probes := []struct {
		name   string
		method string
		target string
	}{
		{name: "models", method: http.MethodGet, target: "/v1/models"},
		{name: "status", method: http.MethodGet, target: "/v1/status"},
		{name: "metrics", method: http.MethodGet, target: "/metrics"},
		{name: "devshard models", method: http.MethodGet, target: "/devshard/7/v1/models"},
		{name: "devshard status", method: http.MethodGet, target: "/devshard/7/v1/status"},
		{name: "unmatched", method: http.MethodGet, target: "/favicon.ico"},
	}
	for _, probe := range probes {
		t.Run(probe.name, func(t *testing.T) {
			live := newHarness(t)
			live.request(t, probe.method, probe.target, "", map[string]string{"Authorization": "Bearer guess"})
			if got := live.comparisons.Load(); got != 0 {
				t.Fatalf("%s compared the admin key %d times; a cheap public route must not be a timing oracle", probe.target, got)
			}
		})
	}
}

func TestAnUnauthenticatedRequestNeverReachesTheKeyComparison(t *testing.T) {
	live := newHarness(t)
	live.request(t, http.MethodGet, "/v1/admin/state", "", nil)
	if got := live.comparisons.Load(); got != 0 {
		t.Fatalf("a request with no Authorization header compared the key %d times", got)
	}
}

func TestTheAdminKeyGateAcceptsOnlyTheConfiguredKey(t *testing.T) {
	testCases := []struct {
		name          string
		authorization string
		want          bool
	}{
		{name: "exact", authorization: "Bearer " + adminKey, want: true},
		{name: "trailing whitespace is trimmed", authorization: "Bearer " + adminKey + "  ", want: true},
		{name: "leading whitespace is trimmed", authorization: "Bearer   " + adminKey, want: true},
		{name: "wrong key", authorization: "Bearer " + adminKey + "x"},
		{name: "prefix of the key", authorization: "Bearer " + adminKey[:4]},
		{name: "no bearer scheme", authorization: adminKey},
		{name: "basic scheme", authorization: "Basic " + adminKey},
		{name: "empty", authorization: ""},
	}
	gate := newKeyGate(adminKey)
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := gate.authenticate(testCase.authorization); got != testCase.want {
				t.Fatalf("authenticate(%q): got %v, want %v", testCase.authorization, got, testCase.want)
			}
		})
	}
}

func TestAnUnconfiguredKeyGateAuthenticatesNothing(t *testing.T) {
	gate := newKeyGate("", "   ")
	if gate.authenticate("Bearer ") || gate.authenticate("Bearer anything") {
		t.Fatal("an unconfigured gate authenticated a request")
	}
}

func TestTheOperatorRoutesRefuseAWrongKey(t *testing.T) {
	live := newHarness(t)
	recorder := live.request(t, http.MethodGet, "/v1/admin/state", "", map[string]string{"Authorization": "Bearer " + adminKey + "x"})
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", recorder.Code)
	}
	if got := live.comparisons.Load(); got != 1 {
		t.Fatalf("comparisons on an operator route: got %d, want 1", got)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type: got %q, want application/json", got)
	}
}

func TestTheOperatorRoutesAre404WhenNoAdminKeyIsConfigured(t *testing.T) {
	live := newHarness(t)
	live.swapConfig(func(next *config.Config) { next.Server.AdminAPIKey = "" })
	recorder := live.request(t, http.MethodGet, "/v1/admin/state", "", adminHeaders())
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status: got %d, want 404", recorder.Code)
	}
}

func TestTheAdminFlagStillReachesTheRequestFilters(t *testing.T) {
	live := newHarness(t)
	live.swapConfig(func(next *config.Config) {
		next.Limits.DefaultMaxTokens = 16
		next.Limits.MaxTokensCap = 32
	})
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"qwen","max_tokens":4096,"messages":[{"role":"user","content":"hi"}]}`, adminHeaders())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if got := live.comparisons.Load(); got != 1 {
		t.Fatalf("the chat path must resolve the admin flag exactly once: got %d", got)
	}
}

func TestAClientAPIKeyOpensAnAPIKeyGatedModel(t *testing.T) {
	live := newHarness(t)
	live.swapConfig(func(next *config.Config) {
		next.Limits.ModelAccess = map[string]string{"qwen": config.ModelAccessAPIKey}
	})
	anonymous := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous: got %d, want 401", anonymous.Code)
	}
	authorized := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody,
		map[string]string{"Authorization": "Bearer " + clientKey})
	if authorized.Code != http.StatusOK {
		t.Fatalf("with an API key: got %d (%s), want 200", authorized.Code, authorized.Body.String())
	}
}

func TestAnAdminOnlyModelRefusesAClientKey(t *testing.T) {
	live := newHarness(t)
	live.swapConfig(func(next *config.Config) {
		next.Limits.ModelAccess = map[string]string{"other": config.ModelAccessOpen}
	})
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", chatBody,
		map[string]string{"Authorization": "Bearer " + clientKey})
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("got %d, want 401", recorder.Code)
	}
	if got := live.limiter.acquires.Load(); got != 0 {
		t.Fatalf("a denied request took %d limiter slots", got)
	}
}
