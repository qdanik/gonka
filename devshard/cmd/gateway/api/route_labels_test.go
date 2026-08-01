package api

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"devshard/cmd/gateway/metrics"

	dto "github.com/prometheus/client_model/go"
)

var (
	// legacyRouteLabels is the label domain the existing dashboards select on. A string that differs here
	// empties every panel filtered on it, with no error anywhere.
	legacyRouteLabels = []string{
		"/v1/models",
		"/v1/chat/completions",
		"/v1/status",
		"/v1/admin/state",
		"/v1/admin/settings",
		"/v1/admin/devshards",
		"/v1/admin/devshards/{id}",
		"/v1/admin/devshards/{id}/activate",
		"/v1/admin/devshards/{id}/deactivate",
		"/v1/admin/devshards/{id}/settle",
		"/v1/admin/devshards/{id}/participants",
		"/v1/admin/escrows",
		"/v1/admin/suspicious-hosts",
		"/v1/admin/participants/unquarantine",
		"/v1/debug/rotation",
		"/v1/debug/memstats",
		"/devshard/{id}/",
		"/devshard/{id}/openapi.json",
		"/devshard/{id}/v1/models",
		"/devshard/{id}/v1/chat/completions",
		"/devshard/{id}/v1/status",
		"/devshard/{id}/v1/state",
		"/devshard/{id}/v1/finalize",
		"/devshard/{id}/v1/debug/pending",
		"/devshard/{id}/v1/debug/state",
		"/devshard/{id}/v1/debug/inferences",
		"/devshard/{id}/v1/debug/perf",
		"/devshard/{id}/v1/debug/pairwise",
		"/devshard/{id}/v1/debug/signatures",
		"/devshard/{id}/v1/debug/signatures/collect",
		"/devshard/{id}/v1/debug/sync-hosts",
	}

	// deliberateDivergences are the only labels allowed to sit outside the legacy domain.
	deliberateDivergences = []string{otherRouteLabel, "/v1/requests/{id}"}
)

func TestEveryRouteCarriesItsExactMetricLabel(t *testing.T) {
	expected := map[string]string{
		"/v1/models":                            "/v1/models",
		"/v1/chat/completions":                  "/v1/chat/completions",
		"/v1/status":                            "/v1/status",
		"/metrics":                              "",
		"/devshard/{id}/v1/models":              "/devshard/{id}/v1/models",
		"/devshard/{id}/v1/chat/completions":    "/devshard/{id}/v1/chat/completions",
		"/devshard/{id}/v1/status":              "/devshard/{id}/v1/status",
		"/devshard/{id}/v1/finalize":            "/devshard/{id}/v1/finalize",
		"/devshard/{id}/v1/state":               "/devshard/{id}/v1/state",
		"/devshard/{id}/v1/debug/state":         "/devshard/{id}/v1/debug/state",
		"/devshard/{id}/v1/debug/inferences":    "/devshard/{id}/v1/debug/inferences",
		"/devshard/{id}/v1/debug/pending":       "/devshard/{id}/v1/debug/pending",
		"/devshard/{id}/v1/debug/signatures":    "/devshard/{id}/v1/debug/signatures",
		"/v1/requests/{id}":                     "/v1/requests/{id}",
		"/v1/admin/state":                       "/v1/admin/state",
		"/v1/admin/settings":                    "/v1/admin/settings",
		"/v1/admin/devshards":                   "/v1/admin/devshards",
		"/v1/admin/devshards/import":            "/v1/admin/devshards/{id}",
		"/v1/admin/devshards/{id}":              "/v1/admin/devshards/{id}",
		"/v1/admin/devshards/{id}/activate":     "/v1/admin/devshards/{id}/activate",
		"/v1/admin/devshards/{id}/deactivate":   "/v1/admin/devshards/{id}/deactivate",
		"/v1/admin/devshards/{id}/settle":       "/v1/admin/devshards/{id}/settle",
		"/v1/admin/devshards/{id}/participants": "/v1/admin/devshards/{id}/participants",
		"/v1/admin/escrows":                     "/v1/admin/escrows",
		"/v1/admin/suspicious-hosts":            "/v1/admin/suspicious-hosts",
		"/v1/admin/participants/unquarantine":   "/v1/admin/participants/unquarantine",
		"/v1/debug/rotation":                    "/v1/debug/rotation",
		"/v1/debug/memstats":                    "/v1/debug/memstats",
		"/":                                     otherRouteLabel,
	}

	live := newHarness(t)
	registered := live.server.routes()
	if len(registered) != len(expected) {
		t.Fatalf("route count: got %d, want %d — a new route needs its label pinned here", len(registered), len(expected))
	}
	for _, route := range registered {
		want, known := expected[route.pattern]
		if !known {
			t.Fatalf("route %q has no pinned label", route.pattern)
		}
		if route.label != want {
			t.Fatalf("route %q label: got %q, want %q", route.pattern, route.label, want)
		}
	}
}

func TestEveryInstrumentedLabelIsALegacyLabelOrADeclaredDivergence(t *testing.T) {
	live := newHarness(t)
	for _, route := range live.server.routes() {
		if route.label == "" {
			continue
		}
		if slices.Contains(legacyRouteLabels, route.label) || slices.Contains(deliberateDivergences, route.label) {
			continue
		}
		t.Fatalf("route %q emits label %q, which is neither a legacy label nor a declared divergence", route.pattern, route.label)
	}
}

func TestUnmatchedPathsFoldIntoOneLabel(t *testing.T) {
	probes := []string{
		"/favicon.ico",
		"/debug/pprof/",
		"/debug/pprof/heap",
		"/debug/pprof/profile?seconds=30",
		"/devshard/abc/",
		"/devshard/abc/v1/requests/9f2c1d4e-0000-4444-8888-aaaabbbbcccc",
	}
	for _, probe := range probes {
		t.Run(probe, func(t *testing.T) {
			live := newHarness(t)
			recorder := live.request(t, http.MethodGet, probe, "", nil)
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("status: got %d, want 404", recorder.Code)
			}
			if got := live.telemetry.served; got != otherRouteLabel {
				t.Fatalf("label for %q: got %q, want %q", probe, got, otherRouteLabel)
			}
		})
	}
}

// A traversal probe is answered by the mux's path-cleaning redirect, above every registered route, so
// it reaches no label at all.
func TestATraversalProbeReachesNoLabel(t *testing.T) {
	for _, probe := range []string{"/../etc/passwd", "/v1/models/../../secret"} {
		t.Run(probe, func(t *testing.T) {
			live := newHarness(t)
			recorder := live.request(t, http.MethodGet, probe, "", nil)
			if recorder.Code != http.StatusMovedPermanently && recorder.Code != http.StatusTemporaryRedirect {
				t.Fatalf("status: got %d, want a cleaning redirect", recorder.Code)
			}
			if live.telemetry.served != "" {
				t.Fatalf("label: got %q, want none", live.telemetry.served)
			}
		})
	}
}

func TestARequestIDNeverReachesALabel(t *testing.T) {
	requestID := "9f2c1d4e-0000-4444-8888-aaaabbbbcccc"
	live := newHarness(t)
	live.request(t, http.MethodGet, "/v1/requests/"+requestID, "", adminHeaders())
	if live.telemetry.served != "/v1/requests/{id}" {
		t.Fatalf("label: got %q, want %q", live.telemetry.served, "/v1/requests/{id}")
	}
	for _, label := range append(live.telemetry.labels, live.telemetry.served) {
		if strings.Contains(label, requestID) {
			t.Fatalf("label %q leaked the request id", label)
		}
	}
}

// TestMetricsScrapeDoesNotCountItself drives the real telemetry rather than the harness fake: the
// families it would populate are the point of the assertion.
func TestMetricsScrapeDoesNotCountItself(t *testing.T) {
	telemetry := metrics.New()
	live := newHarness(t)
	live.server.telemetry = telemetry
	live.server.handler = live.server.buildHandler()

	for range 2 {
		recorder := httptest.NewRecorder()
		live.server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("scrape status: got %d", recorder.Code)
		}
	}
	// One instrumented request proves the family exists at all, so an empty result cannot pass by accident.
	live.server.Handler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/v1/models", nil))

	requests := gatheredFamily(t, telemetry, "devshard_http_requests_total")
	if len(requests) == 0 {
		t.Fatal("devshard_http_requests_total is absent, so the assertion below proves nothing")
	}
	for _, metric := range requests {
		if labelValue(metric, "path") == "/metrics" {
			t.Fatalf("the scrape counted itself: %v", metric)
		}
	}
	for _, metric := range gatheredFamily(t, telemetry, "devshard_http_request_duration_seconds") {
		if labelValue(metric, "path") == "/metrics" {
			t.Fatalf("the duration histogram measured its own exposition: %v", metric)
		}
	}
}

func gatheredFamily(t *testing.T, telemetry *metrics.Metrics, name string) []*dto.Metric {
	t.Helper()
	families, err := telemetry.Registry().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()
		}
	}
	return nil
}

func labelValue(metric *dto.Metric, name string) string {
	for _, pair := range metric.GetLabel() {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}
