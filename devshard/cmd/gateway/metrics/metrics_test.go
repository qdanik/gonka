package metrics

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInstrumentRouteCountsRequestsWithPreservedFamilyNames(t *testing.T) {
	gatewayMetrics := New()
	instrumented := gatewayMetrics.InstrumentRoute("/v1/test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	request := httptest.NewRequest(http.MethodGet, "/v1/test", nil)
	recorder := httptest.NewRecorder()
	instrumented.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("instrumented handler must pass through status, got %d", recorder.Code)
	}

	exposition := scrape(t, gatewayMetrics)
	wantCounter := `devshard_http_requests_total{method="GET",path="/v1/test",status="418"} 1`
	if !strings.Contains(exposition, wantCounter) {
		t.Fatalf("exposition missing %q\n---\n%s", wantCounter, exposition)
	}
	if !strings.Contains(exposition, `devshard_http_request_duration_seconds_count{method="GET",path="/v1/test"} 1`) {
		t.Fatalf("exposition missing duration histogram for the route\n---\n%s", exposition)
	}
}

func TestHandlerServesGoRuntimeCollectors(t *testing.T) {
	gatewayMetrics := New()
	exposition := scrape(t, gatewayMetrics)
	if !strings.Contains(exposition, "go_goroutines") {
		t.Fatal("exposition missing go_goroutines — Go collector not registered")
	}
}

func TestDefaultStatusIsRecordedAs200WhenHandlerWritesBody(t *testing.T) {
	gatewayMetrics := New()
	instrumented := gatewayMetrics.InstrumentRoute("/plain", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok")) // implicit 200, WriteHeader never called
	}))
	recorder := httptest.NewRecorder()
	instrumented.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/plain", nil))

	exposition := scrape(t, gatewayMetrics)
	if !strings.Contains(exposition, `devshard_http_requests_total{method="POST",path="/plain",status="200"} 1`) {
		t.Fatalf("implicit 200 not recorded\n---\n%s", exposition)
	}
}

func scrape(t *testing.T, gatewayMetrics *Metrics) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	gatewayMetrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatalf("reading exposition: %v", err)
	}
	return string(body)
}

// net/http hands the handler whatever RFC 7230 token the client sent, and InstrumentRoute wraps the
// catch-all route outside authentication. A raw method label would let one unauthenticated caller mint
// a permanent series per probe -- the hazard the route label already avoids by never carrying a path.
func TestInstrumentRouteBoundsTheMethodLabel(t *testing.T) {
	telemetry := New()
	handler := telemetry.InstrumentRoute("other", http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))

	for probe := range 200 {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Method = fmt.Sprintf("PROBE%d", probe)
		handler.ServeHTTP(httptest.NewRecorder(), request)
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	families, err := telemetry.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}
	for _, family := range families {
		if family.GetName() != "devshard_http_requests_total" {
			continue
		}
		if series := len(family.GetMetric()); series != 2 {
			t.Fatalf("200 client-chosen methods produced %d series, want 2 (other + GET)", series)
		}
		return
	}
	t.Fatal("devshard_http_requests_total was not registered")
}
