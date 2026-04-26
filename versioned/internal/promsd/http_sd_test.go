package promsd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func newRoutes(m map[string]string) *atomic.Value {
	v := &atomic.Value{}
	v.Store(m)
	return v
}

func TestHandler_EmitsSortedTargetGroups(t *testing.T) {
	handler := Handler(newRoutes(map[string]string{
		"v0.2.12": "localhost:5001",
		"v0.2.11": "localhost:5000",
	}), "versiond:8080")

	req := httptest.NewRequest(http.MethodGet, "/prometheus/sd", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var groups []struct {
		Targets []string          `json:"targets"`
		Labels  map[string]string `json:"labels"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &groups); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("got %d groups, want 2", len(groups))
	}
	if groups[0].Targets[0] != "versiond:8080" {
		t.Fatalf("target = %q, want versiond:8080", groups[0].Targets[0])
	}
	if groups[0].Labels["version"] != "v0.2.11" {
		t.Fatalf("first version = %q, want v0.2.11", groups[0].Labels["version"])
	}
	if groups[0].Labels["__metrics_path__"] != "/v0.2.11/metrics" {
		t.Fatalf("first metrics path = %q", groups[0].Labels["__metrics_path__"])
	}
	if groups[1].Labels["version"] != "v0.2.12" {
		t.Fatalf("second version = %q, want v0.2.12", groups[1].Labels["version"])
	}
}

func TestHandler_EmptyRoutes(t *testing.T) {
	handler := Handler(newRoutes(map[string]string{}), "versiond:8080")

	req := httptest.NewRequest(http.MethodGet, "/prometheus/sd", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); body != "[]\n" {
		t.Fatalf("body = %q, want []", body)
	}
}