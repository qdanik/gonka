package proxy

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracetest "go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
	})

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider()
	provider.RegisterSpanProcessor(recorder)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return recorder
}

func findIntAttribute(attrs []attribute.KeyValue, key string) (int64, bool) {
	for _, attr := range attrs {
		if string(attr.Key) == key && attr.Value.Type() == attribute.INT64 {
			return attr.Value.AsInt64(), true
		}
	}
	return 0, false
}

func newRoutes(m map[string]string) *atomic.Value {
	v := &atomic.Value{}
	v.Store(m)
	return v
}

func TestProxy_BasicForwarding(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "path=%s", r.URL.Path)
	}))
	defer backend.Close()

	// Extract host:port from backend URL
	addr := strings.TrimPrefix(backend.URL, "http://")
	routes := newRoutes(map[string]string{"v1": addr})

	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "path=/chat/completions" {
		t.Errorf("body = %q", string(body))
	}
}

func TestProxy_RootPath(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "path=%s", r.URL.Path)
	}))
	defer backend.Close()

	addr := strings.TrimPrefix(backend.URL, "http://")
	routes := newRoutes(map[string]string{"v1": addr})

	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "path=/" {
		t.Errorf("body = %q", string(body))
	}
}

func TestProxy_VersionNotFound(t *testing.T) {
	routes := newRoutes(map[string]string{})
	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

func TestProxy_NoVersionPrefix(t *testing.T) {
	routes := newRoutes(map[string]string{})
	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestProxy_QueryParams(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "query=%s", r.URL.RawQuery)
	}))
	defer backend.Close()

	addr := strings.TrimPrefix(backend.URL, "http://")
	routes := newRoutes(map[string]string{"v1": addr})

	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/search?q=hello&limit=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "query=q=hello&limit=10" {
		t.Errorf("body = %q", string(body))
	}
}

func TestProxy_SSEStreaming(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("ResponseWriter does not implement Flusher")
		}
		for i := 0; i < 3; i++ {
			fmt.Fprintf(w, "data: event %d\n\n", i)
			flusher.Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer backend.Close()

	addr := strings.TrimPrefix(backend.URL, "http://")
	routes := newRoutes(map[string]string{"v1": addr})

	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("Content-Type = %q", resp.Header.Get("Content-Type"))
	}

	scanner := bufio.NewScanner(resp.Body)
	var events []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			events = append(events, line)
		}
	}
	if len(events) != 3 {
		t.Errorf("got %d events, want 3", len(events))
	}
}

func TestProxy_InjectsTraceparent(t *testing.T) {
	recorder := setupTraceRecorder(t)

	var backendTraceparent string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendTraceparent = r.Header.Get("traceparent")
		fmt.Fprint(w, "ok")
	}))
	defer backend.Close()

	addr := strings.TrimPrefix(backend.URL, "http://")
	routes := newRoutes(map[string]string{"v1": addr})

	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("traceparent", "00-11111111111111111111111111111111-2222222222222222-01")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	if backendTraceparent == "" {
		t.Fatal("backend did not receive traceparent header")
	}
	if !strings.Contains(backendTraceparent, "11111111111111111111111111111111") {
		t.Fatalf("backend traceparent %q does not preserve incoming trace id", backendTraceparent)
	}
	if got := len(recorder.Ended()); got != 1 {
		t.Fatalf("ended spans = %d, want 1", got)
	}
}

func TestProxy_NoVersionPrefix_CreatesSpan(t *testing.T) {
	recorder := setupTraceRecorder(t)
	routes := newRoutes(map[string]string{})
	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if got := len(recorder.Ended()); got != 1 {
		t.Fatalf("ended spans = %d, want 1", got)
	}
	statusCode, ok := findIntAttribute(recorder.Ended()[0].Attributes(), "http.status_code")
	if !ok || statusCode != http.StatusBadRequest {
		t.Fatalf("http.status_code = %d, found=%t, want %d", statusCode, ok, http.StatusBadRequest)
	}
}

func TestProxy_VersionNotFound_CreatesSpan(t *testing.T) {
	recorder := setupTraceRecorder(t)
	routes := newRoutes(map[string]string{})
	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
	if got := len(recorder.Ended()); got != 1 {
		t.Fatalf("ended spans = %d, want 1", got)
	}
	statusCode, ok := findIntAttribute(recorder.Ended()[0].Attributes(), "http.status_code")
	if !ok || statusCode != http.StatusNotFound {
		t.Fatalf("http.status_code = %d, found=%t, want %d", statusCode, ok, http.StatusNotFound)
	}
}

func TestProxy_BackendTransportError_CreatesFailedSpan(t *testing.T) {
	recorder := setupTraceRecorder(t)
	routes := newRoutes(map[string]string{"v1": "127.0.0.1:1"})
	handler := Handler(routes)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/foo")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadGateway)
	}
	if got := len(recorder.Ended()); got != 1 {
		t.Fatalf("ended spans = %d, want 1", got)
	}
	statusCode, ok := findIntAttribute(recorder.Ended()[0].Attributes(), "http.status_code")
	if !ok || statusCode != http.StatusBadGateway {
		t.Fatalf("http.status_code = %d, found=%t, want %d", statusCode, ok, http.StatusBadGateway)
	}
}
