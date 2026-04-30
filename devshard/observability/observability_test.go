package observability

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider()
	provider.RegisterSpanProcessor(recorder)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func TestRequestOperationRecordsAttributesAndErrors(t *testing.T) {
	recorder := setupTraceRecorder(t)

	_, op := StartRequest(context.Background(), http.MethodPost, "/v1/devshard/sessions/:id/chat/completions")
	Request.SetEscrowID(op, "escrow-1")
	Request.SetSessionID(op, "escrow-1")
	Request.SetSender(op, "gonka1sender")
	op.SetHTTPStatus(http.StatusUnauthorized)
	op.Finish(errors.New("unauthorized"))

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	attrs := spans[0].Attributes()
	assertHasAttr(t, attrs, "http.method", http.MethodPost)
	assertHasAttr(t, attrs, "http.route", "/v1/devshard/sessions/:id/chat/completions")
	assertHasAttr(t, attrs, "devshard.escrow_id", "escrow-1")
	assertHasAttr(t, attrs, "devshard.session_id", "escrow-1")
	assertHasAttr(t, attrs, "devshard.sender", "gonka1sender")
	assertHasAttr(t, attrs, "http.status_code", http.StatusUnauthorized)
}

func assertHasAttr(t *testing.T, attrs []attribute.KeyValue, key string, expected any) {
	t.Helper()
	for _, attr := range attrs {
		if string(attr.Key) != key {
			continue
		}
		switch want := expected.(type) {
		case string:
			if attr.Value.AsString() != want {
				t.Fatalf("attr %s: expected %q, got %q", key, want, attr.Value.AsString())
			}
			return
		case int:
			if int(attr.Value.AsInt64()) != want {
				t.Fatalf("attr %s: expected %d, got %d", key, want, attr.Value.AsInt64())
			}
			return
		}
	}
	t.Fatalf("missing attr %s", key)
}