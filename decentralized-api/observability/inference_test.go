package observability

import (
	"context"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
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

func TestInferenceRequestSpan(t *testing.T) {
	recorder := setupTraceRecorder(t)

	ctx, op := Inference.StartRequest(context.Background(), http.MethodPost)
	Inference.SetRequestIdentity(op, "model-a", "gonka1requester")
	Inference.SetTransferAddress(op, "gonka1transfer")
	Inference.MarkExecutorPath(op, "inf-1")
	op.Finish(nil)

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	span := spans[0]
	if span.Name() != "inference.request" {
		t.Fatalf("unexpected span name: %s", span.Name())
	}
	attrs := span.Attributes()
	assertHasAttr(t, attrs, "http.method", http.MethodPost)
	assertHasAttr(t, attrs, "http.route", "/v1/chat/completions")
	assertHasAttr(t, attrs, "model", "model-a")
	assertHasAttr(t, attrs, "requester.address", "gonka1requester")
	assertHasAttr(t, attrs, "transfer.address", "gonka1transfer")
	assertHasAttr(t, attrs, "request.path", "executor")
	assertHasAttr(t, attrs, "inference.id", "inf-1")
	if trace.SpanFromContext(ctx).SpanContext().TraceID() != span.SpanContext().TraceID() {
		t.Fatalf("expected returned context to carry request span")
	}
}

func TestValidationRetryAndSimilarity(t *testing.T) {
	recorder := setupTraceRecorder(t)

	_, validationOp := Inference.StartValidationExecution(context.Background(), "inf-2", "model-b", 42, true)
	Inference.AddValidationRetry(validationOp, 2, context.DeadlineExceeded)
	Inference.SetValidationResult(validationOp, struct{}{})
	validationOp.Finish(nil)

	_, compareOp := Inference.StartCompareLogits(context.Background(), "inf-2")
	Inference.SetSimilarity(compareOp, 0.995)
	compareOp.Finish(nil)

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	assertHasAttr(t, spans[0].Attributes(), "revalidation", true)
	assertHasAttr(t, spans[0].Attributes(), "validation.result", "struct {}")
	if len(spans[0].Events()) != 1 || spans[0].Events()[0].Name != "validation.retry" {
		t.Fatalf("expected validation.retry event")
	}
	assertHasAttr(t, spans[1].Attributes(), "validation.similarity", 0.995)
}

func TestContextPropagationHelpers(t *testing.T) {
	recorder := setupTraceRecorder(t)

	ctx, parentOp := Inference.StartRequest(context.Background(), http.MethodPost)
	headers := make(http.Header)
	Inference.InjectRequestContext(ctx, headers)
	childCtx := Inference.ExtractRequestContext(context.Background(), headers)
	_, childOp := Inference.StartPayloadFetch(childCtx, "http://executor/v1/inference/payloads", "gonka1validator", 7)
	childOp.Finish(nil)
	parentOp.Finish(nil)

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}
	var parentTraceID trace.TraceID
	for _, span := range spans {
		if span.Name() == "inference.request" {
			parentTraceID = span.SpanContext().TraceID()
		}
	}
	for _, span := range spans {
		if span.Name() == "inference.payload.fetch" && span.SpanContext().TraceID() != parentTraceID {
			t.Fatalf("expected propagated trace id")
		}
	}
}

func TestPayloadRetrievalAttemptIsChildOfRetrieval(t *testing.T) {
	recorder := setupTraceRecorder(t)

	ctx, retrievalOp := Inference.StartPayloadRetrieval(context.Background(), "inf-3", "gonka1executor", 7)
	Inference.AddPayloadAttempt(retrievalOp, 2)
	_, attemptOp := Inference.StartPayloadRetrievalAttempt(ctx, "inf-3", "gonka1executor", 7, 2)
	attemptOp.Finish(nil)
	retrievalOp.Finish(nil)

	spans := recorder.Ended()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	var retrievalSpan, attemptSpan sdktrace.ReadOnlySpan
	for _, span := range spans {
		switch span.Name() {
		case "inference.payload.retrieve":
			retrievalSpan = span
		case "inference.payload.retrieve.attempt":
			attemptSpan = span
		}
	}
	if retrievalSpan == nil || attemptSpan == nil {
		t.Fatalf("expected retrieval and attempt spans to be present")
	}
	if attemptSpan.Parent().SpanID() != retrievalSpan.SpanContext().SpanID() {
		t.Fatalf("expected attempt span to be a child of retrieval span")
	}
	if len(retrievalSpan.Events()) != 1 || retrievalSpan.Events()[0].Name != "payload.attempt" {
		t.Fatalf("expected payload.attempt event on retrieval span")
	}
	assertHasAttr(t, attemptSpan.Attributes(), "payload.attempt", 2)
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
		case int64:
			if attr.Value.AsInt64() != want {
				t.Fatalf("attr %s: expected %d, got %d", key, want, attr.Value.AsInt64())
			}
			return
		case bool:
			if attr.Value.AsBool() != want {
				t.Fatalf("attr %s: expected %t, got %t", key, want, attr.Value.AsBool())
			}
			return
		case float64:
			if attr.Value.AsFloat64() != want {
				t.Fatalf("attr %s: expected %f, got %f", key, want, attr.Value.AsFloat64())
			}
			return
		}
	}
	t.Fatalf("missing attr %s", key)
}
