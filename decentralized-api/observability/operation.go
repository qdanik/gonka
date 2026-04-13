package observability

import (
	"context"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const meterName = "decentralized-api/observability"

var (
	instrumentsMu sync.Mutex
	instrumentProvider metric.MeterProvider

	activeOperations  metric.Int64UpDownCounter
	operationDuration metric.Float64Histogram
	operationErrors   metric.Int64Counter
	promptTokens      metric.Int64Histogram
	completionTokens  metric.Int64Histogram
	totalTokens       metric.Int64Histogram
)

type Operation struct {
	ctx         context.Context
	span        trace.Span
	name        string
	start       time.Time
	metricAttrs []attribute.KeyValue
}

func StartOperation(
	ctx context.Context,
	tracerName string,
	spanName string,
	kind trace.SpanKind,
	spanAttrs []attribute.KeyValue,
	metricAttrs []attribute.KeyValue,
) (context.Context, *Operation) {
	initInstruments()
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName, trace.WithSpanKind(kind), trace.WithAttributes(spanAttrs...))
	attrs := withOperation(metricAttrs, spanName)
	activeOperations.Add(ctx, 1, metric.WithAttributes(attrs...))
	return ctx, &Operation{
		ctx:         ctx,
		span:        span,
		name:        spanName,
		start:       time.Now(),
		metricAttrs: attrs,
	}
}

func (o *Operation) Context() context.Context {
	if o == nil {
		return context.Background()
	}
	return o.ctx
}

func (o *Operation) Span() trace.Span {
	if o == nil {
		return trace.SpanFromContext(context.Background())
	}
	return o.span
}

func (o *Operation) AddEvent(name string, attrs ...attribute.KeyValue) {
	if o == nil {
		return
	}
	o.span.AddEvent(name, trace.WithAttributes(attrs...))
}

func (o *Operation) RecordTokens(prompt uint64, completion uint64, attrs ...attribute.KeyValue) {
	if o == nil {
		return
	}
	metricAttrs := append(append([]attribute.KeyValue{}, o.metricAttrs...), attrs...)
	if prompt > 0 {
		promptTokens.Record(o.ctx, int64(prompt), metric.WithAttributes(metricAttrs...))
	}
	if completion > 0 {
		completionTokens.Record(o.ctx, int64(completion), metric.WithAttributes(metricAttrs...))
	}
	total := prompt + completion
	if total > 0 {
		totalTokens.Record(o.ctx, int64(total), metric.WithAttributes(metricAttrs...))
	}
	o.span.SetAttributes(
		attribute.Int64("inference.tokens.prompt", int64(prompt)),
		attribute.Int64("inference.tokens.completion", int64(completion)),
		attribute.Int64("inference.tokens.total", int64(total)),
	)
}

func (o *Operation) Finish(err error, attrs ...attribute.KeyValue) {
	if o == nil {
		return
	}
	if len(attrs) > 0 {
		o.span.SetAttributes(attrs...)
	}
	if err != nil {
		o.span.RecordError(err)
		o.span.SetStatus(codes.Error, err.Error())
		operationErrors.Add(o.ctx, 1, metric.WithAttributes(o.metricAttrs...))
	} else {
		o.span.SetStatus(codes.Ok, "")
	}
	operationDuration.Record(o.ctx, time.Since(o.start).Seconds(), metric.WithAttributes(o.metricAttrs...))
	activeOperations.Add(o.ctx, -1, metric.WithAttributes(o.metricAttrs...))
	o.span.End()
}

func (o *Operation) FinishErr(err *error, attrs ...attribute.KeyValue) {
	if err == nil {
		o.Finish(nil, attrs...)
		return
	}
	o.Finish(*err, attrs...)
}

func Extract(ctx context.Context, headers http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(headers))
}

func Inject(ctx context.Context, headers http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(headers))
}

func initInstruments() {
	provider := otel.GetMeterProvider()

	instrumentsMu.Lock()
	defer instrumentsMu.Unlock()

	if instrumentProvider == provider {
		return
	}

	meter := provider.Meter(meterName)
	newActiveOperations, err := meter.Int64UpDownCounter("decentralized_api.inference.active_operations")
	if err != nil {
		logObservabilityError("metrics.init_active_operations_failed", "Failed to initialize active operations metric", err)
		return
	}
	newOperationDuration, err := meter.Float64Histogram("decentralized_api.inference.operation.duration_seconds")
	if err != nil {
		logObservabilityError("metrics.init_duration_failed", "Failed to initialize operation duration metric", err)
		return
	}
	newOperationErrors, err := meter.Int64Counter("decentralized_api.inference.operation.errors")
	if err != nil {
		logObservabilityError("metrics.init_errors_failed", "Failed to initialize operation errors metric", err)
		return
	}
	newPromptTokens, err := meter.Int64Histogram("decentralized_api.inference.prompt_tokens")
	if err != nil {
		logObservabilityError("metrics.init_prompt_tokens_failed", "Failed to initialize prompt tokens metric", err)
		return
	}
	newCompletionTokens, err := meter.Int64Histogram("decentralized_api.inference.completion_tokens")
	if err != nil {
		logObservabilityError("metrics.init_completion_tokens_failed", "Failed to initialize completion tokens metric", err)
		return
	}
	newTotalTokens, err := meter.Int64Histogram("decentralized_api.inference.total_tokens")
	if err != nil {
		logObservabilityError("metrics.init_total_tokens_failed", "Failed to initialize total tokens metric", err)
		return
	}

	activeOperations = newActiveOperations
	operationDuration = newOperationDuration
	operationErrors = newOperationErrors
	promptTokens = newPromptTokens
	completionTokens = newCompletionTokens
	totalTokens = newTotalTokens
	instrumentProvider = provider
}

func withOperation(attrs []attribute.KeyValue, operation string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs)+1)
	out = append(out, attribute.String("operation", operation))
	out = append(out, attrs...)
	return out
}
