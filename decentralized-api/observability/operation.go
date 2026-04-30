package observability

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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
	tracerName tracerID,
	spanName spanID,
	kind trace.SpanKind,
	spanAttrs []attribute.KeyValue,
	metricAttrs []attribute.KeyValue,
) (context.Context, *Operation) {
	initInstruments()
	ctx, span := otel.Tracer(string(tracerName)).Start(ctx, string(spanName), trace.WithSpanKind(kind), trace.WithAttributes(spanAttrs...))
	attrs := withOperation(metricAttrs, string(spanName))
	recordOTelOperationStarted(ctx, attrs)
	recordPrometheusOperationStarted(attrs)
	return ctx, &Operation{
		ctx:         ctx,
		span:        span,
		name:        string(spanName),
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
	recordOTelOperationTokens(o.ctx, metricAttrs, prompt, completion)
	recordPrometheusOperationTokens(metricAttrs, prompt, completion)
	total := prompt + completion
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
	} else {
		o.span.SetStatus(codes.Ok, "")
	}
	recordOTelOperationFinished(o.ctx, o.metricAttrs, o.start, err)
	recordPrometheusOperationFinished(o.metricAttrs, o.start, err)
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

func withOperation(attrs []attribute.KeyValue, operation string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs)+1)
	out = append(out, attribute.String("operation", operation))
	out = append(out, attrs...)
	return out
}
