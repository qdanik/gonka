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
	statusCode  int
}

func StartOperation(
	ctx context.Context,
	tracer tracerID,
	span spanID,
	kind trace.SpanKind,
	spanAttrs []attribute.KeyValue,
	metricAttrs []attribute.KeyValue,
) (context.Context, *Operation) {
	initMetrics()
	ctx, startedSpan := otel.Tracer(string(tracer)).Start(
		ctx,
		string(span),
		trace.WithSpanKind(kind),
		trace.WithAttributes(spanAttrs...),
	)
	recordOperationStarted(metricAttrs)
	return ctx, &Operation{
		ctx:         ctx,
		span:        startedSpan,
		name:        string(span),
		start:       time.Now(),
		metricAttrs: metricAttrs,
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

func (o *Operation) SetHTTPStatus(statusCode int) {
	if o == nil || statusCode == 0 {
		return
	}
	o.statusCode = statusCode
	o.span.SetAttributes(attribute.Int("http.status_code", statusCode))
}

func (o *Operation) Finish(err error) {
	if o == nil {
		return
	}
	statusCode := o.statusCode
	if statusCode == 0 {
		if err != nil {
			statusCode = http.StatusInternalServerError
		} else {
			statusCode = http.StatusOK
		}
		o.span.SetAttributes(attribute.Int("http.status_code", statusCode))
	}

	if err != nil {
		o.span.RecordError(err)
		o.span.SetStatus(codes.Error, err.Error())
	} else {
		o.span.SetStatus(codes.Ok, "")
	}

	recordOperationFinished(o.metricAttrs, o.start, statusCode, err)
	o.span.End()
}

func (o *Operation) FinishErr(err *error) {
	if err == nil {
		o.Finish(nil)
		return
	}
	o.Finish(*err)
}

func Extract(ctx context.Context, headers http.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(headers))
}

func Inject(ctx context.Context, headers http.Header) {
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(headers))
}

func sanitizeRoute(route string) string {
	if route == "" {
		return "unknown"
	}
	return route
}

func requestMetricLabels(attrs []attribute.KeyValue) (string, string) {
	method := "unknown"
	route := "unknown"
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "method":
			method = attr.Value.AsString()
		case "route":
			route = sanitizeRoute(attr.Value.AsString())
		}
	}
	return method, route
}