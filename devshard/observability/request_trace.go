package observability

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type RequestTraceService interface {
	ExtractRequestContext(ctx context.Context, headers http.Header) context.Context
	InjectRequestContext(ctx context.Context, headers http.Header)
	StartRequest(ctx context.Context, method string, route string) (context.Context, *Operation)
	SetEscrowID(op *Operation, escrowID string)
	SetSessionID(op *Operation, sessionID string)
	SetSender(op *Operation, sender string)
	SetHTTPStatus(op *Operation, statusCode int)
}

type otelRequestTraceService struct{}

func NewRequestTraceService() RequestTraceService {
	return otelRequestTraceService{}
}

func (otelRequestTraceService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return Extract(ctx, headers)
}

func (otelRequestTraceService) InjectRequestContext(ctx context.Context, headers http.Header) {
	Inject(ctx, headers)
}

func (otelRequestTraceService) StartRequest(ctx context.Context, method string, route string) (context.Context, *Operation) {
	route = sanitizeRoute(route)
	return StartOperation(
		ctx,
		tracerName.Transport,
		spanName.Request.Request,
		trace.SpanKindServer,
		[]attribute.KeyValue{
			attribute.String("http.method", method),
			attribute.String("http.route", route),
		},
		[]attribute.KeyValue{
			attribute.String("method", method),
			attribute.String("route", route),
		},
	)
	}

func (otelRequestTraceService) SetEscrowID(op *Operation, escrowID string) {
	if op == nil || escrowID == "" {
		return
	}
	op.Span().SetAttributes(attribute.String("devshard.escrow_id", escrowID))
}

func (otelRequestTraceService) SetSessionID(op *Operation, sessionID string) {
	if op == nil || sessionID == "" {
		return
	}
	op.Span().SetAttributes(attribute.String("devshard.session_id", sessionID))
}

func (otelRequestTraceService) SetSender(op *Operation, sender string) {
	if op == nil || sender == "" {
		return
	}
	op.Span().SetAttributes(attribute.String("devshard.sender", sender))
}

func (otelRequestTraceService) SetHTTPStatus(op *Operation, statusCode int) {
	if op == nil || statusCode == 0 {
		return
	}
	op.SetHTTPStatus(statusCode)
}

func StartRequest(ctx context.Context, method string, route string) (context.Context, *Operation) {
	return Request.StartRequest(ctx, method, route)
}