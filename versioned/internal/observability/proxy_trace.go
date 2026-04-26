package observability

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type ProxyTraceService interface {
	ExtractRequestContext(ctx context.Context, headers http.Header) context.Context
	InjectRequestContext(ctx context.Context, headers http.Header)
	StartRequest(ctx context.Context, method string, path string, version string, target string) (context.Context, *Operation)
	SetHTTPStatus(op *Operation, statusCode int)
}

type otelProxyTraceService struct{}

func NewProxyTraceService() ProxyTraceService {
	return otelProxyTraceService{}
}

func (otelProxyTraceService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return Extract(ctx, headers)
}

func (otelProxyTraceService) InjectRequestContext(ctx context.Context, headers http.Header) {
	Inject(ctx, headers)
}

func (otelProxyTraceService) StartRequest(ctx context.Context, method string, path string, version string, target string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Proxy,
		spanName.Proxy.Request,
		trace.SpanKindServer,
		[]attribute.KeyValue{
			attribute.String("service.name", "versiond"),
			attribute.String("http.method", method),
			attribute.String("http.route", "/{version}"),
			attribute.String("http.target", path),
			attribute.String("version.name", version),
			attribute.String("proxy.target", target),
		},
	)
}

func (otelProxyTraceService) SetHTTPStatus(op *Operation, statusCode int) {
	if op == nil || statusCode == 0 {
		return
	}
	op.SetHTTPStatus(statusCode)
}