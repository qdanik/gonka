package proxy

import (
	"context"
	"log/slog"
	"net/http"

	"versioned/internal/observability"

	"go.opentelemetry.io/otel/trace"
)

type requestLogging struct {
	request   *http.Request
	requestOp *observability.Operation
	version   string
	target    string
	status    int
}

func newRequestLogging(r *http.Request, requestOp *observability.Operation, version string, target string) *requestLogging {
	return &requestLogging{request: r, requestOp: requestOp, version: version, target: target}
}

func startRequestLogging(r *http.Request, version string, target string) (context.Context, *requestLogging) {
	requestContext := observability.Proxy.ExtractRequestContext(r.Context(), r.Header)
	requestContext, requestOp := observability.Proxy.StartRequest(requestContext, r.Method, r.URL.Path, version, target)
	requestLogging := newRequestLogging(r, requestOp, version, target)
	requestLogging.logReceived()
	return requestContext, requestLogging
}

func (logging *requestLogging) logReceived() {
	spanContext := logging.spanContext()
	slog.Info(
		"versiond.proxy.request",
		"service", "versiond",
		"method", logging.request.Method,
		"path", logging.request.URL.Path,
		"version", logging.version,
		"target", logging.target,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	)
}

func (logging *requestLogging) setHTTPStatus(statusCode int) {
	if logging == nil || statusCode == 0 {
		return
	}
	logging.status = statusCode
	if logging.requestOp != nil {
		logging.requestOp.SetHTTPStatus(statusCode)
	}
}

func (logging *requestLogging) finish(requestErr *error) {
	statusCode := logging.status
	if statusCode == 0 {
		if requestErr != nil && *requestErr != nil {
			statusCode = http.StatusInternalServerError
		} else {
			statusCode = http.StatusOK
		}
		logging.setHTTPStatus(statusCode)
	}

	if logging.requestOp != nil {
		logging.requestOp.FinishErr(requestErr)
	}

	spanContext := logging.spanContext()
	attrs := []any{
		"service", "versiond",
		"method", logging.request.Method,
		"path", logging.request.URL.Path,
		"version", logging.version,
		"target", logging.target,
		"status_code", statusCode,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	}
	if requestErr != nil && *requestErr != nil {
		attrs = append(attrs, "error", *requestErr)
		slog.Error("versiond.proxy.request_failed", attrs...)
		return
	}
	slog.Info("versiond.proxy.request_completed", attrs...)
}

func (logging *requestLogging) spanContext() trace.SpanContext {
	if logging == nil || logging.requestOp == nil {
		return trace.SpanContext{}
	}
	return logging.requestOp.Span().SpanContext()
}