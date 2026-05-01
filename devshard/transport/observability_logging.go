package transport

import (
	"devshard/observability"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/trace"
)

type requestLogging struct {
	ctx       echo.Context
	requestOp *observability.Operation
}

func newRequestLogging(ctx echo.Context, requestOp *observability.Operation) *requestLogging {
	return &requestLogging{ctx: ctx, requestOp: requestOp}
}

func (logging *requestLogging) logReceived(escrowID string, sessionID string) {
	spanContext := logging.spanContext()
	slog.Info(
		"devshard.request.received",
		"service", observability.ServiceName,
		"method", logging.ctx.Request().Method,
		"route", requestRoute(logging.ctx),
		"escrow_id", escrowID,
		"session_id", sessionID,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	)
}

func (logging *requestLogging) finish(sender string, requestErr error) {
	spanContext := logging.spanContext()
	statusCode := requestStatusCode(logging.ctx, requestErr)
	message := "devshard.request.completed"
	attrs := []any{
		"service", observability.ServiceName,
		"method", logging.ctx.Request().Method,
		"route", requestRoute(logging.ctx),
		"status_code", statusCode,
		"sender", sender,
		"requester_address", sender,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	}
	if requestErr != nil {
		message = "devshard.request.failed"
		attrs = append(attrs, "error", requestErr)
	}
	slog.Info(message, attrs...)
}

func (logging *requestLogging) spanContext() trace.SpanContext {
	if logging == nil || logging.requestOp == nil {
		return trace.SpanContext{}
	}
	return logging.requestOp.Span().SpanContext()
}

func requestRoute(c echo.Context) string {
	route := c.Path()
	if route != "" {
		return route
	}
	return c.Request().URL.Path
}

func requestStatusCode(c echo.Context, requestErr error) int {
	statusCode := c.Response().Status
	if httpErr, ok := requestErr.(*echo.HTTPError); ok {
		return httpErr.Code
	}
	if statusCode == 0 {
		if requestErr != nil {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	}
	if requestErr != nil && statusCode < http.StatusBadRequest {
		return http.StatusInternalServerError
	}
	return statusCode
}
