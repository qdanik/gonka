package public

import (
	"log/slog"
	"net/http"

	"decentralized-api/observability"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/trace"
)

type inferenceRequestLogging struct {
	ctx       echo.Context
	requestOp *observability.Operation
}

func newInferenceRequestLogging(ctx echo.Context, requestOp *observability.Operation) *inferenceRequestLogging {
	return &inferenceRequestLogging{ctx: ctx, requestOp: requestOp}
}

func (logging *inferenceRequestLogging) withOperation(requestOp *observability.Operation) *inferenceRequestLogging {
	if logging == nil {
		return &inferenceRequestLogging{requestOp: requestOp}
	}
	return &inferenceRequestLogging{ctx: logging.ctx, requestOp: requestOp}
}

func (logging *inferenceRequestLogging) logReceived() {
	spanContext := logging.spanContext()
	slog.Info(
		"inference.request.received",
		"service", "api",
		"method", logging.requestMethod(),
		"path", logging.requestPath(),
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	)
}

func (logging *inferenceRequestLogging) logClassified(request *ChatRequest) {
	if request == nil {
		return
	}
	requestKind := "transfer"
	if request.InferenceId != "" && request.Seed != "" {
		requestKind = "executor"
	}
	spanContext := logging.spanContext()
	slog.Info(
		"inference.request.classified",
		"service", "api",
		"method", logging.requestMethod(),
		"path", logging.requestPath(),
		"request_kind", requestKind,
		"model", request.OpenAiRequest.Model,
		"requester_address", request.RequesterAddress,
		"transfer_address", request.TransferAddress,
		"inference_id", request.InferenceId,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	)
}

func (logging *inferenceRequestLogging) finishLog(requestErr error) {
	spanContext := logging.spanContext()
	statusCode := requestStatusCode(logging.ctx, requestErr)
	message := "inference.request.completed"
	attrs := []any{
		"service", "api",
		"method", logging.requestMethod(),
		"path", logging.requestPath(),
		"status_code", statusCode,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	}
	if requestErr != nil {
		message = "inference.request.failed"
		attrs = append(attrs, "error", requestErr)
	}
	slog.Info(message, attrs...)
}

func (logging *inferenceRequestLogging) finish(requestErr *error) {
	if logging.requestOp != nil {
		logging.requestOp.FinishErr(requestErr)
	}
	if requestErr == nil {
		logging.finishLog(nil)
		return
	}
	logging.finishLog(*requestErr)
}

func (logging *inferenceRequestLogging) logAssigned(inferenceID string, requesterAddress string, transferAddress string) {
	spanContext := logging.spanContext()
	slog.Info(
		"inference.request.assigned",
		"service", "api",
		"method", logging.requestMethod(),
		"path", logging.requestPath(),
		"inference_id", inferenceID,
		"requester_address", requesterAddress,
		"transfer_address", transferAddress,
		"trace_id", spanContext.TraceID().String(),
		"span_id", spanContext.SpanID().String(),
	)
}

func (logging *inferenceRequestLogging) spanContext() trace.SpanContext {
	if logging == nil || logging.requestOp == nil {
		return trace.SpanContext{}
	}
	return logging.requestOp.Span().SpanContext()
}

func (logging *inferenceRequestLogging) requestMethod() string {
	if logging == nil || logging.ctx == nil || logging.ctx.Request() == nil {
		return ""
	}
	return logging.ctx.Request().Method
}

func (logging *inferenceRequestLogging) requestPath() string {
	if logging == nil || logging.ctx == nil || logging.ctx.Request() == nil || logging.ctx.Request().URL == nil {
		return ""
	}
	return logging.ctx.Request().URL.Path
}

func requestStatusCode(ctx echo.Context, requestErr error) int {
	if ctx == nil {
		if httpErr, ok := requestErr.(*echo.HTTPError); ok {
			return httpErr.Code
		}
		if requestErr != nil {
			return http.StatusInternalServerError
		}
		return http.StatusOK
	}
	statusCode := ctx.Response().Status
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
