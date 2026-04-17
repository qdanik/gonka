package observability

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type InferenceTraceService interface {
	ExtractRequestContext(ctx context.Context, headers http.Header) context.Context
	InjectRequestContext(ctx context.Context, headers http.Header)
	StartRequest(ctx context.Context, method string) (context.Context, *Operation)
	SetRequestIdentity(op *Operation, model string, requester string)
	SetTransferAddress(op *Operation, transferAddress string)
	MarkTransferPath(op *Operation)
	MarkExecutorPath(op *Operation, inferenceID string)
	StartTransfer(ctx context.Context, model string, requester string) (context.Context, *Operation)
	StartForwardExecutor(ctx context.Context, model string, executorAddress string, executorURL string) (context.Context, *Operation)
	StartExecutor(ctx context.Context, inferenceID string, model string, requester string, transferAddress string) (context.Context, *Operation)
	StartMLNodeExecution(ctx context.Context, inferenceID string, model string) (context.Context, *Operation)
	SetMLNodeTarget(op *Operation, nodeID string, nodeURL string)
	StartFinishSubmission(ctx context.Context, inferenceID string, executorAddress string, model string) (context.Context, *Operation)
	SetModel(op *Operation, model string)
	SetResponseHash(op *Operation, responseHash string)
	StartValidationEvent(ctx context.Context, inferenceCount int) (context.Context, *Operation)
	StartStatusUpdateEvent(ctx context.Context, inferenceCount int) (context.Context, *Operation)
	StartValidationSample(ctx context.Context, candidateCount int) (context.Context, *Operation)
	SetSampledCount(op *Operation, sampledCount int)
	StartValidationExecution(ctx context.Context, inferenceID string, model string, epochID int64, revalidation bool) (context.Context, *Operation)
	AddValidationRetry(op *Operation, attempt int, err error)
	SetValidationResult(op *Operation, result any)
	StartPayloadRetrieval(ctx context.Context, inferenceID string, executorAddress string, epochID int64) (context.Context, *Operation)
	StartPayloadRetrievalAttempt(ctx context.Context, inferenceID string, executorAddress string, epochID int64, attempt int) (context.Context, *Operation)
	AddPayloadAttempt(op *Operation, attempt int)
	StartPayloadFetch(ctx context.Context, requestURL string, validatorAddress string, epochID int64) (context.Context, *Operation)
	StartValidationMLNode(ctx context.Context, inferenceID string, model string, nodeID string) (context.Context, *Operation)
	StartCompareLogits(ctx context.Context, inferenceID string) (context.Context, *Operation)
	SetSimilarity(op *Operation, similarity float64)
	SetHTTPStatus(op *Operation, statusCode int)
}

type otelInferenceTraceService struct{}

func NewInferenceTraceService() InferenceTraceService {
	return otelInferenceTraceService{}
}

func (otelInferenceTraceService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return Extract(ctx, headers)
}

func (otelInferenceTraceService) InjectRequestContext(ctx context.Context, headers http.Header) {
	Inject(ctx, headers)
}

func (otelInferenceTraceService) StartRequest(ctx context.Context, method string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Public,
		spanName.Inference.Request,
		trace.SpanKindServer,
		[]attribute.KeyValue{
			attribute.String("http.method", method),
			attribute.String("http.route", "/v1/chat/completions"),
		},
		nil,
	)
}

func (otelInferenceTraceService) SetRequestIdentity(op *Operation, model string, requester string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(
		attribute.String("model", model),
		attribute.String("requester.address", requester),
	)
}

func (otelInferenceTraceService) SetTransferAddress(op *Operation, transferAddress string) {
	if op == nil || transferAddress == "" {
		return
	}
	op.Span().SetAttributes(attribute.String("transfer.address", transferAddress))
}

func (otelInferenceTraceService) MarkTransferPath(op *Operation) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("request.path", "transfer"))
}

func (otelInferenceTraceService) MarkExecutorPath(op *Operation, inferenceID string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(
		attribute.String("request.path", "executor"),
		attribute.String("inference.id", inferenceID),
	)
}

func (otelInferenceTraceService) StartTransfer(ctx context.Context, model string, requester string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Public,
		spanName.Inference.Transfer,
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("model", model),
			attribute.String("requester.address", requester),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) StartForwardExecutor(ctx context.Context, model string, executorAddress string, executorURL string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Public,
		spanName.Inference.ForwardExecutor,
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("executor.address", executorAddress),
			attribute.String("executor.url", executorURL),
			attribute.String("model", model),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) StartExecutor(ctx context.Context, inferenceID string, model string, requester string, transferAddress string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Public,
		spanName.Inference.Execute,
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("model", model),
			attribute.String("requester.address", requester),
			attribute.String("transfer.address", transferAddress),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) StartMLNodeExecution(ctx context.Context, inferenceID string, model string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Public,
		spanName.MLNode.ChatCompletions,
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("model", model),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) SetMLNodeTarget(op *Operation, nodeID string, nodeURL string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(
		attribute.String("mlnode.node.id", nodeID),
		attribute.String("mlnode.url", nodeURL),
	)
}

func (otelInferenceTraceService) StartFinishSubmission(ctx context.Context, inferenceID string, executorAddress string, model string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Public,
		spanName.Inference.FinishSubmit,
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("executor.address", executorAddress),
			attribute.String("model", model),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) SetModel(op *Operation, model string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("model", model))
}

func (otelInferenceTraceService) SetResponseHash(op *Operation, responseHash string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("response.hash", responseHash))
}

func (otelInferenceTraceService) StartValidationEvent(ctx context.Context, inferenceCount int) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.EventListener,
		spanName.Inference.ValidationEvent,
		trace.SpanKindConsumer,
		[]attribute.KeyValue{attribute.Int("inference.count", inferenceCount)},
		nil,
	)
}

func (otelInferenceTraceService) StartStatusUpdateEvent(ctx context.Context, inferenceCount int) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.EventListener,
		spanName.Inference.StatusUpdateEvent,
		trace.SpanKindConsumer,
		[]attribute.KeyValue{attribute.Int("inference.count", inferenceCount)},
		nil,
	)
}

func (otelInferenceTraceService) StartValidationSample(ctx context.Context, candidateCount int) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.Inference.ValidationSample,
		trace.SpanKindInternal,
		[]attribute.KeyValue{attribute.Int("candidate.count", candidateCount)},
		nil,
	)
}

func (otelInferenceTraceService) SetSampledCount(op *Operation, sampledCount int) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.Int("sampled.count", sampledCount))
}

func (otelInferenceTraceService) StartValidationExecution(ctx context.Context, inferenceID string, model string, epochID int64, revalidation bool) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.Inference.ValidationExecute,
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("model", model),
			attribute.Int64("epoch.id", epochID),
			attribute.Bool("revalidation", revalidation),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) AddValidationRetry(op *Operation, attempt int, err error) {
	if op == nil || err == nil {
		return
	}
	op.AddEvent("validation.retry",
		attribute.Int("attempt", attempt),
		attribute.String("error", err.Error()),
	)
}

func (otelInferenceTraceService) SetValidationResult(op *Operation, result any) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("validation.result", fmt.Sprintf("%T", result)))
}

func (otelInferenceTraceService) StartPayloadRetrieval(ctx context.Context, inferenceID string, executorAddress string, epochID int64) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.Inference.PayloadRetrieve,
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("executor.address", executorAddress),
			attribute.Int64("epoch.id", epochID),
		},
		nil,
	)
}

func (otelInferenceTraceService) StartPayloadRetrievalAttempt(ctx context.Context, inferenceID string, executorAddress string, epochID int64, attempt int) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.Inference.PayloadRetrieveAttempt,
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("executor.address", executorAddress),
			attribute.Int64("epoch.id", epochID),
			attribute.Int("payload.attempt", attempt),
		},
		nil,
	)
}

func (otelInferenceTraceService) AddPayloadAttempt(op *Operation, attempt int) {
	if op == nil {
		return
	}
	op.AddEvent("payload.attempt", attribute.Int("attempt", attempt))
}

func (otelInferenceTraceService) StartPayloadFetch(ctx context.Context, requestURL string, validatorAddress string, epochID int64) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.Inference.PayloadFetch,
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("executor.url", requestURL),
			attribute.String("validator.address", validatorAddress),
			attribute.Int64("epoch.id", epochID),
		},
		nil,
	)
}

func (otelInferenceTraceService) StartValidationMLNode(ctx context.Context, inferenceID string, model string, nodeID string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.MLNode.ChatCompletionsValidation,
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("model", model),
			attribute.String("mlnode.node.id", nodeID),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (otelInferenceTraceService) StartCompareLogits(ctx context.Context, inferenceID string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		tracerName.Validation,
		spanName.Inference.CompareLogits,
		trace.SpanKindInternal,
		[]attribute.KeyValue{attribute.String("inference.id", inferenceID)},
		nil,
	)
}

func (otelInferenceTraceService) SetSimilarity(op *Operation, similarity float64) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.Float64("validation.similarity", similarity))
}

func (otelInferenceTraceService) SetHTTPStatus(op *Operation, statusCode int) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.Int("http.status_code", statusCode))
}
