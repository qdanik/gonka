package telemetry

import (
	"context"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type InferenceService struct{}

var Inference InferenceService

func (InferenceService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return Extract(ctx, headers)
}

func (InferenceService) InjectRequestContext(ctx context.Context, headers http.Header) {
	Inject(ctx, headers)
}

func (InferenceService) StartRequest(ctx context.Context, method string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.public",
		"inference.request",
		trace.SpanKindServer,
		[]attribute.KeyValue{
			attribute.String("http.method", method),
			attribute.String("http.route", "/v1/chat/completions"),
		},
		nil,
	)
}

func (InferenceService) SetRequestIdentity(op *Operation, model string, requester string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(
		attribute.String("model", model),
		attribute.String("requester.address", requester),
	)
}

func (InferenceService) MarkTransferPath(op *Operation) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("request.path", "transfer"))
}

func (InferenceService) MarkExecutorPath(op *Operation, inferenceID string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(
		attribute.String("request.path", "executor"),
		attribute.String("inference.id", inferenceID),
	)
}

func (InferenceService) StartTransfer(ctx context.Context, model string, requester string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.public",
		"inference.transfer",
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("model", model),
			attribute.String("requester.address", requester),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (InferenceService) StartForwardExecutor(ctx context.Context, model string, executorAddress string, executorURL string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.public",
		"inference.transfer.forward_executor",
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("executor.address", executorAddress),
			attribute.String("executor.url", executorURL),
			attribute.String("model", model),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (InferenceService) StartExecutor(ctx context.Context, inferenceID string, model string, requester string, transferAddress string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.public",
		"inference.executor.execute",
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

func (InferenceService) StartMLNodeExecution(ctx context.Context, inferenceID string, model string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.public",
		"mlnode.chat.completions",
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("model", model),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (InferenceService) SetMLNodeTarget(op *Operation, nodeID string, nodeURL string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(
		attribute.String("mlnode.node.id", nodeID),
		attribute.String("mlnode.url", nodeURL),
	)
}

func (InferenceService) StartFinishSubmission(ctx context.Context, inferenceID string, executorAddress string, model string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.public",
		"inference.finish.submit",
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("executor.address", executorAddress),
			attribute.String("model", model),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (InferenceService) SetModel(op *Operation, model string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("model", model))
}

func (InferenceService) SetResponseHash(op *Operation, responseHash string) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("response.hash", responseHash))
}

func (InferenceService) StartValidationEvent(ctx context.Context, inferenceCount int) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.event-listener",
		"inference.validation.event",
		trace.SpanKindConsumer,
		[]attribute.KeyValue{attribute.Int("inference.count", inferenceCount)},
		nil,
	)
}

func (InferenceService) StartValidationSample(ctx context.Context, candidateCount int) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.validation",
		"inference.validation.sample",
		trace.SpanKindInternal,
		[]attribute.KeyValue{attribute.Int("candidate.count", candidateCount)},
		nil,
	)
}

func (InferenceService) SetSampledCount(op *Operation, sampledCount int) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.Int("sampled.count", sampledCount))
}

func (InferenceService) StartValidationExecution(ctx context.Context, inferenceID string, model string, epochID int64, revalidation bool) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.validation",
		"inference.validation.execute",
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

func (InferenceService) AddValidationRetry(op *Operation, attempt int, err error) {
	if op == nil || err == nil {
		return
	}
	op.AddEvent("validation.retry",
		attribute.Int("attempt", attempt),
		attribute.String("error", err.Error()),
	)
}

func (InferenceService) SetValidationResult(op *Operation, result any) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.String("validation.result", fmt.Sprintf("%T", result)))
}

func (InferenceService) StartPayloadRetrieval(ctx context.Context, inferenceID string, executorAddress string, epochID int64) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.validation",
		"inference.payload.retrieve",
		trace.SpanKindInternal,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("executor.address", executorAddress),
			attribute.Int64("epoch.id", epochID),
		},
		nil,
	)
}

func (InferenceService) AddPayloadAttempt(op *Operation, attempt int) {
	if op == nil {
		return
	}
	op.AddEvent("payload.attempt", attribute.Int("attempt", attempt))
}

func (InferenceService) StartPayloadFetch(ctx context.Context, requestURL string, validatorAddress string, epochID int64) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.validation",
		"inference.payload.fetch",
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("executor.url", requestURL),
			attribute.String("validator.address", validatorAddress),
			attribute.Int64("epoch.id", epochID),
		},
		nil,
	)
}

func (InferenceService) StartValidationMLNode(ctx context.Context, inferenceID string, model string, nodeID string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.validation",
		"mlnode.chat.completions.validation",
		trace.SpanKindClient,
		[]attribute.KeyValue{
			attribute.String("inference.id", inferenceID),
			attribute.String("model", model),
			attribute.String("mlnode.node.id", nodeID),
		},
		[]attribute.KeyValue{attribute.String("model", model)},
	)
}

func (InferenceService) StartCompareLogits(ctx context.Context, inferenceID string) (context.Context, *Operation) {
	return StartOperation(
		ctx,
		"decentralized-api.validation",
		"inference.validation.compare_logits",
		trace.SpanKindInternal,
		[]attribute.KeyValue{attribute.String("inference.id", inferenceID)},
		nil,
	)
}

func (InferenceService) SetSimilarity(op *Operation, similarity float64) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.Float64("validation.similarity", similarity))
}

func (InferenceService) SetHTTPStatus(op *Operation, statusCode int) {
	if op == nil {
		return
	}
	op.Span().SetAttributes(attribute.Int("http.status_code", statusCode))
}
