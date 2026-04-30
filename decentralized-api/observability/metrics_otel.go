package observability

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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

func recordOTelOperationStarted(ctx context.Context, metricAttrs []attribute.KeyValue) {
	initInstruments()
	activeOperations.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
}

func recordOTelOperationTokens(ctx context.Context, metricAttrs []attribute.KeyValue, prompt uint64, completion uint64) {
	initInstruments()
	if prompt > 0 {
		promptTokens.Record(ctx, int64(prompt), metric.WithAttributes(metricAttrs...))
	}
	if completion > 0 {
		completionTokens.Record(ctx, int64(completion), metric.WithAttributes(metricAttrs...))
	}
	total := prompt + completion
	if total > 0 {
		totalTokens.Record(ctx, int64(total), metric.WithAttributes(metricAttrs...))
	}
}

func recordOTelOperationFinished(ctx context.Context, metricAttrs []attribute.KeyValue, startedAt time.Time, err error) {
	initInstruments()
	if err != nil {
		operationErrors.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
	}
	operationDuration.Record(ctx, time.Since(startedAt).Seconds(), metric.WithAttributes(metricAttrs...))
	activeOperations.Add(ctx, -1, metric.WithAttributes(metricAttrs...))
}