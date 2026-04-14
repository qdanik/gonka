package event_listener

import (
	"context"
	"testing"

	"decentralized-api/internal/event_listener/chainevents"
	"decentralized-api/statsstorage"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type capturingStatsStorage struct {
	updateCtx context.Context
	updates   []struct {
		id     string
		status string
	}
}

func (c *capturingStatsStorage) UpsertInference(ctx context.Context, rec statsstorage.InferenceRecord) error {
	return nil
}
func (c *capturingStatsStorage) UpdateInferenceStatus(ctx context.Context, inferenceID, status string) error {
	c.updateCtx = ctx
	c.updates = append(c.updates, struct {
		id     string
		status string
	}{id: inferenceID, status: status})
	return nil
}
func (c *capturingStatsStorage) GetDeveloperInferencesByTime(ctx context.Context, developer string, timeFrom, timeTo statsstorage.UnixMillis) ([]statsstorage.InferenceRecord, error) {
	return nil, nil
}
func (c *capturingStatsStorage) GetSummaryByDeveloperEpochsBackwards(ctx context.Context, developer string, epochsN int32) (statsstorage.Summary, error) {
	return statsstorage.Summary{}, nil
}
func (c *capturingStatsStorage) GetSummaryByEpochsBackwards(ctx context.Context, epochsN int32) (statsstorage.Summary, error) {
	return statsstorage.Summary{}, nil
}
func (c *capturingStatsStorage) GetSummaryByTimePeriod(ctx context.Context, timeFrom, timeTo statsstorage.UnixMillis) (statsstorage.Summary, error) {
	return statsstorage.Summary{}, nil
}
func (c *capturingStatsStorage) GetModelStatsByTime(ctx context.Context, timeFrom, timeTo statsstorage.UnixMillis) ([]statsstorage.ModelSummary, error) {
	return nil, nil
}
func (c *capturingStatsStorage) GetDebugStats(ctx context.Context) (statsstorage.DebugStats, error) {
	return statsstorage.DebugStats{}, nil
}
func (c *capturingStatsStorage) PruneOlderThan(ctx context.Context, cutoffTimestamp statsstorage.UnixMillis) error {
	return nil
}
func (c *capturingStatsStorage) Close() {}

func setupEventTraceRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider()
	provider.RegisterSpanProcessor(recorder)
	oldProvider := otel.GetTracerProvider()
	oldPropagator := otel.GetTextMapPropagator()
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(oldProvider)
		otel.SetTextMapPropagator(oldPropagator)
		_ = provider.Shutdown(context.Background())
	})
	return recorder
}

func TestInferenceStatusUpdatedEventHandler_UsesObservedContext(t *testing.T) {
	recorder := setupEventTraceRecorder(t)
	storage := &capturingStatsStorage{}
	handler := &InferenceStatusUpdatedEventHandler{}
	el := &EventListener{statsStorage: storage}
	event := &chainevents.JSONRPCResponse{
		Result: chainevents.Result{Events: map[string][]string{
			"inference_status_updated.inference_id": {"inf-1"},
			"inference_status_updated.status":       {"INVALIDATED"},
		}},
	}

	err := handler.Handle(event, el)
	require.NoError(t, err)
	require.NotNil(t, storage.updateCtx)
	require.True(t, trace.SpanFromContext(storage.updateCtx).SpanContext().IsValid())
	require.Len(t, storage.updates, 1)
	require.Equal(t, "inf-1", storage.updates[0].id)
	require.Equal(t, "INVALIDATED", storage.updates[0].status)

	spans := recorder.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "inference.status_update.event", spans[0].Name())
}