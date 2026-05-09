package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// InferenceSpan wraps an OTel span for a single ML inference execution.
// Call Finish on success, FinishError on failure. Either must be called exactly once.
type InferenceSpan struct {
	span     trace.Span
	start    time.Time
	escrowID string
}

// StartInferenceExecution begins an OTel span covering ML engine execution for a
// single inference. The returned context carries the span and should be passed to
// the engine. Call Finish or FinishError when execution ends.
func StartInferenceExecution(ctx context.Context, escrowID string, inferenceID uint64) (context.Context, *InferenceSpan) {
	ctx, span := getTracer(string(tracerName.Inference)).Start(
		ctx,
		string(spanName.Inference.Execution),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("devshard.escrow_id", escrowID),
			attribute.Int64("devshard.inference_id", int64(inferenceID)),
		),
	)
	logObservabilityInfo("inference.span.start", "started inference span",
		"escrow_id", escrowID,
		"inference_id", inferenceID,
		"is_recording", span.IsRecording(),
	)
	return ctx, &InferenceSpan{
		span:     span,
		start:    time.Now(),
		escrowID: escrowID,
	}
}

// Finish records a successful execution. Records duration, token counts, and
// emits devshard_inference_total{result="completed"} and token metrics.
func (s *InferenceSpan) Finish(inputTokens, outputTokens uint64) {
	s.span.SetAttributes(
		attribute.Int64("devshard.input_tokens", int64(inputTokens)),
		attribute.Int64("devshard.output_tokens", int64(outputTokens)),
	)
	s.span.SetStatus(codes.Ok, "")
	s.span.End()

	dur := time.Since(s.start)
	logObservabilityInfo("inference.span.finish", "finished inference span",
		"escrow_id", s.escrowID,
		"result", "completed",
		"duration_ms", dur.Milliseconds(),
		"input_tokens", inputTokens,
		"output_tokens", outputTokens,
	)
	recordInferenceExecution("completed", s.escrowID)
	recordInferenceExecDuration(s.escrowID, dur)
	recordInferenceTokens("input", s.escrowID, inputTokens)
	recordInferenceTokens("output", s.escrowID, outputTokens)
}

// FinishError records a failed execution. Marks the OTel span as error so
// Jaeger shows it with error=true, enabling drill-down from the dashboard.
// Emits devshard_inference_total{result="failed"}.
func (s *InferenceSpan) FinishError(err error) {
	s.FinishErrorWithTokens(err, 0, 0)
}

// FinishErrorWithTokens records a failed execution while still preserving token
// counts. Use when the ML engine completed successfully but a later step failed
// (e.g. signing), so token usage is known and should be counted.
func (s *InferenceSpan) FinishErrorWithTokens(err error, inputTokens, outputTokens uint64) {
	s.span.RecordError(err)
	s.span.SetStatus(codes.Error, err.Error())
	if inputTokens > 0 || outputTokens > 0 {
		s.span.SetAttributes(
			attribute.Int64("devshard.input_tokens", int64(inputTokens)),
			attribute.Int64("devshard.output_tokens", int64(outputTokens)),
		)
	}
	s.span.End()

	dur := time.Since(s.start)
	logObservabilityError("inference.span.finish", "finished inference span with error", err,
		"escrow_id", s.escrowID,
		"result", "failed",
		"duration_ms", dur.Milliseconds(),
		"input_tokens", inputTokens,
		"output_tokens", outputTokens,
	)
	recordInferenceExecution("failed", s.escrowID)
	recordInferenceExecDuration(s.escrowID, dur)
	recordInferenceTokens("input", s.escrowID, inputTokens)
	recordInferenceTokens("output", s.escrowID, outputTokens)
}
