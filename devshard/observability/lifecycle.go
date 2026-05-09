package observability

import "strings"

type LifecycleEvent struct {
	InferenceID      uint64
	Reason           string
	Where            string
	FailureWhere     string
	ReceiptExpected  bool
	ReceiptObserved  bool
	ExecutionStarted bool
	FinishPublished  bool
}

func RecordLifecycleCheckpoint(event LifecycleEvent, args ...any) {
	recordLifecycleCheckpoint(sanitizeLifecycleValue(event.Where))
	logObservabilityInfo(
		"lifecycle.checkpoint",
		"devshard.lifecycle.checkpoint",
		append(lifecycleEventArgs(event), args...)...,
	)
}

func RecordLifecycleTerminal(terminal string, event LifecycleEvent, args ...any) {
	terminal = sanitizeLifecycleValue(terminal)
	reason := sanitizeLifecycleValue(event.Reason)
	where := sanitizeLifecycleValue(event.Where)
	recordLifecycleTerminal(terminal, reason, where)
	logObservabilityInfo(
		"lifecycle.terminal",
		"devshard.lifecycle.terminal",
		append([]any{"terminal", terminal}, append(lifecycleEventArgs(event), args...)...)...,
	)
}

func RecordLifecycleInterruption(class string, event LifecycleEvent, args ...any) {
	class = sanitizeLifecycleValue(class)
	reason := sanitizeLifecycleValue(event.Reason)
	where := sanitizeLifecycleValue(event.Where)
	recordLifecycleInterruption(class, reason, where)
	logObservabilityWarn(
		"lifecycle.interruption",
		"devshard.lifecycle.interruption",
		append([]any{"class", class}, append(lifecycleEventArgs(event), args...)...)...,
	)
}

func RecordLifecycleOrphan(kind string, event LifecycleEvent, args ...any) {
	kind = sanitizeLifecycleValue(kind)
	reason := sanitizeLifecycleValue(event.Reason)
	where := sanitizeLifecycleValue(event.Where)
	recordLifecycleOrphan(kind, reason, where)
	logObservabilityWarn(
		"lifecycle.orphan",
		"devshard.lifecycle.orphan",
		append([]any{"kind", kind}, append(lifecycleEventArgs(event), args...)...)...,
	)
}

func RecordValidationQueueDepth(escrowID string, depth int) {
	setValidationQueueDepth(escrowID, depth)
}

func RecordMempoolSize(escrowID string, size int) {
	setMempoolSize(escrowID, size)
}

func lifecycleEventArgs(event LifecycleEvent) []any {
	args := []any{
		"inference_id", event.InferenceID,
		"reason", sanitizeLifecycleValue(event.Reason),
		"where", sanitizeLifecycleValue(event.Where),
		"receipt_expected", event.ReceiptExpected,
		"receipt_observed", event.ReceiptObserved,
		"execution_started", event.ExecutionStarted,
		"finish_published", event.FinishPublished,
	}
	if event.FailureWhere != "" {
		args = append(args, "failure_where", sanitizeLifecycleValue(event.FailureWhere))
	}
	return args
}

func sanitizeLifecycleValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return "unknown"
	}
	value = strings.ReplaceAll(value, " ", "_")
	return value
}