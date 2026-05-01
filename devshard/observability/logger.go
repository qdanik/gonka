package observability

import "log/slog"

const (
	ServiceName = "devshardd"
	ObservabilityName = "observability"
)

func logObservabilityInfo(event string, message string, args ...any) {
	attrs := append([]any{
		"service", ServiceName,
		"subsystem", ObservabilityName,
		"event", event,
	}, args...)
	slog.Info(message, attrs...)
}

func logObservabilityWarn(event string, message string, args ...any) {
	attrs := append([]any{
		"service", ServiceName,
		"subsystem", ObservabilityName,
		"event", event,
	}, args...)
	slog.Warn(message, attrs...)
}

func logObservabilityError(event string, message string, err error, args ...any) error {
	attrs := append([]any{
		"service", ServiceName,
		"subsystem", ObservabilityName,
		"event", event,
	}, args...)
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	slog.Error(message, attrs...)
	return err
}