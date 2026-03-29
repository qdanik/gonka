package observability

import "log/slog"

const observabilityLogComponent = "observability"

func logObservabilityInfo(event string, message string, args ...any) {
	attrs := append([]any{"component", observabilityLogComponent, "event", event}, args...)
	slog.Info(message, attrs...)
}

func logObservabilityWarn(event string, message string, args ...any) {
	attrs := append([]any{"component", observabilityLogComponent, "event", event}, args...)
	slog.Warn(message, attrs...)
}

func logObservabilityError(event string, message string, err error, args ...any) error {
	attrs := append([]any{"component", observabilityLogComponent, "event", event}, args...)
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	slog.Error(message, attrs...)
	return err
}
