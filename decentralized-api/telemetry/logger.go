package telemetry

import "log/slog"

const telemetryLogComponent = "telemetry"

func logTelemetryInfo(event string, message string, args ...any) {
	attrs := append([]any{"component", telemetryLogComponent, "event", event}, args...)
	slog.Info(message, attrs...)
}

func logTelemetryWarn(event string, message string, args ...any) {
	attrs := append([]any{"component", telemetryLogComponent, "event", event}, args...)
	slog.Warn(message, attrs...)
}

func logTelemetryError(event string, message string, err error, args ...any) error {
	attrs := append([]any{"component", telemetryLogComponent, "event", event}, args...)
	if err != nil {
		attrs = append(attrs, "error", err)
	}
	slog.Error(message, attrs...)
	return err
}
