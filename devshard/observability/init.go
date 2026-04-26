package observability

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"

	"devshard/logging"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	envEnabled  = "DEVSHARD_OTEL_ENABLED"
	envEndpoint = "DEVSHARD_OTEL_ENDPOINT"
	envHeaders  = "DEVSHARD_OTEL_HEADERS"
)

type Config struct {
	ServiceName        string
	ServiceVersion     string
	ParticipantAddress string
}

func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	initMetrics()
	if !otelEnabled() {
		return func(context.Context) error { return nil }, nil
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})

	endpoint := strings.TrimSpace(os.Getenv(envEndpoint))
	if endpoint == "" {
		logging.Warn("OpenTelemetry enabled but endpoint is empty; tracing disabled", "subsystem", "observability", "env", envEndpoint)
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(
			attribute.String("service.name", valueOrDefault(cfg.ServiceName, "devshardd")),
			attribute.String("service.version", valueOrDefault(cfg.ServiceVersion, "unknown")),
			attribute.String("participant.address", cfg.ParticipantAddress),
		),
	)
	if err != nil {
		return nil, err
	}

	traceExporter, err := otlptracegrpc.New(ctx, newTraceExporterOptions(endpoint, parseHeaders(strings.TrimSpace(os.Getenv(envHeaders))))...)
	if err != nil {
		return nil, err
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(traceProvider)

	return func(shutdownCtx context.Context) error {
		shutdownErr := traceProvider.Shutdown(shutdownCtx)
		if shutdownErr != nil {
			logging.Error("Failed to shutdown OpenTelemetry providers", "subsystem", "observability", "error", shutdownErr)
		}
		return errors.Join(shutdownErr)
	}, nil
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func otelEnabled() bool {
	raw := strings.TrimSpace(os.Getenv(envEnabled))
	if raw == "" {
		return false
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		logging.Warn("Invalid OpenTelemetry enabled flag; tracing disabled", "subsystem", "observability", "env", envEnabled, "value", raw)
		return false
	}
	return enabled
}

func parseHeaders(raw string) map[string]string {
	result := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(pair), "=")
		if !found {
			if strings.TrimSpace(pair) != "" {
				logging.Warn("Skipping malformed OTLP header", "subsystem", "observability", "raw", strings.TrimSpace(pair))
			}
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			logging.Warn("Skipping OTLP header with empty key or value", "subsystem", "observability", "raw", strings.TrimSpace(pair))
			continue
		}
		result[key] = value
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func newTraceExporterOptions(endpoint string, headers map[string]string) []otlptracegrpc.Option {
	opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(endpoint)}
	if len(headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(headers))
	}
	return opts
}