package observability

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	envEnabled  = "DAPI_OTEL_ENABLED"
	envEndpoint = "DAPI_OTEL_ENDPOINT"
	envHeaders  = "DAPI_OTEL_HEADERS"
)

type Config struct {
	ServiceName        string
	ServiceVersion     string
	ParticipantAddress string
}

func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if !otelEnabled() {
		logObservabilityInfo("init.disabled", "OpenTelemetry disabled", "env", envEnabled)
		return func(context.Context) error { return nil }, nil
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})

	endpoint := otlpEndpoint()
	if endpoint == "" {
		logObservabilityWarn("init.endpoint_missing", "OpenTelemetry enabled but endpoint is empty; observability will stay disabled", "env", envEndpoint)
		return func(context.Context) error { return nil }, nil
	}

	headers := otlpHeaders()

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(
			attribute.String("service.name", valueOrDefault(cfg.ServiceName, "decentralized-api")),
			attribute.String("service.version", valueOrDefault(cfg.ServiceVersion, "unknown")),
			attribute.String("participant.address", cfg.ParticipantAddress),
		),
	)
	if err != nil {
		return nil, logObservabilityError("init.resource_failed", "Failed to build OpenTelemetry resource", err, "endpoint", endpoint)
	}

	traceExporter, err := otlptracegrpc.New(ctx, newTraceExporterOptions(endpoint, headers)...)
	if err != nil {
		return nil, logObservabilityError("init.trace_exporter_failed", "Failed to create OTLP trace exporter", err, "endpoint", endpoint)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, newMetricExporterOptions(endpoint, headers)...)
	if err != nil {
		return nil, logObservabilityError("init.metric_exporter_failed", "Failed to create OTLP metric exporter", err, "endpoint", endpoint)
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	reader := metric.NewPeriodicReader(metricExporter, metric.WithInterval(15*time.Second))
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(res),
	)

	otel.SetTracerProvider(traceProvider)
	otel.SetMeterProvider(meterProvider)
	initInstruments()
	logObservabilityInfo("init.ready", "OpenTelemetry initialized", "endpoint", endpoint, "headers_configured", len(headers) > 0)

	return func(shutdownCtx context.Context) error {
		shutdownErr := errors.Join(
			meterProvider.Shutdown(shutdownCtx),
			traceProvider.Shutdown(shutdownCtx),
		)
		if shutdownErr != nil {
			logObservabilityError("shutdown.failed", "Failed to shutdown OpenTelemetry providers", shutdownErr)
		}
		return shutdownErr
	}, nil
}

func valueOrDefault(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func otelEnabled() bool {
	enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv(envEnabled)))
	if err != nil {
		if raw := strings.TrimSpace(os.Getenv(envEnabled)); raw != "" {
			logObservabilityWarn("config.invalid_enabled", "Invalid OpenTelemetry enabled flag; observability will stay disabled", "env", envEnabled, "value", raw)
		}
		return false
	}
	return enabled
}

func otlpEndpoint() string {
	return strings.TrimSpace(os.Getenv(envEndpoint))
}

func otlpHeaders() map[string]string {
	return parseHeaders(strings.TrimSpace(os.Getenv(envHeaders)))
}

func parseHeaders(raw string) map[string]string {
	result := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(pair), "=")
		if !found {
			if strings.TrimSpace(pair) != "" {
				logObservabilityWarn("config.invalid_header", "Skipping malformed OTLP header", "raw", strings.TrimSpace(pair))
			}
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			logObservabilityWarn("config.invalid_header", "Skipping OTLP header with empty key or value", "raw", strings.TrimSpace(pair))
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

func newMetricExporterOptions(endpoint string, headers map[string]string) []otlpmetricgrpc.Option {
	opts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(endpoint)}
	if len(headers) > 0 {
		opts = append(opts, otlpmetricgrpc.WithHeaders(headers))
	}
	return opts
}
