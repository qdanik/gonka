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
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	envEnabled  = "DAPI_OTEL_ENABLED"
	envEndpoint = "OTEL_ENDPOINT"
	envHeaders  = "OTEL_HEADERS"
)

type Config struct {
	ServiceName        string
	ServiceVersion     string
	ParticipantAddress string
}

// Providers holds pre-built OTel trace and meter providers together with a
// combined shutdown function. Used to share a single provider setup across
// multiple observability packages (e.g. devshard) without re-creating
// exporters or overwriting the service resource.
type Providers struct {
	Trace    trace.TracerProvider
	Meter    metric.MeterProvider
	Shutdown func(context.Context) error
}

// BuildProviders creates OTel trace and metric providers for the given config
// but does NOT set them as the global providers and does NOT check any enabled
// env var — the caller decides when and whether to activate them.
// Returns nil without error when OTEL_ENDPOINT is not configured.
func BuildProviders(ctx context.Context, cfg Config) (*Providers, error) {
	endpoint := otlpEndpoint()
	if endpoint == "" {
		return nil, nil
	}

	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, err
	}

	headers := otlpHeaders()
	traceExporter, err := otlptracegrpc.New(ctx, newTraceExporterOptions(endpoint, headers)...)
	if err != nil {
		return nil, logObservabilityError("init.trace_exporter_failed", "Failed to create OTLP trace exporter", err, "endpoint", endpoint)
	}

	metricExporter, err := otlpmetricgrpc.New(ctx, newMetricExporterOptions(endpoint, headers)...)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, logObservabilityError("init.metric_exporter_failed", "Failed to create OTLP metric exporter", err, "endpoint", endpoint)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
	reader := sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithInterval(15*time.Second))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))

	return &Providers{
		Trace: tp,
		Meter: mp,
		Shutdown: func(shutdownCtx context.Context) error {
			return errors.Join(mp.Shutdown(shutdownCtx), tp.Shutdown(shutdownCtx))
		},
	}, nil
}

func buildResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	return resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(
			attribute.String("service.name", valueOrDefault(cfg.ServiceName, ServiceName)),
			attribute.String("service.version", valueOrDefault(cfg.ServiceVersion, "unknown")),
			attribute.String("participant.address", cfg.ParticipantAddress),
		),
	)
}

func Init(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if !otelEnabled() {
		logObservabilityInfo("init.disabled", "OpenTelemetry disabled", "env", envEnabled)
		return func(context.Context) error { return nil }, nil
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})

	providers, err := BuildProviders(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if providers == nil {
		logObservabilityWarn("init.endpoint_missing", "OpenTelemetry enabled but endpoint is empty; observability will stay disabled", "env", envEndpoint)
		return func(context.Context) error { return nil }, nil
	}

	endpoint := otlpEndpoint()
	headers := otlpHeaders()
	otel.SetTracerProvider(providers.Trace)
	otel.SetMeterProvider(providers.Meter)
	initInstruments()
	logObservabilityInfo("init.ready", "OpenTelemetry initialized", "endpoint", endpoint, "headers_configured", len(headers) > 0)

	return func(shutdownCtx context.Context) error {
		shutdownErr := providers.Shutdown(shutdownCtx)
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
