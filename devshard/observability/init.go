package observability

import (
	"context"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// pkgTracerProvider holds the TracerProvider set during Init so that
// getTracer() uses the devshard-specific provider instead of the global OTel
// provider. This avoids conflicts when a host binary (e.g. decentralized-api)
// has already installed its own provider under a different service.name.
var pkgTracerProvider atomic.Value // stores trace.TracerProvider

// getTracer returns a Tracer from the devshard-local provider when Init has
// been called, otherwise falls back to the OTel global (e.g. in tests).
func getTracer(name string) trace.Tracer {
	if v := pkgTracerProvider.Load(); v != nil {
		return v.(trace.TracerProvider).Tracer(name)
	}
	return otel.Tracer(name)
}

const (
	envEnabled  = "DEVSHARD_OTEL_ENABLED"
	envEndpoint = "OTEL_ENDPOINT"
	envHeaders  = "OTEL_HEADERS"
)

type Config struct {
	ServiceName        string
	ServiceVersion     string
	ParticipantAddress string
}

// Option customises Init behaviour.
type Option func(*initOptions)

type initOptions struct {
	traceProvider trace.TracerProvider
	meterProvider metric.MeterProvider
	shutdown      func(context.Context) error
}

// WithOtelProviders injects pre-built OTel trace and meter providers so Init
// does not create its own. The shutdown function is returned by Init and will
// be called on process exit.
//
// Use this when the host binary already built providers via
// decentralized-api/observability.BuildProviders — both devshard spans and
// dapi-package spans then share one exporter and one service.name in Jaeger.
func WithOtelProviders(tp trace.TracerProvider, mp metric.MeterProvider, shutdown func(context.Context) error) Option {
	return func(o *initOptions) {
		o.traceProvider = tp
		o.meterProvider = mp
		o.shutdown = shutdown
	}
}

// Init initialises Prometheus metrics and, when DEVSHARD_OTEL_ENABLED=true,
// the OTel global trace provider for devshard spans.
//
// When opts contains WithOtelProviders the pre-built providers are activated
// instead of building new ones — no env-var check is performed in that case.
func Init(ctx context.Context, cfg Config, opts ...Option) (func(context.Context) error, error) {
	InitMetricsOnly()

	initOpts := &initOptions{}
	for _, opt := range opts {
		opt(initOpts)
	}

	// Pre-built providers injected by the host binary — store locally so
	// getTracer() uses the devshard-specific provider without touching the
	// global OTel provider (the host already manages it).
	if initOpts.traceProvider != nil {
		otel.SetTextMapPropagator(propagation.TraceContext{})
		pkgTracerProvider.Store(initOpts.traceProvider)
		if initOpts.meterProvider != nil {
			otel.SetMeterProvider(initOpts.meterProvider)
		}
		logObservabilityInfo("init.ready", "OpenTelemetry initialised via injected providers", "pid", os.Getpid())
		shutdown := initOpts.shutdown
		if shutdown == nil {
			shutdown = func(context.Context) error { return nil }
		}
		return shutdown, nil
	}

	// Standalone init — check env var and build own trace provider.
	if !otelEnabled() {
		logObservabilityInfo("init.disabled", "OpenTelemetry disabled", "env", envEnabled)
		return func(context.Context) error { return nil }, nil
	}

	otel.SetTextMapPropagator(propagation.TraceContext{})

	endpoint := strings.TrimSpace(os.Getenv(envEndpoint))
	if endpoint == "" {
		logObservabilityWarn("init.endpoint_missing", "OpenTelemetry enabled but endpoint is empty; tracing disabled", "env", envEndpoint)
		return func(context.Context) error { return nil }, nil
	}

	res, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(
			attribute.String("service.name", valueOrDefault(cfg.ServiceName, ServiceName)),
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
	pkgTracerProvider.Store(trace.TracerProvider(traceProvider))
	logObservabilityInfo(
		"init.ready",
		"OpenTelemetry initialized",
		"endpoint", endpoint,
		"service_version", valueOrDefault(cfg.ServiceVersion, "unknown"),
		"participant_address", cfg.ParticipantAddress,
		"pid", os.Getpid(),
	)

	return func(shutdownCtx context.Context) error {
		return traceProvider.Shutdown(shutdownCtx)
	}, nil
}

// InitMetricsOnly initialises only the Prometheus metrics for this process.
// Use this when the host binary owns the global OTel provider via its own
// observability.Init call (e.g. decentralized-api/observability.Init).
func InitMetricsOnly() {
	initMetrics()
}

// OtelEnabled reports whether DEVSHARD_OTEL_ENABLED is set to true.
// Callers can use this to decide whether to build OTel providers.
func OtelEnabled() bool {
	return otelEnabled()
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
		logObservabilityWarn("config.invalid_enabled", "Invalid OpenTelemetry enabled flag; tracing disabled", "env", envEnabled, "value", raw)
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
