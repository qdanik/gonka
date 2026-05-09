package observability

import (
	participantobs "decentralized-api/observability/participant"
	setupreportobs "decentralized-api/observability/setupreport"
	devshardobs "devshard/observability"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
)

var (
	prometheusMetricsOnce sync.Once

	prometheusActiveOperations *prometheus.GaugeVec
	prometheusOperationDuration *prometheus.HistogramVec
	prometheusOperationErrors *prometheus.CounterVec
	prometheusPromptTokens *prometheus.HistogramVec
	prometheusCompletionTokens *prometheus.HistogramVec
	prometheusTotalTokens *prometheus.HistogramVec
)

func MetricsHandler() http.Handler {
	initPrometheusMetrics()
	return promhttp.Handler()
}

func initPrometheusMetrics() {
	prometheusMetricsOnce.Do(func() {
		prometheusActiveOperations = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "decentralized_api_inference_active_operations",
				Help: "Number of in-flight decentralized-api inference operations.",
			},
			[]string{"operation", "model"},
		)
		prometheusOperationDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "decentralized_api_inference_operation_duration_seconds",
				Help:    "Duration of decentralized-api inference operations.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "model"},
		)
		prometheusOperationErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "decentralized_api_inference_operation_errors_total",
				Help: "Count of decentralized-api inference operations that ended with an error.",
			},
			[]string{"operation", "model"},
		)
		prometheusPromptTokens = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "decentralized_api_inference_prompt_tokens",
				Help:    "Prompt token counts recorded by decentralized-api inference operations.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "model"},
		)
		prometheusCompletionTokens = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "decentralized_api_inference_completion_tokens",
				Help:    "Completion token counts recorded by decentralized-api inference operations.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "model"},
		)
		prometheusTotalTokens = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "decentralized_api_inference_total_tokens",
				Help:    "Total token counts recorded by decentralized-api inference operations.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation", "model"},
		)
		prometheus.MustRegister(
			prometheusActiveOperations,
			prometheusOperationDuration,
			prometheusOperationErrors,
			prometheusPromptTokens,
			prometheusCompletionTokens,
			prometheusTotalTokens,
			devshardobs.NewApprovedVersionsCollector(),
			participantobs.NewCollector(),
			setupreportobs.NewCollector(),
		)
	})
}

func recordPrometheusOperationStarted(metricAttrs []attribute.KeyValue) {
	initPrometheusMetrics()
	operation, model := prometheusMetricLabels(metricAttrs)
	prometheusActiveOperations.WithLabelValues(operation, model).Inc()
}

func recordPrometheusOperationTokens(metricAttrs []attribute.KeyValue, prompt uint64, completion uint64) {
	initPrometheusMetrics()
	operation, model := prometheusMetricLabels(metricAttrs)
	if prompt > 0 {
		prometheusPromptTokens.WithLabelValues(operation, model).Observe(float64(prompt))
	}
	if completion > 0 {
		prometheusCompletionTokens.WithLabelValues(operation, model).Observe(float64(completion))
	}
	total := prompt + completion
	if total > 0 {
		prometheusTotalTokens.WithLabelValues(operation, model).Observe(float64(total))
	}
}

func recordPrometheusOperationFinished(metricAttrs []attribute.KeyValue, startedAt time.Time, err error) {
	initPrometheusMetrics()
	operation, model := prometheusMetricLabels(metricAttrs)
	if err != nil {
		prometheusOperationErrors.WithLabelValues(operation, model).Inc()
	}
	prometheusOperationDuration.WithLabelValues(operation, model).Observe(time.Since(startedAt).Seconds())
	prometheusActiveOperations.WithLabelValues(operation, model).Dec()
}

func prometheusMetricLabels(attrs []attribute.KeyValue) (string, string) {
	operation := "unknown"
	model := "unknown"
	for _, attr := range attrs {
		switch string(attr.Key) {
		case "operation":
			operation = attr.Value.AsString()
		case "model":
			if value := attr.Value.AsString(); value != "" {
				model = value
			}
		}
	}
	return operation, model
}