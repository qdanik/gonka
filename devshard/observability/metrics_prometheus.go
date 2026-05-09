package observability

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
)

var (
	metricsOnce sync.Once

	requestActiveOperations *prometheus.GaugeVec
	requestDuration         *prometheus.HistogramVec
	requestErrors           *prometheus.CounterVec
	lifecycleCheckpoints    *prometheus.CounterVec
	lifecycleTerminal       *prometheus.CounterVec
	lifecycleInterruptions  *prometheus.CounterVec
	lifecycleOrphans        *prometheus.CounterVec
	validationQueueDepth    *prometheus.GaugeVec
	mempoolSize             *prometheus.GaugeVec

	inferenceExecutions   *prometheus.CounterVec
	inferenceExecDuration *prometheus.HistogramVec
	inferenceTokens       *prometheus.CounterVec
)

func MetricsHandler() http.Handler {
	initMetrics()
	return promhttp.Handler()
}

func initMetrics() {
	metricsOnce.Do(func() {
		requestActiveOperations = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "devshard_request_active_operations",
				Help: "Number of in-flight devshard HTTP requests by method and route.",
			},
			[]string{"method", "route"},
		)
		requestDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "devshard_request_duration_seconds",
				Help:    "Duration of devshard HTTP requests by method, route, and status code.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "route", "status_code"},
		)
		requestErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_request_errors_total",
				Help: "Count of devshard HTTP requests that ended with an error.",
			},
			[]string{"method", "route", "status_code"},
		)
		lifecycleCheckpoints = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_lifecycle_checkpoint_total",
				Help: "Count of local devshard lifecycle checkpoints by stop point.",
			},
			[]string{"where"},
		)
		lifecycleTerminal = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_lifecycle_terminal_total",
				Help: "Count of local devshard lifecycle terminal outcomes by terminal, reason, and stop point.",
			},
			[]string{"terminal", "reason", "where"},
		)
		lifecycleInterruptions = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_lifecycle_interruption_total",
				Help: "Count of local devshard lifecycle interruptions by class, reason, and stop point.",
			},
			[]string{"class", "reason", "where"},
		)
		lifecycleOrphans = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_lifecycle_orphan_total",
				Help: "Count of local devshard orphan events by kind, reason, and stop point.",
			},
			[]string{"kind", "reason", "where"},
		)
		validationQueueDepth = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "devshard_validation_queue_depth",
				Help: "Current devshard validation queue depth by escrow.",
			},
			[]string{"escrow_id"},
		)
		mempoolSize = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "devshard_mempool_size",
				Help: "Current devshard mempool size by escrow.",
			},
			[]string{"escrow_id"},
		)
		inferenceExecutions = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_inference_total",
				Help: "Count of inference executions by result (completed/failed) and escrow.",
			},
			[]string{"result", "escrow_id"},
		)
		inferenceExecDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "devshard_inference_execution_duration_seconds",
				Help:    "Duration of devshard ML inference execution (engine.Execute call).",
				Buckets: []float64{1, 2, 5, 10, 20, 30, 60, 120, 300},
			},
			[]string{"escrow_id"},
		)
		inferenceTokens = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "devshard_inference_tokens_total",
				Help: "Cumulative input and output tokens processed by devshard inference engine.",
			},
			[]string{"token_type", "escrow_id"},
		)
		prometheus.MustRegister(
			requestActiveOperations,
			requestDuration,
			requestErrors,
			lifecycleCheckpoints,
			lifecycleTerminal,
			lifecycleInterruptions,
			lifecycleOrphans,
			validationQueueDepth,
			mempoolSize,
			inferenceExecutions,
			inferenceExecDuration,
			inferenceTokens,
		)

		// TODO: remove after test
		// Pre-seed label combinations so the series appear in /metrics immediately
		// (value=0) without waiting for the first real observation.
		inferenceExecutions.WithLabelValues("completed", "unknown").Add(0)
		inferenceExecutions.WithLabelValues("failed", "unknown").Add(0)
		inferenceTokens.WithLabelValues("input", "unknown").Add(0)
		inferenceTokens.WithLabelValues("output", "unknown").Add(0)
		requestActiveOperations.WithLabelValues("POST", "/sessions/:id/chat/completions").Add(0)
		requestErrors.WithLabelValues("POST", "/sessions/:id/chat/completions", "500").Add(0)
	})
}

func recordOperationStarted(metricAttrs []attribute.KeyValue) {
	initMetrics()
	method, route := requestMetricLabels(metricAttrs)
	requestActiveOperations.WithLabelValues(method, route).Inc()
}

func recordOperationFinished(metricAttrs []attribute.KeyValue, startedAt time.Time, statusCode int, err error) {
	initMetrics()
	method, route := requestMetricLabels(metricAttrs)
	if err != nil {
		requestErrors.WithLabelValues(method, route, fmt.Sprintf("%d", statusCode)).Inc()
	}
	requestDuration.WithLabelValues(method, route, fmt.Sprintf("%d", statusCode)).Observe(time.Since(startedAt).Seconds())
	requestActiveOperations.WithLabelValues(method, route).Dec()
}

func recordLifecycleCheckpoint(where string) {
	initMetrics()
	lifecycleCheckpoints.WithLabelValues(sanitizeLifecycleValue(where)).Inc()
}

func recordLifecycleTerminal(terminal string, reason string, where string) {
	initMetrics()
	lifecycleTerminal.WithLabelValues(
		sanitizeLifecycleValue(terminal),
		sanitizeLifecycleValue(reason),
		sanitizeLifecycleValue(where),
	).Inc()
}

func recordLifecycleInterruption(class string, reason string, where string) {
	initMetrics()
	lifecycleInterruptions.WithLabelValues(
		sanitizeLifecycleValue(class),
		sanitizeLifecycleValue(reason),
		sanitizeLifecycleValue(where),
	).Inc()
}

func recordLifecycleOrphan(kind string, reason string, where string) {
	initMetrics()
	lifecycleOrphans.WithLabelValues(
		sanitizeLifecycleValue(kind),
		sanitizeLifecycleValue(reason),
		sanitizeLifecycleValue(where),
	).Inc()
}

func setValidationQueueDepth(escrowID string, depth int) {
	initMetrics()
	if depth < 0 {
		depth = 0
	}
	validationQueueDepth.WithLabelValues(sanitizeEscrowID(escrowID)).Set(float64(depth))
}

func setMempoolSize(escrowID string, size int) {
	initMetrics()
	if size < 0 {
		size = 0
	}
	mempoolSize.WithLabelValues(sanitizeEscrowID(escrowID)).Set(float64(size))
}

func sanitizeEscrowID(escrowID string) string {
	if escrowID == "" {
		return "unknown"
	}
	return escrowID
}

func recordInferenceExecution(result string, escrowID string) {
	initMetrics()
	inferenceExecutions.WithLabelValues(result, sanitizeEscrowID(escrowID)).Inc()
}

func recordInferenceExecDuration(escrowID string, d time.Duration) {
	initMetrics()
	inferenceExecDuration.WithLabelValues(sanitizeEscrowID(escrowID)).Observe(d.Seconds())
}

func recordInferenceTokens(tokenType string, escrowID string, count uint64) {
	initMetrics()
	inferenceTokens.WithLabelValues(tokenType, sanitizeEscrowID(escrowID)).Add(float64(count))
}
