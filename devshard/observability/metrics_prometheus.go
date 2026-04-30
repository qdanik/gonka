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
		prometheus.MustRegister(requestActiveOperations, requestDuration, requestErrors)
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