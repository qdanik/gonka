// Package metrics owns the gateway's Prometheus registry. Family names are
// frozen: dashboards and alerts depend on the devshard_* names carried over
// from devshardctl (spec §3).
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the registry and the HTTP instrumentation families. Later
// phases register their own families on Registry().
type Metrics struct {
	registry            *prometheus.Registry
	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
}

// New builds the registry with Go/process collectors and the HTTP families.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	httpRequestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devshard_http_requests_total",
			Help: "Total HTTP requests handled by the devshard gateway.",
		},
		[]string{"path", "method", "status"},
	)
	httpRequestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "devshard_http_request_duration_seconds",
			Help:    "HTTP request duration by route.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path", "method"},
	)
	registry.MustRegister(httpRequestsTotal, httpRequestDuration)

	return &Metrics{
		registry:            registry,
		httpRequestsTotal:   httpRequestsTotal,
		httpRequestDuration: httpRequestDuration,
	}
}

// Registry exposes the underlying registry for other packages' families.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the Prometheus exposition.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// InstrumentRoute wraps next, recording count and duration under the fixed
// routeLabel. Pass the route PATTERN (e.g. "/devshard/{id}/v1/status"), never
// the raw request path — label cardinality must stay bounded.
func (m *Metrics) InstrumentRoute(routeLabel string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		m.httpRequestsTotal.WithLabelValues(routeLabel, r.Method, strconv.Itoa(recorder.status)).Inc()
		m.httpRequestDuration.WithLabelValues(routeLabel, r.Method).Observe(time.Since(startedAt).Seconds())
	})
}

// statusRecorder captures the response status for labeling. Flush is
// forwarded so streaming handlers keep working when wrapped.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
