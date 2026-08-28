package metrics

import "github.com/prometheus/client_golang/prometheus"

// LimitRecorder counts what the gateway's own limiter turned away. The caps and the in-flight counts
// are gauges on LimitsCollector; this is the only place the rejections themselves are counted.
type LimitRecorder struct {
	rejections *prometheus.CounterVec
}

func NewLimitRecorder(telemetry *Metrics) *LimitRecorder {
	recorder := &LimitRecorder{
		rejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_limit_rejections_total",
			Help: "Total requests the gateway limiter turned away, by which cap they ran into.",
		}, []string{"model", "reason"}),
	}
	telemetry.Register(recorder.rejections)
	return recorder
}

func (r *LimitRecorder) Rejected(model, reason string) {
	if r == nil {
		return
	}
	r.rejections.WithLabelValues(metricLabel(model, labelUnknown), metricLabel(reason, labelUnknown)).Inc()
}
