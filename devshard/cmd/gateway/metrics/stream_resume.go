package metrics

import "github.com/prometheus/client_golang/prometheus"

// StreamSource is the gateway's account of the streams nobody is reading; plain integers avoid an import cycle with api.
type StreamSource interface {
	StreamStats() (orphans, bytes int64)
}

// StreamResumeRecorder counts what became of streams whose client left, and how explicit resumes were answered.
type StreamResumeRecorder struct {
	orphanOutcomes *prometheus.CounterVec
	resumes        *prometheus.CounterVec
}

func NewStreamResumeRecorder(telemetry *Metrics) *StreamResumeRecorder {
	recorder := &StreamResumeRecorder{
		orphanOutcomes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_stream_orphan_outcomes_total",
			Help: "Streams whose client left, by model and by whether the reply was stored, adopted, failed or dropped for a bound.",
		}, []string{"model", "result"}),
		resumes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "devshard_gateway_stream_resume_total",
			Help: "Explicit stream resumes, by model and by whether they were served from a live stream or the cache, or refused.",
		}, []string{"model", "result"}),
	}
	telemetry.Register(recorder.orphanOutcomes, recorder.resumes)
	return recorder
}

func (r *StreamResumeRecorder) OrphanOutcome(model, result string) {
	if r == nil {
		return
	}
	r.orphanOutcomes.WithLabelValues(metricLabel(model, labelUnknown), metricLabel(result, labelUnknown)).Inc()
}

func (r *StreamResumeRecorder) Resume(model, result string) {
	if r == nil {
		return
	}
	r.resumes.WithLabelValues(metricLabel(model, labelUnknown), metricLabel(result, labelUnknown)).Inc()
}

type StreamCollector struct {
	streams StreamSource

	orphans *prometheus.Desc
	bytes   *prometheus.Desc
}

func NewStreamCollector(streams StreamSource) *StreamCollector {
	return &StreamCollector{
		streams: streams,
		orphans: gaugeDesc("devshard_gateway_stream_orphans", "Streams whose client left and that nobody is reading yet."),
		bytes:   gaugeDesc("devshard_gateway_stream_orphan_bytes", "Bytes the streams nobody is reading hold."),
	}
}

func (c *StreamCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.orphans
	ch <- c.bytes
}

func (c *StreamCollector) Collect(ch chan<- prometheus.Metric) {
	if c.streams == nil {
		return
	}
	orphans, bytes := c.streams.StreamStats()
	gauge(ch, c.orphans, float64(orphans))
	gauge(ch, c.bytes, float64(bytes))
}
