package accounting

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

type dispositionLabels struct {
	disposition   Disposition
	ghostReason   string
	timeoutAction string
	timeoutReason string
}

// Gauges, not counters: a disposition moves, so a series goes down as well as up.
type Collector struct {
	book *Book

	assigned     *prometheus.Desc
	disposition  *prometheus.Desc
	chainMissed  *prometheus.Desc
	chainInvalid *prometheus.Desc
	pending      *prometheus.Desc
	unobserved   *prometheus.Desc
	overcounted  *prometheus.Desc
	rejected     *prometheus.Desc
	finding      *prometheus.Desc
}

func NewCollector(book *Book) *Collector {
	participantLabels := []string{"epoch", "participant", "model"}
	return &Collector{
		book: book,
		assigned: prometheus.NewDesc("devshard_gateway_nonces_assigned",
			"Nonces the chain assigns to this participant's slots.", participantLabels, nil),
		disposition: prometheus.NewDesc("devshard_gateway_nonces_by_disposition",
			"Classified nonces by what became of them.",
			append([]string{"disposition", "ghost_reason", "timeout_action", "timeout_reason"}, participantLabels...), nil),
		chainMissed: prometheus.NewDesc("devshard_gateway_nonces_chain_missed",
			"Misses the chain recorded against this participant's slots.", participantLabels, nil),
		chainInvalid: prometheus.NewDesc("devshard_gateway_nonces_chain_invalid",
			"Invalid verdicts the chain recorded against this participant's slots.", participantLabels, nil),
		pending: prometheus.NewDesc("devshard_gateway_nonces_pending",
			"Nonces seen unfinished whose timeout has not settled.", participantLabels, nil),
		unobserved: prometheus.NewDesc("devshard_gateway_nonces_unobserved",
			"Assigned nonces the ledger never saw: protocol overhead plus races still running.", participantLabels, nil),
		overcounted: prometheus.NewDesc("devshard_gateway_nonces_overcounted",
			"Nonces classified beyond what the chain assigned; non-zero means the ledger and the chain disagree.", participantLabels, nil),
		rejected: prometheus.NewDesc("devshard_gateway_nonce_facts_rejected_total",
			"Facts dropped because their escrow was never opened.", nil, nil),
		finding: prometheus.NewDesc("devshard_gateway_nonce_finding",
			"Findings raised against a participant, valued at the rate that raised them. Alert on presence.",
			append([]string{"code", "severity"}, participantLabels...), nil),
	}
}

func (c *Collector) Describe(descs chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{
		c.assigned, c.disposition, c.chainMissed, c.chainInvalid,
		c.pending, c.unobserved, c.overcounted, c.rejected, c.finding,
	} {
		descs <- desc
	}
}

func (c *Collector) Collect(metrics chan<- prometheus.Metric) {
	metrics <- prometheus.MustNewConstMetric(c.rejected, prometheus.CounterValue, float64(c.book.Rejected()))
	labels := make([]string, 0, 7)
	summed := make(map[dispositionLabels]uint64)
	for _, record := range c.book.Query(QueryFilter{}) {
		epoch := strconv.FormatUint(record.EpochIndex, 10)
		for _, gauge := range []struct {
			desc  *prometheus.Desc
			value float64
		}{
			{c.assigned, float64(record.Assigned)},
			{c.chainMissed, float64(record.ChainMissed)},
			{c.chainInvalid, float64(record.ChainInvalid)},
			{c.pending, float64(record.Pending)},
			{c.unobserved, float64(record.Unobserved)},
			{c.overcounted, float64(record.Overcounted)},
		} {
			metrics <- prometheus.MustNewConstMetric(gauge.desc, prometheus.GaugeValue, gauge.value,
				epoch, record.Participant, record.Model)
		}
		for _, finding := range record.Findings {
			labels = append(labels[:0], finding.Code, string(finding.Severity), epoch, record.Participant, record.Model)
			metrics <- prometheus.MustNewConstMetric(c.finding, prometheus.GaugeValue,
				float64(finding.Part)/float64(finding.Whole), labels...)
		}
		clear(summed)
		for _, counter := range record.Counters {
			summed[dispositionLabels{
				disposition:   counter.Disposition,
				ghostReason:   counter.GhostReason,
				timeoutAction: counter.TimeoutAction,
				timeoutReason: counter.TimeoutReason,
			}] += counter.Count
		}
		for series, count := range summed {
			labels = append(labels[:0], string(series.disposition), series.ghostReason,
				series.timeoutAction, series.timeoutReason, epoch, record.Participant, record.Model)
			metrics <- prometheus.MustNewConstMetric(c.disposition, prometheus.GaugeValue, float64(count), labels...)
		}
	}
}
