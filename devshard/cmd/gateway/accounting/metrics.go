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

// Gauges rather than counters: a disposition moves when a nonce is reclassified, so a series goes
// down as well as up and a counter would misread every move as a reset.
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
	for _, record := range c.book.Query(QueryFilter{}) {
		identity := []string{strconv.FormatUint(record.EpochIndex, 10), record.Participant, record.Model}
		for desc, value := range map[*prometheus.Desc]float64{
			c.assigned:     float64(record.Assigned),
			c.chainMissed:  float64(record.ChainMissed),
			c.chainInvalid: float64(record.ChainInvalid),
			c.pending:      float64(record.Pending),
			c.unobserved:   float64(record.Unobserved),
			c.overcounted:  float64(record.Overcounted),
		} {
			metrics <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, identity...)
		}
		for _, finding := range record.Findings {
			metrics <- prometheus.MustNewConstMetric(c.finding, prometheus.GaugeValue,
				float64(finding.Part)/float64(finding.Whole),
				append([]string{finding.Code, string(finding.Severity)}, identity...)...)
		}
		summed := make(map[dispositionLabels]uint64, len(record.Counters))
		for _, counter := range record.Counters {
			summed[dispositionLabels{
				disposition:   counter.Disposition,
				ghostReason:   counter.GhostReason,
				timeoutAction: counter.TimeoutAction,
				timeoutReason: counter.TimeoutReason,
			}] += counter.Count
		}
		for labels, count := range summed {
			metrics <- prometheus.MustNewConstMetric(c.disposition, prometheus.GaugeValue, float64(count),
				append([]string{
					string(labels.disposition), labels.ghostReason, labels.timeoutAction, labels.timeoutReason,
				}, identity...)...)
		}
	}
}
