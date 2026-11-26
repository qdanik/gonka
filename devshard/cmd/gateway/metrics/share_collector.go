package metrics

import "github.com/prometheus/client_golang/prometheus"

// ShareRow is one host's targets and pass outcomes for one model.
type ShareRow struct {
	Participant string
	Model       string
	Target      float64
	Offered     float64
	Answered    uint64
	Missed      uint64
	Owed        uint64
	Unvoted     uint64
}

// ShareSources takes its readers as functions, so metrics keeps no edge to the scheduler.
type ShareSources struct {
	Models func() []string
	Hosts  func(model string) []string
	Row    func(participant, model string) (ShareRow, bool)
	// Mode is the host_load_mode in force for the model: 0 off, 1 weight, 2 balance.
	Mode func(model string) float64
}

// ShareCollector reads the share ledger at scrape time.
type ShareCollector struct {
	sources ShareSources

	target  *prometheus.Desc
	offered *prometheus.Desc
	passes  *prometheus.Desc
	mode    *prometheus.Desc
}

func NewShareCollector(sources ShareSources) *ShareCollector {
	return &ShareCollector{
		sources: sources,
		target:  gaugeDesc("devshard_gateway_participant_target_share", "The share of a model's nonces host_load_mode targets a participant with.", "participant_key", "model"),
		offered: gaugeDesc("devshard_gateway_participant_offered_share", "The share of a model's decayed offered nonces a participant was bound.", "participant_key", "model"),
		passes:  counterDesc("devshard_gateway_participant_passes_total", "Passes by what became of them: answered, missed, owed or unvoted.", "participant_key", "model", "outcome"),
		mode:    gaugeDesc("devshard_gateway_host_load_mode", "The host_load_mode in force for a model: 0 off, 1 weight, 2 balance.", "model"),
	}
}

func (c *ShareCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range []*prometheus.Desc{c.target, c.offered, c.passes, c.mode} {
		ch <- desc
	}
}

func (c *ShareCollector) Collect(ch chan<- prometheus.Metric) {
	for _, model := range c.sources.Models() {
		gauge(ch, c.mode, c.sources.Mode(model), model)
		for _, participant := range c.sources.Hosts(model) {
			row, held := c.sources.Row(participant, model)
			if !held {
				continue
			}
			gauge(ch, c.target, row.Target, participant, model)
			gauge(ch, c.offered, row.Offered, participant, model)
			counter(ch, c.passes, float64(row.Answered), participant, model, "answered")
			counter(ch, c.passes, float64(row.Missed), participant, model, "missed")
			counter(ch, c.passes, float64(row.Owed), participant, model, "owed")
			counter(ch, c.passes, float64(row.Unvoted), participant, model, "unvoted")
		}
	}
}
