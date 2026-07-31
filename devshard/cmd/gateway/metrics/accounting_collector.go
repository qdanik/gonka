package metrics

import (
	"devshard/cmd/gateway/store"

	"github.com/prometheus/client_golang/prometheus"
)

// LedgerSource is the accounting ledger's own count of what it wrote and what it lost; *store.Ledger
// satisfies it.
type LedgerSource interface {
	Stats() store.LedgerStats
}

type AccountingCollector struct {
	ledger LedgerSource

	written *prometheus.Desc
	lost    *prometheus.Desc
}

func NewAccountingCollector(ledger LedgerSource) *AccountingCollector {
	return &AccountingCollector{
		ledger:  ledger,
		written: counterDesc("devshard_gateway_accounting_rows_written_total", "Accounting rows the ledger wrote."),
		lost:    counterDesc("devshard_gateway_accounting_rows_lost_total", "Accounting rows the ledger lost, by cause.", "cause"),
	}
}

func (c *AccountingCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.written
	ch <- c.lost
}

func (c *AccountingCollector) Collect(ch chan<- prometheus.Metric) {
	if c.ledger == nil {
		return
	}
	stats := c.ledger.Stats()
	counter(ch, c.written, float64(stats.Written))
	counter(ch, c.lost, float64(stats.Dropped), "shed")
	counter(ch, c.lost, float64(stats.Failed), "write_failed")
}
