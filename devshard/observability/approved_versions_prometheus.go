package observability

import (
	"sort"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type ApprovedVersion struct {
	Name   string
	Binary string
	SHA256 string
}

var (
	approvedVersionsProviderMu sync.RWMutex
	approvedVersionsProvider   = func() []ApprovedVersion { return nil }

	approvedVersionsTotalDesc = prometheus.NewDesc(
		"decentralized_api_devshard_approved_versions_total",
		"Number of approved devshard versions currently known to decentralized-api.",
		nil,
		nil,
	)
	approvedVersionInfoDesc = prometheus.NewDesc(
		"decentralized_api_devshard_approved_version_info",
		"Approved devshard versions currently known to decentralized-api.",
		[]string{"version", "binary", "sha256"},
		nil,
	)
)

type ApprovedVersionsCollector struct{}

func NewApprovedVersionsCollector() *ApprovedVersionsCollector {
	return &ApprovedVersionsCollector{}
}

func SetApprovedVersionsProvider(provider func() []ApprovedVersion) {
	if provider == nil {
		provider = func() []ApprovedVersion { return nil }
	}

	approvedVersionsProviderMu.Lock()
	defer approvedVersionsProviderMu.Unlock()
	approvedVersionsProvider = provider
}

func (c *ApprovedVersionsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- approvedVersionsTotalDesc
	ch <- approvedVersionInfoDesc
}

func (c *ApprovedVersionsCollector) Collect(ch chan<- prometheus.Metric) {
	approvedVersionsProviderMu.RLock()
	provider := approvedVersionsProvider
	approvedVersionsProviderMu.RUnlock()

	versions := provider()
	sort.Slice(versions, func(i, j int) bool {
		return versions[i].Name < versions[j].Name
	})

	count := 0
	for _, version := range versions {
		if version.Name == "" {
			continue
		}
		count++
		ch <- prometheus.MustNewConstMetric(
			approvedVersionInfoDesc,
			prometheus.GaugeValue,
			1,
			version.Name,
			version.Binary,
			version.SHA256,
		)
	}

	ch <- prometheus.MustNewConstMetric(
		approvedVersionsTotalDesc,
		prometheus.GaugeValue,
		float64(count),
	)
}