// Package setupreportobs exposes the /admin/v1/setup/report data as Prometheus
// metrics so Grafana can graph block height over time and show per-check status.
package setupreportobs

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// CheckStatus encoding used in metric values:
//
//	2 = PASS
//	1 = UNAVAILABLE
//	0 = FAIL
const (
	StatusPass        = float64(2)
	StatusUnavailable = float64(1)
	StatusFail        = float64(0)
)

// GPUStat holds per-device stats for a single GPU on an MLNode.
type GPUStat struct {
	NodeID      string
	GPUIndex    string
	Name        string
	TotalMemGB  float64
	FreeMemGB   float64
	UsedMemGB   float64
	Utilization float64 // -1 if not reported
	Temperature float64 // -1 if not reported
	Available   float64 // 1.0 = available, 0.0 = unavailable
}

// ReportSnapshot is a lightweight snapshot of the cached setup report.
// It is populated by the provider function wired in main.go.
type ReportSnapshot struct {
	// OverallStatus encodes the report-level status.
	OverallStatus float64

	// Checks is the per-check status (check_id → encoded status).
	Checks map[string]float64

	// BlockHeight is the latest known chain block height.
	BlockHeight int64

	// SecondsSinceBlock is seconds elapsed since the latest block was produced.
	SecondsSinceBlock float64

	// Counts of checks by result.
	PassedChecks      int
	FailedChecks      int
	UnavailableChecks int

	// Cold key details (from cold_key_configured check).
	ColdKeyAddress string
	ColdKeyPubkey  string

	// Permissions details (from permissions_granted check).
	PermissionsGranted int64
	PermissionsMissing int64

	// Feegrant details (from feegrant_allowance check).
	FeegrantAllowanceType string
	FeegrantColdKey       string
	FeegrantWarmKey       string
	FeegrantSpendLimit    string
	FeegrantExpiration    float64 // Unix timestamp; 0 if not set.

	// Epoch details (from active_in_epoch check).
	EpochNumber int64
	EpochWeight int64

	// Missed requests details (from missed_requests_threshold check).
	MissedPercentage float64
	MissedRequests   int64
	TotalRequests    int64
	InferenceCount   int64

	// GPU stats (from mlnode_* checks).
	GPUStats []GPUStat
}

var (
	providerMu sync.RWMutex
	provider   = func() *ReportSnapshot { return nil }

	overallStatusDesc = prometheus.NewDesc(
		"decentralized_api_setup_overall_status",
		"Overall setup report status: 2=PASS, 1=UNAVAILABLE, 0=FAIL.",
		nil, nil,
	)
	checkStatusDesc = prometheus.NewDesc(
		"decentralized_api_setup_check_status",
		"Per-check status from the setup report: 2=PASS, 1=UNAVAILABLE, 0=FAIL.",
		[]string{"check_id"}, nil,
	)
	blockHeightDesc = prometheus.NewDesc(
		"decentralized_api_block_height",
		"Latest chain block height observed by this node.",
		nil, nil,
	)
	secondsSinceBlockDesc = prometheus.NewDesc(
		"decentralized_api_seconds_since_block",
		"Seconds elapsed since the latest chain block was produced.",
		nil, nil,
	)
	checksPassedDesc = prometheus.NewDesc(
		"decentralized_api_setup_checks_passed",
		"Number of setup checks that passed.",
		nil, nil,
	)
	checksFailedDesc = prometheus.NewDesc(
		"decentralized_api_setup_checks_failed",
		"Number of setup checks that failed.",
		nil, nil,
	)
	checksUnavailableDesc = prometheus.NewDesc(
		"decentralized_api_setup_checks_unavailable",
		"Number of setup checks that were unavailable.",
		nil, nil,
	)

	// Cold key info: address and pubkey as labels, value = check status.
	coldKeyInfoDesc = prometheus.NewDesc(
		"decentralized_api_setup_cold_key_info",
		"Cold key details (address, pubkey). Value is check status: 2=PASS, 1=UNAVAILABLE, 0=FAIL.",
		[]string{"address", "pubkey"}, nil,
	)

	// Permissions counts.
	permissionsGrantedDesc = prometheus.NewDesc(
		"decentralized_api_setup_permissions_granted_count",
		"Number of authz permissions granted to the warm key.",
		nil, nil,
	)
	permissionsMissingDesc = prometheus.NewDesc(
		"decentralized_api_setup_permissions_missing_count",
		"Number of required authz permissions missing from the warm key.",
		nil, nil,
	)

	// Feegrant info: key addresses and allowance details as labels, value = check status.
	feegrantInfoDesc = prometheus.NewDesc(
		"decentralized_api_setup_feegrant_info",
		"Fee grant details (allowance type, key addresses, spend limit). Value is check status: 2=PASS, 1=UNAVAILABLE, 0=FAIL.",
		[]string{"allowance_type", "cold_key", "warm_key", "spend_limit"}, nil,
	)
	feegrantExpirationDesc = prometheus.NewDesc(
		"decentralized_api_setup_feegrant_expiration_unix",
		"Unix timestamp when the fee grant allowance expires. 0 if not set.",
		nil, nil,
	)

	// Epoch details.
	epochNumberDesc = prometheus.NewDesc(
		"decentralized_api_setup_epoch_number",
		"Current epoch number in which this participant is active.",
		nil, nil,
	)
	epochWeightDesc = prometheus.NewDesc(
		"decentralized_api_setup_epoch_weight",
		"Participant weight in the current epoch.",
		nil, nil,
	)

	// Missed requests details.
	missedPctDesc = prometheus.NewDesc(
		"decentralized_api_setup_missed_requests_pct",
		"Percentage of inference requests missed in the evaluation window.",
		nil, nil,
	)
	missedCountDesc = prometheus.NewDesc(
		"decentralized_api_setup_missed_requests_count",
		"Number of inference requests missed in the evaluation window.",
		nil, nil,
	)
	totalRequestsDesc = prometheus.NewDesc(
		"decentralized_api_setup_total_requests_count",
		"Total number of inference requests in the evaluation window.",
		nil, nil,
	)
	inferenceCountDesc = prometheus.NewDesc(
		"decentralized_api_setup_inference_count",
		"Number of inferences completed in the evaluation window.",
		nil, nil,
	)

	// GPU metrics (per node_id, gpu_index, name).
	gpuMemTotalDesc = prometheus.NewDesc(
		"decentralized_api_mlnode_gpu_memory_total_gb",
		"Total GPU memory in GB.",
		[]string{"node_id", "gpu_index", "name"}, nil,
	)
	gpuMemFreeDesc = prometheus.NewDesc(
		"decentralized_api_mlnode_gpu_memory_free_gb",
		"Free GPU memory in GB.",
		[]string{"node_id", "gpu_index", "name"}, nil,
	)
	gpuMemUsedDesc = prometheus.NewDesc(
		"decentralized_api_mlnode_gpu_memory_used_gb",
		"Used GPU memory in GB.",
		[]string{"node_id", "gpu_index", "name"}, nil,
	)
	gpuUtilizationDesc = prometheus.NewDesc(
		"decentralized_api_mlnode_gpu_utilization_pct",
		"GPU utilization percentage.",
		[]string{"node_id", "gpu_index", "name"}, nil,
	)
	gpuTemperatureDesc = prometheus.NewDesc(
		"decentralized_api_mlnode_gpu_temperature_c",
		"GPU temperature in Celsius.",
		[]string{"node_id", "gpu_index", "name"}, nil,
	)
	gpuAvailableDesc = prometheus.NewDesc(
		"decentralized_api_mlnode_gpu_available",
		"1 if GPU is available, 0 otherwise.",
		[]string{"node_id", "gpu_index", "name"}, nil,
	)
)

// SetProvider sets the global snapshot provider. Call this once from main.go
// after the admin server is started and able to produce reports.
func SetProvider(p func() *ReportSnapshot) {
	if p == nil {
		p = func() *ReportSnapshot { return nil }
	}
	providerMu.Lock()
	defer providerMu.Unlock()
	provider = p
}

// Collector implements prometheus.Collector for setup report metrics.
type Collector struct{}

// NewCollector returns a new Collector.
func NewCollector() *Collector { return &Collector{} }

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- overallStatusDesc
	ch <- checkStatusDesc
	ch <- blockHeightDesc
	ch <- secondsSinceBlockDesc
	ch <- checksPassedDesc
	ch <- checksFailedDesc
	ch <- checksUnavailableDesc
	ch <- coldKeyInfoDesc
	ch <- permissionsGrantedDesc
	ch <- permissionsMissingDesc
	ch <- feegrantInfoDesc
	ch <- feegrantExpirationDesc
	ch <- epochNumberDesc
	ch <- epochWeightDesc
	ch <- missedPctDesc
	ch <- missedCountDesc
	ch <- totalRequestsDesc
	ch <- inferenceCountDesc
	ch <- gpuMemTotalDesc
	ch <- gpuMemFreeDesc
	ch <- gpuMemUsedDesc
	ch <- gpuUtilizationDesc
	ch <- gpuTemperatureDesc
	ch <- gpuAvailableDesc
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	providerMu.RLock()
	p := provider
	providerMu.RUnlock()

	snap := p()
	if snap == nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(overallStatusDesc, prometheus.GaugeValue, snap.OverallStatus)
	ch <- prometheus.MustNewConstMetric(blockHeightDesc, prometheus.GaugeValue, float64(snap.BlockHeight))
	ch <- prometheus.MustNewConstMetric(secondsSinceBlockDesc, prometheus.GaugeValue, snap.SecondsSinceBlock)
	ch <- prometheus.MustNewConstMetric(checksPassedDesc, prometheus.GaugeValue, float64(snap.PassedChecks))
	ch <- prometheus.MustNewConstMetric(checksFailedDesc, prometheus.GaugeValue, float64(snap.FailedChecks))
	ch <- prometheus.MustNewConstMetric(checksUnavailableDesc, prometheus.GaugeValue, float64(snap.UnavailableChecks))

	for checkID, status := range snap.Checks {
		ch <- prometheus.MustNewConstMetric(checkStatusDesc, prometheus.GaugeValue, status, checkID)
	}

	// Cold key info.
	if snap.ColdKeyAddress != "" {
		coldKeyStatus := snap.Checks["cold_key_configured"]
		ch <- prometheus.MustNewConstMetric(coldKeyInfoDesc, prometheus.GaugeValue,
			coldKeyStatus, snap.ColdKeyAddress, snap.ColdKeyPubkey)
	}

	// Permissions counts.
	ch <- prometheus.MustNewConstMetric(permissionsGrantedDesc, prometheus.GaugeValue, float64(snap.PermissionsGranted))
	ch <- prometheus.MustNewConstMetric(permissionsMissingDesc, prometheus.GaugeValue, float64(snap.PermissionsMissing))

	// Feegrant info.
	if snap.FeegrantColdKey != "" {
		feegrantStatus := snap.Checks["feegrant_allowance"]
		ch <- prometheus.MustNewConstMetric(feegrantInfoDesc, prometheus.GaugeValue,
			feegrantStatus, snap.FeegrantAllowanceType, snap.FeegrantColdKey, snap.FeegrantWarmKey, snap.FeegrantSpendLimit)
	}
	ch <- prometheus.MustNewConstMetric(feegrantExpirationDesc, prometheus.GaugeValue, snap.FeegrantExpiration)

	// Epoch details.
	ch <- prometheus.MustNewConstMetric(epochNumberDesc, prometheus.GaugeValue, float64(snap.EpochNumber))
	ch <- prometheus.MustNewConstMetric(epochWeightDesc, prometheus.GaugeValue, float64(snap.EpochWeight))

	// Missed requests details.
	ch <- prometheus.MustNewConstMetric(missedPctDesc, prometheus.GaugeValue, snap.MissedPercentage)
	ch <- prometheus.MustNewConstMetric(missedCountDesc, prometheus.GaugeValue, float64(snap.MissedRequests))
	ch <- prometheus.MustNewConstMetric(totalRequestsDesc, prometheus.GaugeValue, float64(snap.TotalRequests))
	ch <- prometheus.MustNewConstMetric(inferenceCountDesc, prometheus.GaugeValue, float64(snap.InferenceCount))

	// GPU stats.
	for _, gpu := range snap.GPUStats {
		labels := []string{gpu.NodeID, gpu.GPUIndex, gpu.Name}
		ch <- prometheus.MustNewConstMetric(gpuMemTotalDesc, prometheus.GaugeValue, gpu.TotalMemGB, labels...)
		ch <- prometheus.MustNewConstMetric(gpuMemFreeDesc, prometheus.GaugeValue, gpu.FreeMemGB, labels...)
		ch <- prometheus.MustNewConstMetric(gpuMemUsedDesc, prometheus.GaugeValue, gpu.UsedMemGB, labels...)
		ch <- prometheus.MustNewConstMetric(gpuAvailableDesc, prometheus.GaugeValue, gpu.Available, labels...)
		if gpu.Utilization >= 0 {
			ch <- prometheus.MustNewConstMetric(gpuUtilizationDesc, prometheus.GaugeValue, gpu.Utilization, labels...)
		}
		if gpu.Temperature >= 0 {
			ch <- prometheus.MustNewConstMetric(gpuTemperatureDesc, prometheus.GaugeValue, gpu.Temperature, labels...)
		}
	}
}
