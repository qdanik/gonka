package participantobs

import (
	"sort"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type RewardSnapshot struct {
	Epoch       uint64
	RewardedGNK float64
	Claimed     bool
}

type ModelStatus struct {
	ModelID    string
	Status     string
	DelegateTo string
}

type MLNodeSnapshot struct {
	NodeID           string
	CurrentStatus    string
	IntendedStatus   string
	PocStatus        string
	ConfiguredModels string
	ActiveModels     string
	PreservedModels  string
	Hardware         string
	Version          string
	Weights          map[string]float64
	EffectiveWeights map[string]float64
	Throughputs      map[string]float64
}

type Snapshot struct {
	Address           string
	ValidatorKey      string
	Status            string
	ParticipantStatus string
	CurrentPhase      string
	EffectiveWeight   float64
	ConfirmationWeight float64
	RewardHistory     []RewardSnapshot
	ModelStatuses     []ModelStatus
	MLNodes           []MLNodeSnapshot
}

var (
	snapshotProviderMu sync.RWMutex
	snapshotProvider   = func() Snapshot { return Snapshot{} }

	infoDesc = prometheus.NewDesc(
		"decentralized_api_participant_info",
		"Participant identity and current runtime phase exposed by decentralized-api.",
		[]string{"address", "validator_key", "status", "participant_status", "phase"},
		nil,
	)
	epochRewardedGNKDesc = prometheus.NewDesc(
		"decentralized_api_participant_epoch_rewarded_gnk",
		"Rewarded GNK for the local participant by epoch, converted from ngonka.",
		[]string{"epoch", "claimed"},
		nil,
	)
	modelStatusDesc = prometheus.NewDesc(
		"decentralized_api_participant_model_status",
		"Model coverage or delegation status for the local participant.",
		[]string{"model_id", "status", "delegate_to"},
		nil,
	)
	mlNodeEffectiveWeightDesc = prometheus.NewDesc(
		"decentralized_api_participant_mlnode_effective_weight",
		"Current effective weight assigned to an ML node for the local participant.",
		[]string{"node_id", "model_id", "current_status"},
		nil,
	)
)

type Collector struct {
	provider func() Snapshot
}

func NewCollector() *Collector {
	return &Collector{}
}

func SetSnapshotProvider(provider func() Snapshot) {
	if provider == nil {
		provider = func() Snapshot { return Snapshot{} }
	}

	snapshotProviderMu.Lock()
	defer snapshotProviderMu.Unlock()
	snapshotProvider = provider
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- infoDesc
	ch <- epochRewardedGNKDesc
	ch <- modelStatusDesc
	ch <- mlNodeEffectiveWeightDesc
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	provider := c.provider
	if provider == nil {
		snapshotProviderMu.RLock()
		provider = snapshotProvider
		snapshotProviderMu.RUnlock()
	}
	if provider == nil {
		return
	}

	snapshot := provider()

	if snapshot.Address != "" {
		ch <- prometheus.MustNewConstMetric(
			infoDesc,
			prometheus.GaugeValue,
			1,
			snapshot.Address,
			snapshot.ValidatorKey,
			snapshot.Status,
			snapshot.ParticipantStatus,
			snapshot.CurrentPhase,
		)
	}

	for _, reward := range snapshot.RewardHistory {
		ch <- prometheus.MustNewConstMetric(
			epochRewardedGNKDesc,
			prometheus.GaugeValue,
			reward.RewardedGNK,
			strconv.FormatUint(reward.Epoch, 10),
			strconv.FormatBool(reward.Claimed),
		)
	}

	for _, status := range snapshot.ModelStatuses {
		ch <- prometheus.MustNewConstMetric(
			modelStatusDesc,
			prometheus.GaugeValue,
			1,
			status.ModelID,
			status.Status,
			status.DelegateTo,
		)
	}

	for _, node := range snapshot.MLNodes {
		effectiveWeightModels := sortedFloatMapKeys(node.EffectiveWeights)
		for _, modelID := range effectiveWeightModels {
			ch <- prometheus.MustNewConstMetric(
				mlNodeEffectiveWeightDesc,
				prometheus.GaugeValue,
				node.EffectiveWeights[modelID],
				node.NodeID,
				modelID,
				node.CurrentStatus,
			)
		}
	}
}

func sortedFloatMapKeys(values map[string]float64) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}