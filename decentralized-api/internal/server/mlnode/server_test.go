package mlnode

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"decentralized-api/apiconfig"
	"decentralized-api/broker"
	"decentralized-api/chainphase"
	"decentralized-api/cosmosclient"
	"decentralized-api/mlnodeclient"
	"decentralized-api/poc/artifacts"

	"github.com/productscience/inference/x/inference/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type stubBrokerChainBridge struct {
	models []string
}

const (
	testModelA = "model-a"
	testModelB = "org/model-b"
)

func (s stubBrokerChainBridge) GetHardwareNodes() (*types.QueryHardwareNodesResponse, error) {
	return &types.QueryHardwareNodesResponse{}, nil
}

func (s stubBrokerChainBridge) SubmitHardwareDiff(diff *types.MsgSubmitHardwareDiff) error {
	return nil
}

func (s stubBrokerChainBridge) GetBlockHash(height int64) (string, error) {
	return "", nil
}

func (s stubBrokerChainBridge) GetGovernanceModels() (*types.QueryModelsAllResponse, error) {
	models := make([]types.Model, 0, len(s.models))
	for _, modelID := range s.models {
		models = append(models, types.Model{Id: modelID})
	}
	return &types.QueryModelsAllResponse{Model: models}, nil
}

func (s stubBrokerChainBridge) GetCurrentEpochGroupData() (*types.QueryCurrentEpochGroupDataResponse, error) {
	return &types.QueryCurrentEpochGroupDataResponse{}, nil
}

func (s stubBrokerChainBridge) GetEpochGroupDataByModelId(pocHeight uint64, modelId string) (*types.QueryGetEpochGroupDataResponse, error) {
	return &types.QueryGetEpochGroupDataResponse{}, nil
}

func (s stubBrokerChainBridge) GetPreservedNodesSnapshot() (*types.QueryPreservedNodesSnapshotResponse, error) {
	return &types.QueryPreservedNodesSnapshotResponse{Found: false}, nil
}

func (s stubBrokerChainBridge) GetParams() (*types.QueryParamsResponse, error) {
	return &types.QueryParamsResponse{}, nil
}

func newMLNodeTestBroker(t *testing.T, phase types.EpochPhase, modelIDs ...string) *broker.Broker {
	t.Helper()

	tracker := &chainphase.ChainPhaseTracker{}
	tracker.Update(
		chainphase.BlockInfo{Height: 110, Hash: "test-hash"},
		&types.Epoch{Index: 1, PocStartBlockHeight: 100},
		&types.EpochParams{
			EpochLength:           1000,
			EpochShift:            0,
			PocStageDuration:      100,
			PocExchangeDuration:   50,
			PocValidationDelay:    10,
			PocValidationDuration: 100,
		},
		true,
		nil,
	)
	testBroker := broker.NewBroker(
		stubBrokerChainBridge{models: modelIDs},
		tracker,
		nil,
		"http://callback",
		mlnodeclient.NewMockClientFactory(),
		&apiconfig.ConfigManager{},
	)

	switch phase {
	case types.PoCValidatePhase:
		tracker.Update(chainphase.BlockInfo{Height: 220, Hash: "test-hash"}, &types.Epoch{Index: 1, PocStartBlockHeight: 100}, &types.EpochParams{
			EpochLength:           1000,
			EpochShift:            0,
			PocStageDuration:      100,
			PocExchangeDuration:   50,
			PocValidationDelay:    10,
			PocValidationDuration: 100,
		}, true, nil)
	case types.PoCGeneratePhase:
		// already set above
	}

	models := make(map[string]apiconfig.ModelConfig, len(modelIDs))
	for _, modelID := range modelIDs {
		models[modelID] = apiconfig.ModelConfig{}
	}

	loadResp := testBroker.LoadNodeToBroker(&apiconfig.InferenceNodeConfig{
		Host:             "127.0.0.1",
		InferenceSegment: "/inference",
		InferencePort:    8081,
		PoCSegment:       "/poc",
		PoCPort:          8082,
		Models:           models,
		Id:            "node-1",
		MaxConcurrent: 1,
	})
	resp := <-loadResp
	if resp.Error != nil {
		t.Fatalf("LoadNodeToBroker failed: %v", resp.Error)
	}

	return testBroker
}

func TestV2GeneratedCallbackRequiresModelScopedRoute(t *testing.T) {
	artifactStore := artifacts.NewManagedArtifactStore(t.TempDir(), 3)
	defer artifactStore.Close()

	server := NewServer(nil, newMLNodeTestBroker(t, types.PoCGeneratePhase, testModelA, testModelB), WithArtifactStore(artifactStore))

	body, err := json.Marshal(map[string]any{
		"block_hash":   "abc",
		"block_height": 100,
		"public_key":   "pub",
		"node_id":      1,
		"artifacts": []map[string]any{
			{"nonce": 1, "vector_b64": base64.StdEncoding.EncodeToString([]byte{1, 2, 3})},
		},
	})
	assert.NoError(t, err)

	unscopedReq := httptest.NewRequest(http.MethodPost, "/v2/poc-batches/generated", bytes.NewReader(body))
	unscopedReq.Header.Set("Content-Type", "application/json")
	unscopedRec := httptest.NewRecorder()
	server.e.ServeHTTP(unscopedRec, unscopedReq)
	assert.Equal(t, http.StatusNotFound, unscopedRec.Code)

	scopedReq := httptest.NewRequest(http.MethodPost, "/v2/poc-batches/model-a/generated", bytes.NewReader(body))
	scopedReq.Header.Set("Content-Type", "application/json")
	scopedRec := httptest.NewRecorder()
	server.e.ServeHTTP(scopedRec, scopedReq)
	assert.Equal(t, http.StatusOK, scopedRec.Code)

	secondReq := httptest.NewRequest(http.MethodPost, "/v2/poc-batches/org%252Fmodel-b/generated", bytes.NewReader(body))
	secondReq.Header.Set("Content-Type", "application/json")
	secondRec := httptest.NewRecorder()
	server.e.ServeHTTP(secondRec, secondReq)
	assert.Equal(t, http.StatusOK, secondRec.Code)

	modelStore, err := artifactStore.GetStore(100, testModelA)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), modelStore.Count())

	otherStore, err := artifactStore.GetStore(100, testModelB)
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), otherStore.Count())
}

func TestV2ValidatedCallbackUsesPathModelID(t *testing.T) {
	mockRecorder := &cosmosclient.MockCosmosMessageClient{}
	mockRecorder.
		On("SubmitPocValidationsV2", mock.MatchedBy(func(msg *types.MsgSubmitPocValidationsV2) bool {
			return msg != nil &&
				msg.PocStageStartBlockHeight == 100 &&
				len(msg.Validations) == 1 &&
				msg.Validations[0].ModelId == testModelA
		})).
		Return(nil).
		Once()

	server := NewServer(mockRecorder, newMLNodeTestBroker(t, types.PoCValidatePhase, testModelA))

	body, err := json.Marshal(map[string]any{
		"block_hash":       "abc",
		"block_height":     100,
		"public_key":       "02b463f7f42e5f4f1d2d0bb1c4b9f8d2c3b1a09c72fbc5d0b8d4c53b37f6f2a540",
		"node_id":          1,
		"n_total":          5,
		"n_mismatch":       0,
		"mismatch_nonces":  []int{},
		"p_value":          1.0,
		"fraud_detected":   false,
	})
	assert.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v2/poc-batches/model-a/validated", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	mockRecorder.AssertExpectations(t)
}

func TestDevshardServiceDiscoveryUsesConfiguredVersions(t *testing.T) {
	configManager := &apiconfig.ConfigManager{}
	configManager.SetDevshardVersions(apiconfig.DevshardVersionsCache{
		Versions: []apiconfig.DevshardVersion{
			{Name: "v1.7.2", Binary: "devshardd"},
			{Name: "", Binary: "skip-me"},
			{Name: "v1.7.0", Binary: "devshardd"},
		},
	})

	server := NewServer(nil, nil, WithConfigManager(configManager))
	req := httptest.NewRequest(http.MethodGet, "/sd/devshardd", nil)
	rec := httptest.NewRecorder()

	server.e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	var groups []prometheusTargetGroup
	err := json.Unmarshal(rec.Body.Bytes(), &groups)
	assert.NoError(t, err)
	assert.Len(t, groups, 2)
	assert.Equal(t, []string{"versiond:8080"}, groups[0].Targets)
	assert.Equal(t, "/v1.7.0/metrics", groups[0].Labels["__metrics_path__"])
	assert.Equal(t, "v1.7.0", groups[0].Labels["version"])
	assert.Equal(t, "devshardd", groups[0].Labels["service"])
	assert.Equal(t, []string{"versiond:8080"}, groups[1].Targets)
	assert.Equal(t, "/v1.7.2/metrics", groups[1].Labels["__metrics_path__"])
	assert.Equal(t, "v1.7.2", groups[1].Labels["version"])
	assert.Equal(t, "devshardd", groups[1].Labels["service"])
}

func TestMetricsRouteExposesPrometheusMetrics(t *testing.T) {
	server := NewServer(nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	server.e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, strings.Contains(rec.Header().Get("Content-Type"), "text/plain"))
	assert.Contains(t, rec.Body.String(), "promhttp_metric_handler_requests_in_flight")
}

func TestMetricsRouteExposesApprovedDevshardVersionMetrics(t *testing.T) {
	configManager := &apiconfig.ConfigManager{}
	configManager.SetDevshardVersions(apiconfig.DevshardVersionsCache{
		Versions: []apiconfig.DevshardVersion{
			{Name: "v1.7.2", Binary: "devshardd", SHA256: "sha-a"},
			{Name: "v1.7.0", Binary: "devshardd", SHA256: "sha-b"},
		},
	})

	server := NewServer(nil, nil, WithConfigManager(configManager))
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	server.e.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "decentralized_api_devshard_approved_versions_total 2")
	assert.Contains(t, rec.Body.String(), "decentralized_api_devshard_approved_version_info")
	assert.Contains(t, rec.Body.String(), "version=\"v1.7.0\"")
	assert.Contains(t, rec.Body.String(), "version=\"v1.7.2\"")
}
