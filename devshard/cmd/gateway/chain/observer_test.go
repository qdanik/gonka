package chain

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/internal/leakcheck"
)

// phaseObserverStub serves the public-API endpoints PhaseObserver polls plus the chain-REST
// preserved-nodes-snapshot and miner /v1/versions endpoints; status/body are mutable mid-test to
// simulate phase transitions and errors.
type phaseObserverStub struct {
	mu                 sync.Mutex
	epochStatus        int
	epochBody          string
	participantsStatus int
	participantsBody   string
	preserved          *PreservedNodes
	preservedFound     bool
	preservedErr       error
	preservedHits      int
	maxNonce           uint64
	maxNonceHeld       bool
	maxNonceErr        error
	maxNonceHits       int
	versionsStatus     int
	versionsBody       string
}

func newPhaseObserverStub() *phaseObserverStub {
	return &phaseObserverStub{
		epochStatus:        http.StatusOK,
		participantsStatus: http.StatusOK,
		versionsStatus:     http.StatusNotFound,
	}
}

func (s *phaseObserverStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch r.URL.Path {
		case "/v1/epochs/latest":
			w.WriteHeader(s.epochStatus)
			w.Write([]byte(s.epochBody))
		case "/v1/epochs/current/participants":
			w.WriteHeader(s.participantsStatus)
			w.Write([]byte(s.participantsBody))
		case "/v1/versions":
			w.WriteHeader(s.versionsStatus)
			w.Write([]byte(s.versionsBody))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func (s *phaseObserverStub) setEpoch(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epochStatus = status
	s.epochBody = body
}

func (s *phaseObserverStub) setParticipants(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.participantsStatus = status
	s.participantsBody = body
}

// PreservedNodes and MaxNonce make the stub the observer's chain reader, so a test programs both the
// public API and the chain from one place and still counts what was asked for.
func (s *phaseObserverStub) PreservedNodes(context.Context) (*PreservedNodes, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preservedHits++
	return s.preserved, s.preservedFound, s.preservedErr
}

func (s *phaseObserverStub) MaxNonce(context.Context) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxNonceHits++
	return s.maxNonce, s.maxNonceHeld, s.maxNonceErr
}

func (s *phaseObserverStub) setPreservedNodes(snapshot *PreservedNodes, found bool, failure error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preserved, s.preservedFound, s.preservedErr = snapshot, found, failure
}

func (s *phaseObserverStub) setMaxNonceValue(value uint64, held bool, failure error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxNonce, s.maxNonceHeld, s.maxNonceErr = value, held, failure
}

func (s *phaseObserverStub) setVersions(status int, body string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versionsStatus = status
	s.versionsBody = body
}

func (s *phaseObserverStub) preservedHitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.preservedHits
}

func (s *phaseObserverStub) maxNonceHitCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxNonceHits
}

func observerEpochJSON(blockHeight int64, epochIndex uint64, phase EpochPhase) string {
	return fmt.Sprintf(`{
		"block_height": %d,
		"phase": %q,
		"latest_epoch": {"index": %d, "poc_start_block_height": 0},
		"is_confirmation_poc_active": false
	}`, blockHeight, string(phase), epochIndex)
}

func observerParticipantsJSON(participantAddr, inferenceURL string, pocWeight uint64) string {
	return fmt.Sprintf(`{
		"active_participants": {
			"participants": [
				{
					"index": %q,
					"inference_url": %q,
					"models": ["model-a"],
					"ml_nodes": [{"ml_nodes": [{"node_id": "node-1", "timeslot_allocation": [true, true], "poc_weight": %d}]}]
				}
			]
		}
	}`, participantAddr, inferenceURL, pocWeight)
}

// observerEpochSwitchJSON is an Inference-phase epoch body with configurable epoch_stages/
// next_epoch_stages set_new_validators heights, used to drive the EpochSwitchBlockHeight ladder.
func observerEpochSwitchJSON(blockHeight, setNewValidators, nextSetNewValidators int64) string {
	return fmt.Sprintf(`{
		"block_height": %d,
		"phase": "Inference",
		"latest_epoch": {"index": 7, "poc_start_block_height": 900},
		"epoch_stages": {"set_new_validators": %d},
		"next_epoch_stages": {"set_new_validators": %d},
		"is_confirmation_poc_active": false
	}`, blockHeight, setNewValidators, nextSetNewValidators)
}

// observerEpochPoCJSON is a PoCGenerate-or-Validate epoch body with an explicit PoC start height.
func observerEpochPoCJSON(phase EpochPhase, pocStartHeight int64) string {
	return fmt.Sprintf(`{
		"block_height": 1000,
		"phase": %q,
		"latest_epoch": {"index": 7, "poc_start_block_height": %d},
		"is_confirmation_poc_active": false
	}`, string(phase), pocStartHeight)
}

// observerEpochConfirmationJSON is an Inference epoch body with an active confirmation-PoC event.
func observerEpochConfirmationJSON(confirmationPhase ConfirmationPoCPhase, triggerHeight int64) string {
	return fmt.Sprintf(`{
		"block_height": 1000,
		"phase": "Inference",
		"latest_epoch": {"index": 7, "poc_start_block_height": 900},
		"is_confirmation_poc_active": true,
		"active_confirmation_poc_event": {"phase": %q, "trigger_height": %d}
	}`, string(confirmationPhase), triggerHeight)
}

// observerTwoMinerParticipantsJSON has gonka1abc with a timeslot-non-preserved node1 (weight 100)
// and gonka1bcd with a timeslot-preserved node2 (weight 40), both serving model-a from baseURL.
func observerTwoMinerParticipantsJSON(baseURL string) string {
	return fmt.Sprintf(`{
		"active_participants": {
			"participants": [
				{
					"index": "gonka1abc",
					"inference_url": %q,
					"models": ["model-a"],
					"ml_nodes": [{"ml_nodes": [{"node_id": "node1", "timeslot_allocation": [true, false], "poc_weight": 100}]}]
				},
				{
					"index": "gonka1bcd",
					"inference_url": %q,
					"models": ["model-a"],
					"ml_nodes": [{"ml_nodes": [{"node_id": "node2", "timeslot_allocation": [true, true], "poc_weight": 40}]}]
				}
			]
		}
	}`, baseURL, baseURL)
}

// observerPreservedSnapshotJSON is a found preserved-nodes snapshot listing only gonka1abc/node1
// under model-a at the given anchor height.
func observerPreservedSnapshot(anchorHeight int64) *PreservedNodes {
	return &PreservedNodes{
		EpisodeAnchorHeight: anchorHeight,
		Models: []PreservedModel{{
			ModelID:      "model-a",
			Participants: []PreservedParticipant{{ParticipantID: "gonka1abc", NodeIDs: []string{"node1"}}},
		}},
	}
}

// newPoCPhaseObserver builds an observer over the stub server; a nil chainReader is the shape of a
// gateway with no chain access, and refresh is driven directly by the tests, never via Start.
func newPoCPhaseObserver(t *testing.T, server *httptest.Server, chainReader Reader) *PhaseObserver {
	t.Helper()
	clock := newFakeClock(time.Unix(100, 0))
	cfg := ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Hour,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	}
	if chainReader != nil {
		cfg.Chain = chainReader
	}
	observer, err := NewPhaseObserver(cfg)
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}
	return observer
}

// waitForSnapshot drains snapshots until predicate matches, failing the test if timeout elapses first.
func waitForSnapshot(t *testing.T, snapshots <-chan PhaseSnapshot, timeout time.Duration, predicate func(PhaseSnapshot) bool) PhaseSnapshot {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case snapshot := <-snapshots:
			if predicate(snapshot) {
				return snapshot
			}
		case <-deadline:
			t.Fatal("timed out waiting for expected snapshot")
			return PhaseSnapshot{}
		}
	}
}

func TestNewPhaseObserver_EmptyPublicAPIBaseURL_Errors(t *testing.T) {
	_, err := NewPhaseObserver(ObserverConfig{})
	if err == nil {
		t.Fatal("NewPhaseObserver() error = nil, want error for empty PublicAPIBaseURL")
	}
}

func TestNewPhaseObserver_AppliesDefaultsForZeroFields(t *testing.T) {
	observer, err := NewPhaseObserver(ObserverConfig{PublicAPIBaseURL: "http://chain.example.invalid"})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}
	if observer.pollInterval != DefaultObserverPollInterval {
		t.Errorf("pollInterval = %v, want %v", observer.pollInterval, DefaultObserverPollInterval)
	}
	if observer.client == nil {
		t.Error("HTTP client default not applied")
	} else if observer.client.Timeout != 5*time.Second {
		t.Errorf("default client timeout = %v, want 5s", observer.client.Timeout)
	}
	if observer.now == nil {
		t.Error("now default not applied")
	}
	if observer.versions == nil {
		t.Error("versions cache not constructed")
	}
}

// TestPhaseObserver_StartPublishesFirstSnapshotAndNotifiesSubscribers covers the immediate first
// refresh on Start (a long PollInterval proves this isn't waiting on the ticker), the derived
// epoch fields, and weights/inferenceURLs folded in from participants.
func TestPhaseObserver_StartPublishesFirstSnapshotAndNotifiesSubscribers(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1000, 7, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 42))

	clock := newFakeClock(time.Unix(100, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Hour,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	snapshots := make(chan PhaseSnapshot, 16)
	defer observer.Subscribe(func(s PhaseSnapshot) { snapshots <- s })()

	observer.Start(context.Background())
	defer observer.Stop()

	first := waitForSnapshot(t, snapshots, 2*time.Second, func(PhaseSnapshot) bool { return true })

	if first.BlockHeight != 1000 {
		t.Errorf("BlockHeight = %d, want 1000", first.BlockHeight)
	}
	if first.EpochIndex != 7 {
		t.Errorf("EpochIndex = %d, want 7", first.EpochIndex)
	}
	if first.EpochPhase != EpochPhaseInference {
		t.Errorf("EpochPhase = %q, want %q", first.EpochPhase, EpochPhaseInference)
	}
	if first.RequestsBlocked {
		t.Error("RequestsBlocked = true, want false for Inference phase")
	}
	if first.BlockReason != BlockReasonNone {
		t.Errorf("BlockReason = %q, want empty", first.BlockReason)
	}
	if !first.LastUpdatedAt.Equal(time.Unix(100, 0)) {
		t.Errorf("LastUpdatedAt = %v, want %v", first.LastUpdatedAt, time.Unix(100, 0))
	}
	wantWeights := map[string]float64{"gonka1abc": 42}
	if !reflect.DeepEqual(first.CurrentWeights, wantWeights) {
		t.Errorf("CurrentWeights = %v, want %v", first.CurrentWeights, wantWeights)
	}
	wantURLs := map[string]string{"gonka1abc": server.URL}
	if !reflect.DeepEqual(first.InferenceURLs, wantURLs) {
		t.Errorf("InferenceURLs = %v, want %v", first.InferenceURLs, wantURLs)
	}
}

// TestPhaseObserver_BlockedPoCPhaseSetsRequestsBlockedAndReason covers a PoC-blocking epoch phase
// via a single direct refresh (no ticker involved).
func TestPhaseObserver_BlockedPoCPhaseSetsRequestsBlockedAndReason(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(500, 3, EpochPhasePoCGenerate))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 10))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if !snapshot.RequestsBlocked {
		t.Error("RequestsBlocked = false, want true for PoCGenerate phase")
	}
	if snapshot.BlockReason != BlockReasonPoC {
		t.Errorf("BlockReason = %q, want %q", snapshot.BlockReason, BlockReasonPoC)
	}
	if snapshot.EpochPhase != EpochPhasePoCGenerate {
		t.Errorf("EpochPhase = %q, want %q", snapshot.EpochPhase, EpochPhasePoCGenerate)
	}
}

// TestPhaseObserver_EpochSwitchBlockHeightUsesFallbackLadder covers the top two ladder rungs end
// to end through Snapshot(): the current epoch's SetNewValidators wins when present, and omitting
// it falls through to the next epoch's SetNewValidators.
func TestPhaseObserver_EpochSwitchBlockHeightUsesFallbackLadder(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 42))

	observer := newPoCPhaseObserver(t, server, nil)

	stub.setEpoch(http.StatusOK, observerEpochSwitchJSON(1000, 1200, 1300))
	observer.refresh(context.Background())
	if got := observer.Snapshot().EpochSwitchBlockHeight; got != 1200 {
		t.Fatalf("EpochSwitchBlockHeight = %d, want 1200 (current epoch set_new_validators)", got)
	}

	stub.setEpoch(http.StatusOK, observerEpochSwitchJSON(1000, 0, 1300))
	observer.refresh(context.Background())
	if got := observer.Snapshot().EpochSwitchBlockHeight; got != 1300 {
		t.Fatalf("EpochSwitchBlockHeight = %d, want 1300 (next epoch set_new_validators, primary omitted)", got)
	}
}

// TestPhaseObserver_TickerDrivesRepeatedRefreshOnPhaseChange covers the ticker loop actually
// re-polling: a stub phase change made after the first notification must surface in a later one.
func TestPhaseObserver_TickerDrivesRepeatedRefreshOnPhaseChange(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(100, 1, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 5))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Millisecond,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	snapshots := make(chan PhaseSnapshot, 256)
	defer observer.Subscribe(func(s PhaseSnapshot) { snapshots <- s })()

	observer.Start(context.Background())
	defer observer.Stop()

	waitForSnapshot(t, snapshots, 2*time.Second, func(s PhaseSnapshot) bool { return s.EpochPhase == EpochPhaseInference })

	stub.setEpoch(http.StatusOK, observerEpochJSON(200, 2, EpochPhasePoCValidate))

	changed := waitForSnapshot(t, snapshots, 2*time.Second, func(s PhaseSnapshot) bool { return s.EpochPhase == EpochPhasePoCValidate })
	if !changed.RequestsBlocked || changed.BlockReason != BlockReasonPoC {
		t.Errorf("after phase change: RequestsBlocked=%v BlockReason=%q, want true/%q", changed.RequestsBlocked, changed.BlockReason, BlockReasonPoC)
	}
	if changed.BlockHeight != 200 || changed.EpochIndex != 2 {
		t.Errorf("BlockHeight/EpochIndex = %d/%d, want 200/2", changed.BlockHeight, changed.EpochIndex)
	}
}

// TestPhaseObserver_CancelStopsFurtherNotifications drives refresh directly (no ticker) so the
// cancel-vs-publish ordering is deterministic instead of racing a background poll loop.
func TestPhaseObserver_CancelStopsFurtherNotifications(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1, 1, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 1))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	cancelledCount := 0
	cancel := observer.Subscribe(func(PhaseSnapshot) { cancelledCount++ })
	controlCount := 0
	observer.Subscribe(func(PhaseSnapshot) { controlCount++ })

	observer.refresh(context.Background())
	if cancelledCount != 1 || controlCount != 1 {
		t.Fatalf("after first refresh: cancelledCount=%d controlCount=%d, want 1/1", cancelledCount, controlCount)
	}

	cancel()
	observer.refresh(context.Background())
	observer.refresh(context.Background())

	if cancelledCount != 1 {
		t.Errorf("cancelled subscriber invoked after cancel: count=%d, want 1 (unchanged)", cancelledCount)
	}
	if controlCount != 3 {
		t.Errorf("control subscriber count=%d, want 3", controlCount)
	}
}

// TestPhaseObserver_EpochFetchErrorKeepsPreviousSnapshotWithLastError documents the chosen
// fetch-error behavior: republish the previous snapshot unchanged except LastError.
func TestPhaseObserver_EpochFetchErrorKeepsPreviousSnapshotWithLastError(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(42, 4, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 7))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	observer.refresh(context.Background())
	good := observer.Snapshot()
	if good.LastError != "" {
		t.Fatalf("precondition: LastError = %q, want empty", good.LastError)
	}

	stub.setEpoch(http.StatusInternalServerError, "")
	observer.refresh(context.Background())
	after := observer.Snapshot()

	if after.LastError == "" {
		t.Error("LastError = empty, want set after epoch fetch failure")
	}
	after.LastError = ""
	if !reflect.DeepEqual(after, good) {
		t.Errorf("snapshot after fetch error = %+v, want unchanged from %+v (aside from LastError)", after, good)
	}
}

// TestPhaseObserver_ParticipantsFetchErrorKeepsPreviousWeightsWithLastError covers the
// participants-only failure: epoch-derived fields still advance, weights/URLs stay stale.
// A participants fetch failing is routine, and the carry-forward path restates every participant-derived
// field by hand. A field added to the fresh path and missed here is not published as unknown, it is
// published as zero -- and a zero weight map means "everything weighs nothing", which stops routing with
// no error anywhere. The second half of this test fails when a new field belongs to neither list, so the
// omission cannot be silent.
func TestPhaseObserver_CarriesEveryParticipantDerivedFieldForward(t *testing.T) {
	carriedAcrossFailure := []string{"LastHealthyAt"}
	participantDerived := []string{
		"CurrentWeights", "FullWeights", "CurrentWeightsByModel", "FullWeightsByModel",
		"Preserved", "PreservedByModel", "InferenceURLs",
	}
	phaseDerived := []string{
		"BlockHeight", "EpochSwitchBlockHeight", "EpochIndex", "EpochPhase", "ConfirmationPoCPhase",
		"RequestsBlocked", "BlockReason", "MaxNonce", "LastUpdatedAt", "LastError",
	}

	previous := PhaseSnapshot{
		LastHealthyAt:         time.Unix(1700000000, 0),
		CurrentWeights:        map[string]float64{"participant-a": 1},
		FullWeights:           map[string]float64{"participant-a": 2},
		CurrentWeightsByModel: map[string]map[string]float64{"model-a": {"participant-a": 3}},
		FullWeightsByModel:    map[string]map[string]float64{"model-a": {"participant-a": 4}},
		Preserved:             []string{"participant-a"},
		PreservedByModel:      map[string][]string{"model-a": {"participant-a"}},
		InferenceURLs:         map[string]string{"participant-a": "http://host.invalid"},
	}
	observer := &PhaseObserver{}
	observer.publishWithPreviousParticipants(PhaseSnapshot{BlockHeight: 9}, previous, "fetch failed")
	carried := reflect.ValueOf(observer.Snapshot())
	source := reflect.ValueOf(previous)

	for _, name := range append(append([]string{}, participantDerived...), carriedAcrossFailure...) {
		if got, want := carried.FieldByName(name).Interface(), source.FieldByName(name).Interface(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s = %v, want the previous snapshot's %v", name, got, want)
		}
	}

	known := map[string]bool{}
	for _, name := range append(append(append([]string{}, participantDerived...), phaseDerived...), carriedAcrossFailure...) {
		known[name] = true
	}
	snapshotType := reflect.TypeFor[PhaseSnapshot]()
	for index := range snapshotType.NumField() {
		if name := snapshotType.Field(index).Name; !known[name] {
			t.Errorf("PhaseSnapshot.%s is in neither list: decide whether the failure path must carry it forward", name)
		}
	}
}

func TestPhaseObserver_ParticipantsFetchErrorKeepsPreviousWeightsWithLastError(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(42, 4, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 7))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	observer.refresh(context.Background())
	good := observer.Snapshot()
	if good.LastError != "" {
		t.Fatalf("precondition: LastError = %q, want empty", good.LastError)
	}

	stub.setEpoch(http.StatusOK, observerEpochJSON(43, 4, EpochPhaseInference))
	stub.setParticipants(http.StatusInternalServerError, "")
	observer.refresh(context.Background())
	after := observer.Snapshot()

	if after.LastError == "" {
		t.Error("LastError = empty, want set after participants fetch failure")
	}
	if after.BlockHeight != 43 {
		t.Errorf("BlockHeight = %d, want 43 (epoch fetch still succeeded)", after.BlockHeight)
	}
	if !reflect.DeepEqual(after.CurrentWeights, good.CurrentWeights) {
		t.Errorf("CurrentWeights = %v, want stale %v", after.CurrentWeights, good.CurrentWeights)
	}
	if !reflect.DeepEqual(after.InferenceURLs, good.InferenceURLs) {
		t.Errorf("InferenceURLs = %v, want stale %v", after.InferenceURLs, good.InferenceURLs)
	}
}

func TestPhaseObserver_DecodesMaxNonceFromDevshardEscrowParams(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(42, 4, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 7))
	stub.setMaxNonceValue(19_800, true, nil)

	observer := newPoCPhaseObserver(t, server, stub)
	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if snapshot.LastError != "" {
		t.Fatalf("LastError = %q, want empty", snapshot.LastError)
	}
	if snapshot.MaxNonce != 19_800 {
		t.Errorf("MaxNonce = %d, want 19800", snapshot.MaxNonce)
	}
}

// The cold-start contract: with no chain access configured, MaxNonce stays 0 through active
// polling and the params route is never dialed. Zero means "not fetched", never "no cap" -- routing
// falls back to a conservative ceiling rather than treating the budget as unlimited.
func TestPhaseObserver_MaxNonceZeroBeforeFirstSuccessfulFetch(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(42, 4, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 7))
	stub.setMaxNonceValue(19_800, true, nil)

	observer := newPoCPhaseObserver(t, server, nil)
	if observer.Snapshot().MaxNonce != 0 {
		t.Fatalf("MaxNonce before first refresh = %d, want 0", observer.Snapshot().MaxNonce)
	}

	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if snapshot.LastError != "" {
		t.Fatalf("LastError = %q, want empty (no chain access is not a failure)", snapshot.LastError)
	}
	if snapshot.MaxNonce != 0 {
		t.Errorf("MaxNonce = %d, want 0 (no chain access, so nothing was fetched)", snapshot.MaxNonce)
	}
	if hits := stub.maxNonceHitCount(); hits != 0 {
		t.Errorf("params reads = %d, want 0 when no chain access is configured", hits)
	}
}

// TestPhaseObserver_MaxNonceFetchErrorKeepsPriorValueAndRestOfSnapshot covers fail-open: a broken
// params endpoint keeps the last known-good MaxNonce and every other field; only LastError changes.
func TestPhaseObserver_MaxNonceFetchErrorKeepsPriorValueAndRestOfSnapshot(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(42, 4, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 7))
	stub.setMaxNonceValue(19_800, true, nil)

	observer := newPoCPhaseObserver(t, server, stub)
	observer.refresh(context.Background())
	good := observer.Snapshot()
	if good.LastError != "" {
		t.Fatalf("precondition: LastError = %q, want empty", good.LastError)
	}
	if good.MaxNonce != 19_800 {
		t.Fatalf("precondition: MaxNonce = %d, want 19800", good.MaxNonce)
	}

	stub.setMaxNonceValue(0, false, errors.New("chain unreachable"))
	observer.refresh(context.Background())
	after := observer.Snapshot()

	if after.LastError == "" {
		t.Error("LastError = empty, want set after max_nonce fetch failure")
	}
	if after.MaxNonce != 19_800 {
		t.Errorf("MaxNonce = %d, want prior value 19800 preserved (fail-open)", after.MaxNonce)
	}
	after.LastError = ""
	if !reflect.DeepEqual(after, good) {
		t.Errorf("snapshot after max_nonce fetch error = %+v, want unchanged from %+v (aside from LastError)", after, good)
	}
}

// TestPhaseObserver_ContextCancelAloneStopsAllGoroutines covers shutdown driven purely by the
// parent context: every goroutine Start spawned must exit without Stop ever being called.
// leakcheck.VerifyNone is registered first so it runs last, after server.Close().
func TestPhaseObserver_ContextCancelAloneStopsAllGoroutines(t *testing.T) {
	defer leakcheck.VerifyNone(t)

	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1, 1, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 1))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Millisecond,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	snapshots := make(chan PhaseSnapshot, 16)
	defer observer.Subscribe(func(s PhaseSnapshot) { snapshots <- s })()

	ctx, cancel := context.WithCancel(context.Background())
	observer.Start(ctx)
	waitForSnapshot(t, snapshots, 2*time.Second, func(PhaseSnapshot) bool { return true })

	cancel()
	select {
	case <-observer.doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("goroutines did not exit within 2s of context cancel")
	}
}

// TestPhaseObserver_StopTerminatesLoopAndVersionsLoop covers clean shutdown of both goroutines
// Start spawns; leakcheck.VerifyNone is registered first so it runs last, after server.Close().
func TestPhaseObserver_StopTerminatesLoopAndVersionsLoop(t *testing.T) {
	defer leakcheck.VerifyNone(t)

	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1, 1, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 1))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Millisecond,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	snapshots := make(chan PhaseSnapshot, 16)
	defer observer.Subscribe(func(s PhaseSnapshot) { snapshots <- s })()

	observer.Start(context.Background())
	waitForSnapshot(t, snapshots, 2*time.Second, func(PhaseSnapshot) bool { return true })

	observer.Stop()
}

// TestPhaseObserver_StopIsIdempotentAndConcurrentSafe covers repeated and racing Stop calls: all
// must return without panicking and leave no goroutines behind.
func TestPhaseObserver_StopIsIdempotentAndConcurrentSafe(t *testing.T) {
	defer leakcheck.VerifyNone(t)

	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1, 1, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 1))

	clock := newFakeClock(time.Unix(0, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Millisecond,
		HTTPClient:       server.Client(),
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}

	snapshots := make(chan PhaseSnapshot, 16)
	defer observer.Subscribe(func(s PhaseSnapshot) { snapshots <- s })()

	observer.Start(context.Background())
	waitForSnapshot(t, snapshots, 2*time.Second, func(PhaseSnapshot) bool { return true })

	var stops sync.WaitGroup
	for range 4 {
		stops.Go(func() {
			observer.Stop()
		})
	}
	stops.Wait()
	observer.Stop()
}

// TestPhaseObserver_StopBeforeStartIsNoOp covers Stop on a never-started observer: it must return
// immediately instead of blocking on the done latch.
func TestPhaseObserver_StopBeforeStartIsNoOp(t *testing.T) {
	observer, err := NewPhaseObserver(ObserverConfig{PublicAPIBaseURL: "http://chain.example.invalid"})
	if err != nil {
		t.Fatalf("NewPhaseObserver(): %v", err)
	}
	observer.Stop()
}

// TestPhaseObserver_PoCCurrentPreservedSnapshotReplacesTimeslotRule covers preservation mode
// selection when the chain-REST snapshot is found at the expected anchor: its membership fully
// replaces the participants' timeslot-allocation proxy in both directions.
func TestPhaseObserver_PoCCurrentPreservedSnapshotReplacesTimeslotRule(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochPoCJSON(EpochPhasePoCGenerate, 950))
	stub.setParticipants(http.StatusOK, observerTwoMinerParticipantsJSON(server.URL))
	stub.setPreservedNodes(observerPreservedSnapshot(950), true, nil)

	observer := newPoCPhaseObserver(t, server, stub)
	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if snapshot.LastError != "" {
		t.Fatalf("LastError = %q, want empty", snapshot.LastError)
	}
	if want := []string{"gonka1abc"}; !reflect.DeepEqual(snapshot.Preserved, want) {
		t.Errorf("Preserved = %v, want %v (snapshot membership, not timeslots)", snapshot.Preserved, want)
	}
	if want := map[string]float64{"gonka1abc": 100, "gonka1bcd": 0}; !reflect.DeepEqual(snapshot.CurrentWeights, want) {
		t.Errorf("CurrentWeights = %v, want %v", snapshot.CurrentWeights, want)
	}
	if want := map[string]float64{"gonka1abc": 100, "gonka1bcd": 40}; !reflect.DeepEqual(snapshot.FullWeights, want) {
		t.Errorf("FullWeights = %v, want %v", snapshot.FullWeights, want)
	}
	if want := map[string][]string{"model-a": {"gonka1abc"}}; !reflect.DeepEqual(snapshot.PreservedByModel, want) {
		t.Errorf("PreservedByModel = %v, want %v", snapshot.PreservedByModel, want)
	}
}

// TestPhaseObserver_ConfirmationGraceMissingSnapshotPreservesAll covers the confirmation-PoC
// grace period with the snapshot not published yet: every participant stays preserved.
func TestPhaseObserver_ConfirmationGraceMissingSnapshotPreservesAll(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochConfirmationJSON(ConfirmationPoCGracePeriod, 555))
	stub.setParticipants(http.StatusOK, observerTwoMinerParticipantsJSON(server.URL))
	stub.setPreservedNodes(nil, false, nil)

	observer := newPoCPhaseObserver(t, server, stub)
	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if !snapshot.RequestsBlocked || snapshot.BlockReason != BlockReasonConfirmationPoC {
		t.Fatalf("RequestsBlocked/BlockReason = %v/%q, want true/%q", snapshot.RequestsBlocked, snapshot.BlockReason, BlockReasonConfirmationPoC)
	}
	if snapshot.LastError != "" {
		t.Fatalf("LastError = %q, want empty", snapshot.LastError)
	}
	if want := []string{"gonka1abc", "gonka1bcd"}; !reflect.DeepEqual(snapshot.Preserved, want) {
		t.Errorf("Preserved = %v, want all participants %v", snapshot.Preserved, want)
	}
	if want := map[string]float64{"gonka1abc": 100, "gonka1bcd": 40}; !reflect.DeepEqual(snapshot.CurrentWeights, want) {
		t.Errorf("CurrentWeights = %v, want all-node %v", snapshot.CurrentWeights, want)
	}
}

// TestPhaseObserver_PoCPreservedSnapshotMissesFallBackToLegacyRule covers every miss that must keep
// the participants-endpoint timeslot rule: the chain has no snapshot, the read fails, the anchor is
// stale, and no chain access is configured at all.
func TestPhaseObserver_PoCPreservedSnapshotMissesFallBackToLegacyRule(t *testing.T) {
	cases := []struct {
		name          string
		withChain     bool
		preserved     *PreservedNodes
		preservedHeld bool
		preservedErr  error
		wantErrSubstr string
		wantZeroHits  bool
	}{
		{name: "chain has no snapshot", withChain: true},
		{name: "read failed", withChain: true, preservedErr: errors.New("chain unreachable"), wantErrSubstr: "chain unreachable"},
		{name: "anchor mismatch", withChain: true, preserved: observerPreservedSnapshot(900), preservedHeld: true},
		{name: "no chain access", preserved: observerPreservedSnapshot(950), preservedHeld: true, wantZeroHits: true},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			stub := newPhaseObserverStub()
			server := httptest.NewServer(stub.handler())
			defer server.Close()
			stub.setEpoch(http.StatusOK, observerEpochPoCJSON(EpochPhasePoCGenerate, 950))
			stub.setParticipants(http.StatusOK, observerTwoMinerParticipantsJSON(server.URL))
			stub.setPreservedNodes(testCase.preserved, testCase.preservedHeld, testCase.preservedErr)

			var chainReader Reader
			if testCase.withChain {
				chainReader = stub
			}
			observer := newPoCPhaseObserver(t, server, chainReader)
			observer.refresh(context.Background())
			snapshot := observer.Snapshot()

			if want := []string{"gonka1bcd"}; !reflect.DeepEqual(snapshot.Preserved, want) {
				t.Errorf("Preserved = %v, want legacy %v", snapshot.Preserved, want)
			}
			if want := map[string]float64{"gonka1abc": 0, "gonka1bcd": 40}; !reflect.DeepEqual(snapshot.CurrentWeights, want) {
				t.Errorf("CurrentWeights = %v, want legacy %v", snapshot.CurrentWeights, want)
			}
			if testCase.wantErrSubstr == "" && snapshot.LastError != "" {
				t.Errorf("LastError = %q, want empty", snapshot.LastError)
			}
			if testCase.wantErrSubstr != "" && !strings.Contains(snapshot.LastError, testCase.wantErrSubstr) {
				t.Errorf("LastError = %q, want substring %q", snapshot.LastError, testCase.wantErrSubstr)
			}
			if testCase.wantZeroHits && stub.preservedHitCount() != 0 {
				t.Errorf("preserved reads = %d, want 0 when no chain access is configured", stub.preservedHitCount())
			}
		})
	}
}

// observerVersionsJSON builds a /v1/versions body advertising node1/node2 capability flags.
func observerVersionsJSON(node1Capable, node2Capable bool) string {
	return fmt.Sprintf(`{"mlnodes": [
		{"node_id": "node1", "poc_validation_inference": %t},
		{"node_id": "node2", "poc_validation_inference": %t}
	]}`, node1Capable, node2Capable)
}

// TestPhaseObserver_ValidationMergeAddsCapableExcludedMiner covers the published validation-phase
// view: with a cold capability cache the merge stays conservative, and after a versions poll the
// legacy-excluded miner rejoins with its capable node's weight.
func TestPhaseObserver_ValidationMergeAddsCapableExcludedMiner(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochPoCJSON(EpochPhasePoCValidate, 950))
	stub.setParticipants(http.StatusOK, observerTwoMinerParticipantsJSON(server.URL))
	stub.setVersions(http.StatusOK, observerVersionsJSON(true, true))

	observer := newPoCPhaseObserver(t, server, stub)

	observer.refresh(context.Background())
	cold := observer.Snapshot()
	if want := []string{"gonka1bcd"}; !reflect.DeepEqual(cold.Preserved, want) {
		t.Fatalf("cold Preserved = %v, want %v (capability unknown => fail closed)", cold.Preserved, want)
	}
	if cold.CurrentWeights["gonka1abc"] != 0 {
		t.Fatalf("cold CurrentWeights[gonka1abc] = %v, want 0", cold.CurrentWeights["gonka1abc"])
	}

	observer.versions.Poll(context.Background())
	observer.refresh(context.Background())
	merged := observer.Snapshot()

	if want := []string{"gonka1abc", "gonka1bcd"}; !reflect.DeepEqual(merged.Preserved, want) {
		t.Errorf("merged Preserved = %v, want %v", merged.Preserved, want)
	}
	if want := map[string]float64{"gonka1abc": 100, "gonka1bcd": 40}; !reflect.DeepEqual(merged.CurrentWeights, want) {
		t.Errorf("merged CurrentWeights = %v, want %v", merged.CurrentWeights, want)
	}
	if want := map[string]map[string]float64{"model-a": {"gonka1abc": 100, "gonka1bcd": 40}}; !reflect.DeepEqual(merged.CurrentWeightsByModel, want) {
		t.Errorf("merged CurrentWeightsByModel = %v, want %v", merged.CurrentWeightsByModel, want)
	}
	if want := map[string][]string{"model-a": {"gonka1abc", "gonka1bcd"}}; !reflect.DeepEqual(merged.PreservedByModel, want) {
		t.Errorf("merged PreservedByModel = %v, want %v", merged.PreservedByModel, want)
	}
	if want := map[string]float64{"gonka1abc": 100, "gonka1bcd": 40}; !reflect.DeepEqual(merged.FullWeights, want) {
		t.Errorf("merged FullWeights = %v, want %v", merged.FullWeights, want)
	}
}

// TestPhaseObserver_ValidationMergeExcludesNonCapableNode covers the fail-closed side: a miner
// whose only node is not validation-inference capable never enters the published preserved set.
func TestPhaseObserver_ValidationMergeExcludesNonCapableNode(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochPoCJSON(EpochPhasePoCValidate, 950))
	stub.setParticipants(http.StatusOK, observerTwoMinerParticipantsJSON(server.URL))
	stub.setVersions(http.StatusOK, observerVersionsJSON(false, true))

	observer := newPoCPhaseObserver(t, server, stub)
	observer.refresh(context.Background())
	observer.versions.Poll(context.Background())
	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if want := []string{"gonka1bcd"}; !reflect.DeepEqual(snapshot.Preserved, want) {
		t.Errorf("Preserved = %v, want %v (non-capable node1 excluded)", snapshot.Preserved, want)
	}
	if snapshot.CurrentWeights["gonka1abc"] != 0 {
		t.Errorf("CurrentWeights[gonka1abc] = %v, want 0", snapshot.CurrentWeights["gonka1abc"])
	}
	if want := map[string][]string{"model-a": {"gonka1bcd"}}; !reflect.DeepEqual(snapshot.PreservedByModel, want) {
		t.Errorf("PreservedByModel = %v, want %v", snapshot.PreservedByModel, want)
	}
}

// TestPhaseObserver_GenerationPhaseSkipsValidationMerge covers the merge's phase gate: outside a
// validation stage, known capability must not add excluded miners back.
func TestPhaseObserver_GenerationPhaseSkipsValidationMerge(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochPoCJSON(EpochPhasePoCGenerate, 950))
	stub.setParticipants(http.StatusOK, observerTwoMinerParticipantsJSON(server.URL))
	stub.setVersions(http.StatusOK, observerVersionsJSON(true, true))

	observer := newPoCPhaseObserver(t, server, stub)
	observer.refresh(context.Background())
	observer.versions.Poll(context.Background())
	observer.refresh(context.Background())
	snapshot := observer.Snapshot()

	if want := []string{"gonka1bcd"}; !reflect.DeepEqual(snapshot.Preserved, want) {
		t.Errorf("Preserved = %v, want %v (no merge during generation)", snapshot.Preserved, want)
	}
	if snapshot.CurrentWeights["gonka1abc"] != 0 {
		t.Errorf("CurrentWeights[gonka1abc] = %v, want 0", snapshot.CurrentWeights["gonka1abc"])
	}
}

// A second Start used to close an already-closed channel, panicking, and to orphan the first pair of
// pollers by overwriting their cancel. goleak catches the orphan; the panic would fail outright.
func TestPhaseObserver_StartTwiceIsANoOpAndRestartWorks(t *testing.T) {
	defer leakcheck.VerifyNone(t)
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1000, 7, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 42))
	observer := newPoCPhaseObserver(t, server, stub)

	ctx := context.Background()
	observer.Start(ctx)
	observer.Start(ctx)
	observer.Stop()

	observer.Start(ctx)
	observer.Stop()
}

// The narration's whole value is silence while a failure persists, so the detector is tested apart
// from the line it drives.
func TestSnapshotHealth_AdvanceSpeaksOnlyOnChange(t *testing.T) {
	t.Parallel()
	steps := []struct {
		name          string
		lastError     string
		wantDegraded  bool
		wantRecovered bool
	}{
		{name: "first healthy poll says nothing", lastError: ""},
		{name: "first failure degrades", lastError: "fetch epoch info: 503", wantDegraded: true},
		{name: "same failure again stays silent", lastError: "fetch epoch info: 503"},
		{name: "a different failure stays silent", lastError: "fetch participants: parse"},
		{name: "success recovers", lastError: "", wantRecovered: true},
		{name: "still healthy stays silent", lastError: ""},
	}
	var health snapshotHealth
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			change := health.advance(step.lastError)
			if change.degraded != step.wantDegraded {
				t.Errorf("degraded = %v, want %v", change.degraded, step.wantDegraded)
			}
			if change.recovered != step.wantRecovered {
				t.Errorf("recovered = %v, want %v", change.recovered, step.wantRecovered)
			}
		})
	}
}
