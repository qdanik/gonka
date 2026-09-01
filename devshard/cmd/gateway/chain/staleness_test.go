package chain

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOnlyACompletePollRefreshesTheHealthyClock(t *testing.T) {
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
		t.Fatalf("NewPhaseObserver: %v", err)
	}

	observer.refresh(context.Background())
	healthy := observer.Snapshot().LastHealthyAt
	if !healthy.Equal(time.Unix(100, 0)) {
		t.Fatalf("LastHealthyAt after a complete poll = %v, want the poll's own instant", healthy)
	}

	stub.setParticipants(http.StatusInternalServerError, "")
	clock.Advance(time.Minute)
	observer.refresh(context.Background())

	published := observer.Snapshot()
	if !published.LastUpdatedAt.Equal(time.Unix(160, 0)) {
		t.Errorf("LastUpdatedAt = %v, want the failed poll's instant", published.LastUpdatedAt)
	}
	if !published.LastHealthyAt.Equal(healthy) {
		t.Errorf("LastHealthyAt = %v, want the last complete poll's %v: a poll whose participants read failed does not refresh the age", published.LastHealthyAt, healthy)
	}
	if published.LastError == "" {
		t.Error("a failed participants read published no error")
	}
}

func TestANonceCeilingFailureStillRefreshesTheHealthyClock(t *testing.T) {
	stub := newPhaseObserverStub()
	server := httptest.NewServer(stub.handler())
	defer server.Close()
	stub.setEpoch(http.StatusOK, observerEpochJSON(1000, 7, EpochPhaseInference))
	stub.setParticipants(http.StatusOK, observerParticipantsJSON("gonka1abc", server.URL, 42))
	stub.setMaxNonceValue(0, false, errors.New("params unavailable"))

	clock := newFakeClock(time.Unix(100, 0))
	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: server.URL,
		PollInterval:     time.Hour,
		HTTPClient:       server.Client(),
		Chain:            stub,
		Now:              clock.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver: %v", err)
	}

	observer.refresh(context.Background())

	published := observer.Snapshot()
	if !published.LastHealthyAt.Equal(time.Unix(100, 0)) {
		t.Errorf("LastHealthyAt = %v, want the poll's instant: the nonce ceiling falls back within the poll and does not hold it back", published.LastHealthyAt)
	}
	if published.LastError == "" {
		t.Error("the tolerated failure was not reported")
	}
}

func TestAHungReadDoesNotWedgeThePollLoop(t *testing.T) {
	reached := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(reached)
		<-r.Context().Done()
	}))
	defer upstream.Close()

	observer, err := NewPhaseObserver(ObserverConfig{
		PublicAPIBaseURL: upstream.URL,
		HTTPClient:       upstream.Client(),
		PollInterval:     50 * time.Millisecond,
		Now:              time.Now,
	})
	if err != nil {
		t.Fatalf("NewPhaseObserver: %v", err)
	}

	observer.Start(context.Background())
	defer observer.Stop()

	<-reached
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if observer.Snapshot().LastError != "" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the poll loop never reported a deadline: one hung read wedges it and the shard refuses everything")
}
