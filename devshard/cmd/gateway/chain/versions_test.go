package chain

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// fakeClock is a mutex-guarded injectable clock so TTL/staleness tests never
// depend on the real wall clock.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// newVersionsStub starts an httptest server serving a fixed /v1/versions
// response; it is closed automatically on test cleanup.
func newVersionsStub(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if body != "" {
			w.Write([]byte(body))
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestVersionsCache_PollSucceeds_NodeCapableWithinTTL(t *testing.T) {
	server := newVersionsStub(t, http.StatusOK, `{"mlnodes":[{"node_id":"node-1","poc_validation_inference":true}]}`)
	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(server.Client(), time.Minute, clock.Now)
	cache.SetCandidates(map[string]string{"miner-1": server.URL})

	cache.Poll(context.Background())

	if !cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("capable node within ttl should be validation-capable")
	}
}

func TestVersionsCache_UnknownMiner_FailsClosed(t *testing.T) {
	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(http.DefaultClient, time.Minute, clock.Now)

	if cache.IsNodeValidationCapable("never-a-candidate", "node-1") {
		t.Fatal("miner never polled should fail closed")
	}
}

func TestVersionsCache_ServerError_FailsClosed(t *testing.T) {
	server := newVersionsStub(t, http.StatusInternalServerError, "")
	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(server.Client(), time.Minute, clock.Now)
	cache.SetCandidates(map[string]string{"miner-1": server.URL})

	cache.Poll(context.Background())

	if cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("500 response should fail closed")
	}
}

func TestVersionsCache_NodeExplicitlyIncapable_ReturnsFalse(t *testing.T) {
	server := newVersionsStub(t, http.StatusOK, `{"mlnodes":[{"node_id":"node-1","poc_validation_inference":false}]}`)
	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(server.Client(), time.Minute, clock.Now)
	cache.SetCandidates(map[string]string{"miner-1": server.URL})

	cache.Poll(context.Background())

	if cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("node explicitly flagged poc_validation_inference=false should not be capable")
	}
}

func TestVersionsCache_EntryOlderThanTTL_FailsClosed(t *testing.T) {
	server := newVersionsStub(t, http.StatusOK, `{"mlnodes":[{"node_id":"node-1","poc_validation_inference":true}]}`)
	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(server.Client(), time.Minute, clock.Now)
	cache.SetCandidates(map[string]string{"miner-1": server.URL})
	cache.Poll(context.Background())
	if !cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("precondition: node should be capable before the ttl elapses")
	}

	clock.Advance(2 * time.Minute)

	if cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("entry older than ttl should fail closed")
	}
}

func TestVersionsCache_SetCandidates_DropsRemovedMiner(t *testing.T) {
	server := newVersionsStub(t, http.StatusOK, `{"mlnodes":[{"node_id":"node-1","poc_validation_inference":true}]}`)
	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(server.Client(), time.Minute, clock.Now)
	cache.SetCandidates(map[string]string{"miner-1": server.URL})
	cache.Poll(context.Background())
	if !cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("precondition: node should be capable before its miner is removed")
	}

	cache.SetCandidates(map[string]string{"miner-2": server.URL})

	if cache.IsNodeValidationCapable("miner-1", "node-1") {
		t.Fatal("miner dropped from candidates must fail closed even with a cached entry")
	}
}

func TestVersionsCache_Run_ExitsOnContextCancel(t *testing.T) {
	// Registered first so it runs last: after the server (and its client
	// connections) are closed below, so the httptest listener goroutine
	// isn't mistaken for a leak.
	defer goleak.VerifyNone(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"mlnodes":[{"node_id":"node-1","poc_validation_inference":true}]}`))
	}))
	defer server.Close()

	clock := newFakeClock(time.Unix(0, 0))
	cache := NewVersionsCache(server.Client(), time.Minute, clock.Now)
	cache.SetCandidates(map[string]string{"miner-1": server.URL})

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		cache.Run(ctx, time.Millisecond)
	}()

	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit within 2s of context cancellation")
	}
}
