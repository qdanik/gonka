package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestRunServesMetricsAndShutsDownGracefully(t *testing.T) {
	port := freePort(t)
	t.Setenv("GATEWAY_PORT", fmt.Sprintf("%d", port))
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- run(ctx) }()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(baseURL + "/metrics")
		if err == nil {
			raw, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				body = string(raw)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not serve /metrics within 5s (last error: %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// go_goroutines comes from the process collector, so it is present on the very first scrape,
	// unlike the request counter which only appears once a route has been served.
	if !strings.Contains(body, "go_goroutines") {
		t.Fatalf("/metrics exposition is missing the process collector:\n%s", body)
	}

	cancel()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("run() after cancel: %v, want nil (graceful shutdown)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return within 5s of cancellation")
	}
}

func TestRunFailsFastOnInvalidEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "not-a-port")
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "GATEWAY_PORT") {
		t.Fatalf("run() with bad env = %v, want error naming GATEWAY_PORT", err)
	}
}

func TestRunFailsFastOnInvalidMergedConfig(t *testing.T) {
	t.Setenv("GATEWAY_MAX_TOKENS_CAP", "1") // below default_max_tokens 3072
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "max_tokens_cap") {
		t.Fatalf("run() with invalid merged config = %v, want max_tokens_cap error", err)
	}
}
