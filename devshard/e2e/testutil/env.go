package testutil

import (
	"os"
	"strings"
	"testing"
	"time"

	"devshard/internal/e2econfig"
)

const DefaultRequestTimeout = 2 * time.Minute

var HostPrivateKeys = []string{
	"0000000000000000000000000000000000000000000000000000000000000011",
	"0000000000000000000000000000000000000000000000000000000000000012",
	"0000000000000000000000000000000000000000000000000000000000000013",
}

const UserPrivateKey = "0000000000000000000000000000000000000000000000000000000000000021"

const AdminAPIKey = "devshard-e2e-admin-key"

// EchoingHosts makes every host answer with the request body it received.
func EchoingHosts(hostCount int) map[int]map[string]string {
	return hostsCarrying(hostCount, e2econfig.StubInferenceEchoRequestEnv, "1")
}

// HostsAnswering pins the body every host returns.
func HostsAnswering(hostCount int, answer string) map[int]map[string]string {
	return hostsCarrying(hostCount, e2econfig.StubInferenceResponseBodyEnv, answer)
}

func hostsCarrying(hostCount int, key, value string) map[int]map[string]string {
	hosts := make(map[int]map[string]string, hostCount)
	for index := range hostCount {
		hosts[index] = map[string]string{key: value}
	}
	return hosts
}

func EnvDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func DebugEnabled() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("DEVSHARD_E2E_DEBUG")))
	return value == "1" || value == "true" || value == "yes"
}

func DebugLogf(t *testing.T, format string, args ...any) {
	t.Helper()
	if DebugEnabled() {
		t.Logf(format, args...)
	}
}
