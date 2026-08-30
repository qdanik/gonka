package e2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"devshard/e2e/testutil"
)

func putSettings(t *testing.T, client *http.Client, clientURL string, overrides map[string]any) {
	t.Helper()
	applied := testutil.PostJSONRaw(t, client, clientURL+"/v1/admin/settings", overrides, testutil.AdminAPIKey)
	if applied.StatusCode != http.StatusOK {
		t.Fatalf("applying %v = %d %s", overrides, applied.StatusCode, applied.Body)
	}
}

// An operator changes a limit on a running gateway and it binds at once. A setting that needs a restart
// to take effect is a setting nobody can use during the incident it was meant for.
func TestE2E_GatewayAppliesANewLimitWithoutARestart(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	putSettings(t, client, env.clientURL, map[string]any{"max_concurrent_requests": 1, "admission_queue_wait_ms": 0})

	const attempts = 8
	statuses := make([]int, attempts)
	var pending sync.WaitGroup
	for attempt := range attempts {
		pending.Add(1)
		go func() {
			defer pending.Done()
			response, err := testutil.SendCompletionRawE(client, env.clientURL,
				fmt.Sprintf("after reconfigure %d", attempt), testutil.AdminAPIKey)
			if err != nil {
				statuses[attempt] = -1
				return
			}
			statuses[attempt] = response.StatusCode
		}()
	}
	pending.Wait()

	for _, status := range statuses {
		if status == http.StatusTooManyRequests {
			return
		}
	}
	t.Errorf("a cap set through /v1/admin/settings admitted all %d at once: statuses %v", attempts, statuses)
}

// Closing a model behind a key at runtime takes effect on the next request, and the key still opens it.
func TestE2E_GatewayLocksAModelBehindAKeyAtRuntime(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{
		gatewayEnvOverrides: map[string]string{"GATEWAY_API_KEYS": testutil.AdminAPIKey},
	})

	putSettings(t, client, env.clientURL, map[string]any{"model_access": map[string]string{"stub-model": "api_key"}})

	anonymous := testutil.SendCompletionRaw(t, client, env.clientURL, "no key", "")
	if anonymous.StatusCode != http.StatusUnauthorized {
		t.Errorf("an unauthenticated request = %d %s, want 401", anonymous.StatusCode, anonymous.Body)
	}
	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "with key", testutil.AdminAPIKey))
}

// A setting that would put a model behind a key nobody holds is refused, and refused as the operator's
// mistake rather than the gateway's: a 502 here would send them hunting a fault that is not there.
func TestE2E_GatewayRefusesASettingThatWouldStrandAModel(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	refused := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/admin/settings",
		map[string]any{"model_access": map[string]string{"stub-model": "api_key"}}, testutil.AdminAPIKey)
	if refused.StatusCode != http.StatusBadRequest {
		t.Errorf("stranding a model = %d %s, want 400", refused.StatusCode, refused.Body)
	}
	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "still served", testutil.AdminAPIKey))
}

// AccessFor treats every model missing from a non-empty map as admin-only, so naming one model closes
// all the others. Deliberate, and sharp enough that a typo in the map takes a served model off the air.
func TestE2E_GatewayClosesModelsLeftOutOfTheAccessMap(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	putSettings(t, client, env.clientURL, map[string]any{"model_access": map[string]string{"some-other-model": "open"}})

	anonymous := testutil.SendCompletionRaw(t, client, env.clientURL, "unlisted model", "")
	if anonymous.StatusCode != http.StatusUnauthorized {
		t.Errorf("a model left out of the access map answered %d %s, want 401", anonymous.StatusCode, anonymous.Body)
	}
}

// What the settings route reports back is what was applied; an operator reads it to confirm the change.
func TestE2E_GatewaySettingsReadBackWhatWasApplied(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	putSettings(t, client, env.clientURL, map[string]any{"max_concurrent_requests": 7})

	status, body := gatewayGet(t, client, env.clientURL+"/v1/admin/settings", testutil.AdminAPIKey)
	if status != http.StatusOK {
		t.Fatalf("GET settings = %d %s", status, body)
	}
	var settings map[string]any
	if err := json.Unmarshal([]byte(body), &settings); err != nil {
		t.Fatalf("settings are not JSON: %v (%s)", err, body)
	}
	if got := testutil.NumericField(t, settings, "max_concurrent_requests"); got != 7 {
		t.Errorf("max_concurrent_requests reads back as %d, want the 7 that was applied", got)
	}
}
