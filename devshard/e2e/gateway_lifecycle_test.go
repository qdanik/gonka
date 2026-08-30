package e2e

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"devshard/e2e/testutil"
)

func gatewayPost(t *testing.T, client *http.Client, url string) testutil.RawResponse {
	t.Helper()
	return testutil.PostRaw(t, client, url, "application/json", nil, testutil.AdminAPIKey)
}

// Deactivating takes an escrow out of routing and activating puts it back. It is how an operator drains
// one shard without stopping the gateway, so the round trip has to leave it serving again.
func TestE2E_GatewayDeactivatesAnEscrowAndBringsItBack(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "before draining", testutil.AdminAPIKey))

	admin := env.clientURL + "/v1/admin/devshards/" + defaultEscrowID
	if deactivated := gatewayPost(t, client, admin+"/deactivate"); deactivated.StatusCode != http.StatusOK {
		t.Fatalf("deactivate = %d %s", deactivated.StatusCode, deactivated.Body)
	}
	if drained := testutil.SendCompletionRaw(t, client, env.clientURL, "while drained", testutil.AdminAPIKey); drained.StatusCode == http.StatusOK {
		t.Errorf("a deactivated escrow still served a request: %s", drained.Body)
	}

	if activated := gatewayPost(t, client, admin+"/activate"); activated.StatusCode != http.StatusOK {
		t.Fatalf("activate = %d %s", activated.StatusCode, activated.Body)
	}
	// Activation re-opens the session rather than flipping a flag, so the promise is that it comes back,
	// not that the very next request finds it.
	awaitServed(t, client, env.clientURL, 30*time.Second)
}

func awaitServed(t *testing.T, client *http.Client, clientURL string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	var last testutil.RawResponse
	for time.Now().Before(deadline) {
		last = testutil.SendCompletionRaw(t, client, clientURL, "after draining", testutil.AdminAPIKey)
		if last.StatusCode == http.StatusOK {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("the escrow never came back into service within %s: last answer %d %s", within, last.StatusCode, last.Body)
}

// A settled escrow is gone, not parked: settlement drops the row that named the only key able to settle
// it, so activating it afterwards finds nothing. Serving it again would spend nonces the settlement missed.
func TestE2E_GatewayCannotActivateAnEscrowItHasSettled(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	admin := env.clientURL + "/v1/admin/devshards/" + defaultEscrowID
	if settled := gatewayPost(t, client, admin+"/settle"); settled.StatusCode != http.StatusOK {
		t.Fatalf("settle = %d %s", settled.StatusCode, settled.Body)
	}
	reactivated := gatewayPost(t, client, admin+"/activate")
	if reactivated.StatusCode != http.StatusNotFound {
		t.Errorf("activating a settled escrow = %d %s, want 404", reactivated.StatusCode, reactivated.Body)
	}
}

// The lifecycle routes act on an escrow that exists, and say so plainly when it does not: acting on an
// unknown id would be worse than refusing.
func TestE2E_GatewayRefusesLifecycleOnAnUnknownEscrow(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	for _, action := range []string{"/activate", "/deactivate", "/settle"} {
		response := gatewayPost(t, client, env.clientURL+"/v1/admin/devshards/999999"+action)
		if response.StatusCode != http.StatusNotFound {
			t.Errorf("POST %s on an unknown escrow = %d %s, want 404", action, response.StatusCode, response.Body)
		}
	}
}

// The escrow inventory an operator reads before touching anything: what the gateway holds, and who is
// in the group it would pay.
func TestE2E_GatewayListsItsEscrowsAndTheirParticipants(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	for _, path := range []string{"/v1/admin/devshards", "/v1/admin/suspicious-hosts"} {
		if status, body := gatewayGet(t, client, env.clientURL+path, testutil.AdminAPIKey); status != http.StatusOK {
			t.Errorf("GET %s = %d %s, want 200", path, status, body)
		}
	}

	status, body := gatewayGet(t, client, env.clientURL+"/v1/admin/devshards/"+defaultEscrowID+"/participants", testutil.AdminAPIKey)
	if status != http.StatusOK {
		t.Fatalf("participants = %d %s", status, body)
	}
	var listed map[string]any
	if err := json.Unmarshal([]byte(body), &listed); err != nil {
		t.Fatalf("participants are not JSON: %v (%s)", err, body)
	}
	slots, _ := listed["slots"].([]any)
	if len(slots) != 3 {
		t.Errorf("the escrow lists %d slots, want the three hosts the stack runs: %s", len(slots), body)
	}
}

// The debug surface an operator reaches for when routing looks wrong. It is admin-gated and always on,
// so a gateway that is refusing traffic can still be asked why.
func TestE2E_GatewayAnswersOnItsDebugRoutes(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	for _, path := range []string{"/v1/debug/rotation", "/v1/debug/memstats"} {
		if status, body := gatewayGet(t, client, env.clientURL+path, testutil.AdminAPIKey); status != http.StatusOK {
			t.Errorf("GET %s = %d %s, want 200", path, status, body)
		}
		if status, _ := gatewayGet(t, client, env.clientURL+path, ""); status == http.StatusOK {
			t.Errorf("GET %s answered without the admin key", path)
		}
	}
}
