package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"devshard/e2e/testutil"
	"devshard/internal/e2econfig"
)

// Test flow:
//  1. Start the three-host environment with every host failing mid-stream with an SSE error.
//  2. Send one completion.
//  3. Assert the client was not told the broken stream succeeded.
//  4. Assert the nonces it spent reached a disposition rather than vanishing.
func TestE2E_GatewaySurfacesAHostErrorMidStream(t *testing.T) {
	failing := map[string]string{e2econfig.StubInferenceSSEErrorEnv: "the model fell over"}
	env, client := startGatewayEnv(t, e2eEnvOptions{
		hostEnvOverrides: map[int]map[string]string{0: failing, 1: failing, 2: failing},
	})

	broken := testutil.SendCompletionRaw(t, client, env.clientURL, "mid-stream failure", testutil.AdminAPIKey)
	if broken.StatusCode == http.StatusOK {
		t.Errorf("a stream that failed mid-answer was reported as success: %s", broken.Body)
	}

	ledger := gatewayLedger(t, client, env.statsURL)
	var recorded uint64
	for _, disposition := range []string{"finished_used", "finished_unused", "finished_usage_unknown",
		"unfinished_refused", "unfinished_execution", "ghost"} {
		recorded += testutil.AccountingDispositionCount(ledger, disposition)
	}
	if recorded == 0 {
		t.Error("a failed stream spent nonces that reached no disposition at all")
	}
}

// Test flow:
//  1. Start the default three-host gateway environment.
//  2. Send one completion and hang up 40ms in, while the answer is still in flight.
//  3. Send a second completion and assert it is served normally.
//  4. Assert the abandoned request raised no findings.
func TestE2E_GatewayAccountsANonceAfterTheClientHangsUp(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	ctx, hangUp := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer hangUp()
	body, err := json.Marshal(testutil.ChatCompletionBody("hung up", false))
	if err != nil {
		t.Fatalf("encoding the request: %v", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		env.clientURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+testutil.AdminAPIKey)
	if response, err := client.Do(request); err == nil {
		response.Body.Close()
	}

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "after the hang-up", testutil.AdminAPIKey))
	if codes := gatewayFindings(t, client, env.statsURL); len(codes) > 0 {
		t.Errorf("a client hanging up raised findings %v, want none", codes)
	}
}
