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

// A host that starts an answer and then emits an SSE error has committed its nonce. The client must be
// told plainly, and the nonce must still reach the books rather than vanish with the broken stream.
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

// The client hangs up while the answer is in flight. The nonce is already committed on chain, so the
// books must still say what became of it: a caller going away cannot take the money off the record.
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
