package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/e2e/testutil"
)

const (
	holdReserveTokens   = 100_000
	holdStandTokenPrice = 7
	managerTick         = 15 * time.Second
)

// holdStandOptions stalls every host and prices the floor so one held reservation takes the escrow below it.
func holdStandOptions() e2eEnvOptions {
	return e2eEnvOptions{
		hostEnvOverrides: testutil.HostsStalling(e2eHostCount, "600000"),
		gatewayEnvOverrides: map[string]string{
			"GATEWAY_MAX_TOKENS_CAP":                  strconv.Itoa(holdReserveTokens),
			"GATEWAY_WARM_NEW_ESCROWS":                "false",
			"DEVSHARD_ESCROW_ROTATION_ENABLED":        "true",
			"GATEWAY_ROTATION_HOLD_ENABLED":           "true",
			"DEVSHARD_ESCROW_ROTATION_PRE_POC_BLOCKS": "0",
			"DEVSHARD_ESCROW_ROTATION_MODELS_JSON": fmt.Sprintf(
				`[{"model_id":%q,"target_count":1,"amount":1000000,"private_key_env":"GATEWAY_REPLACEMENT_KEY"}]`,
				defaultStandModel),
		},
	}
}

// gatewayJournal returns the gateway's log entries carrying one message for one escrow.
func gatewayJournal(ctx context.Context, t *testing.T, env *e2eEnv, message, escrowID string) []map[string]any {
	t.Helper()
	logs, err := env.gateway.Logs(ctx)
	require.NoError(t, err)
	defer logs.Close()
	body, err := io.ReadAll(logs)
	require.NoError(t, err)
	var entries []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		start := strings.IndexByte(line, '{')
		if start < 0 {
			continue
		}
		var entry map[string]any
		if json.Unmarshal([]byte(line[start:]), &entry) != nil {
			continue
		}
		if entry["msg"] == message && entry["escrow"] == escrowID {
			entries = append(entries, entry)
		}
	}
	return entries
}

// sendReservingCompletion asks for the whole cap, so the escrow reserves a full floor for it.
func sendReservingCompletion(client *http.Client, clientURL string) {
	body := testutil.ChatCompletionBody("reserve the whole cap", false)
	body["max_tokens"] = holdReserveTokens
	_, _ = testutil.PostJSONRawE(client, clientURL+"/v1/chat/completions", body, testutil.AdminAPIKey)
}

// awaitOnHold drives the escrow below its floor with a stalled reservation and polls until the tick puts it on hold.
func awaitOnHold(ctx context.Context, t *testing.T, env *e2eEnv, client *http.Client) {
	t.Helper()
	go sendReservingCompletion(client, env.clientURL)

	floor := uint64(holdReserveTokens * holdStandTokenPrice)
	deadline := time.Now().Add(60 * time.Second)
	for testutil.EscrowBalance(t, client, env.clientURL, defaultEscrowID, testutil.AdminAPIKey) >= floor {
		if time.Now().After(deadline) {
			t.Fatalf("the stalled reservation never took escrow %s below its floor of %d", defaultEscrowID, floor)
		}
		time.Sleep(time.Second)
	}

	declined := testutil.SendCompletionRaw(t, client, env.clientURL, "below the floor", testutil.AdminAPIKey)
	if declined.StatusCode == http.StatusOK {
		t.Fatalf("an escrow below its floor still served: %s", declined.Body)
	}

	deadline = time.Now().Add(2 * managerTick)
	for !testutil.ListedDevshard(t, client, env.clientURL, defaultEscrowID, testutil.AdminAPIKey).OnHold {
		if time.Now().After(deadline) {
			t.Fatalf("escrow %s was never put on hold: %+v", defaultEscrowID,
				testutil.ListedDevshard(t, client, env.clientURL, defaultEscrowID, testutil.AdminAPIKey))
		}
		time.Sleep(500 * time.Millisecond)
	}
	if entries := gatewayJournal(ctx, t, env, "escrow put on hold", defaultEscrowID); len(entries) != 1 {
		t.Errorf("the journal carries %d \"escrow put on hold\" lines for escrow %s, want 1", len(entries), defaultEscrowID)
	}
	onHoldSeries := fmt.Sprintf(`devshard_gateway_escrow_on_hold{devshard_id=%q,model=%q} 1`, defaultEscrowID, defaultStandModel)
	if !strings.Contains(gatewayScrape(t, client, env.clientURL), onHoldSeries) {
		t.Errorf("the scrape does not carry %s", onHoldSeries)
	}
}

// Test flow:
//  1. Start the gateway environment with every host stalling, rotation on for the stand model with a replacement key the gateway does not hold, and a max-tokens cap large enough that one held reservation takes the escrow below its floor.
//  2. Send one completion asking for the whole cap and wait until the escrow's balance is below the floor.
//  3. Send a plain completion and assert the escrow does not serve it.
//  4. Poll the admin listing until the escrow is on hold, and assert the journal line and the on-hold gauge.
//  5. Deactivate the escrow, then lower the cap and the resume headroom so its balance would cover a resume.
//  6. Across two manager ticks, assert its row stays inactive, off hold and not parked, and nothing resumed or parked it.
func TestE2E_GatewayKeepsADeactivatedEscrowOnHoldOutOfService(t *testing.T) {
	env, client := startGatewayEnv(t, holdStandOptions())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	awaitOnHold(ctx, t, env, client)

	admin := env.clientURL + "/v1/admin/devshards/" + defaultEscrowID
	if deactivated := gatewayPost(t, client, admin+"/deactivate"); deactivated.StatusCode != http.StatusOK {
		t.Fatalf("deactivate = %d %s", deactivated.StatusCode, deactivated.Body)
	}
	putSettings(t, client, env.clientURL, map[string]any{"max_tokens_cap": e2eMaxTokensCap, "rotation_hold_resume_answers": 1})

	watchUntil := time.Now().Add(2*managerTick + 5*time.Second)
	for time.Now().Before(watchUntil) {
		row := testutil.ListedDevshard(t, client, env.clientURL, defaultEscrowID, testutil.AdminAPIKey)
		if row.Active || row.OnHold || row.SettlementPending {
			t.Fatalf("the deactivated escrow on hold came back: %+v", row)
		}
		time.Sleep(2 * time.Second)
	}
	for _, message := range []string{"escrow resumed from hold", "escrow hold ended, parked for settlement"} {
		if entries := gatewayJournal(ctx, t, env, message, defaultEscrowID); len(entries) > 0 {
			t.Errorf("the journal carries %q for the deactivated escrow: %v", message, entries)
		}
	}
}
