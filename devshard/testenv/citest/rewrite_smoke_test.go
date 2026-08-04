package citest

import (
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"devshard/testenv/citest/harness"
	"devshard/testenv/config"

	"github.com/stretchr/testify/require"
)

// TestRewriteSmoke drives cmd/gateway through the same stack devshardctl runs in: a mock chain, the
// versiond router, devshardd and mock-openai, across real process and network boundaries. The
// in-process suite composes the same packages but never leaves one address space, so nothing there
// exercises container networking, a real socket, or SQLite on a real filesystem.
func TestRewriteSmoke(t *testing.T) {
	if os.Getenv("TESTENV_GATEWAY_SMOKE") != "1" {
		t.Skip("set TESTENV_GATEWAY_SMOKE=1 to run the rewrite against the full stack (Docker)")
	}

	stack := harness.NewStack(t, "citest-rewrite-*")
	harness.RequireLinuxDevshardd(t, stack.TestenvDir)

	harness.WriteSingleVersiondConfig(t, stack.WorkDir)
	stack.RunGencompose(t)
	stack.UpBuild(t) // Up reuses whatever image exists, which would run the previous build of the gateway.

	cfg := stack.LoadConfig(t)
	eps := stack.Endpoints(t, cfg)
	client := harness.HTTPClient()
	poll := 3 * time.Minute

	t.Cleanup(func() {
		if t.Failed() {
			harness.DumpComposeLogs(t, stack, "gateway", "versiond-0", "mock-openai", "mock-chain")
		}
	})

	harness.WaitGETOK(t, client, eps.RewriteHTTP+"/v1/status", poll, "rewrite /v1/status")

	var status map[string]any
	require.NoError(t, harness.GetJSON(client, eps.RewriteHTTP+"/v1/status", &status))
	t.Logf("rewrite status: %v", status)

	chat := map[string]any{
		"model":      "test-model",
		"messages":   []map[string]string{{"role": "user", "content": "rewrite smoke"}},
		"max_tokens": 32,
	}
	var completion map[string]any
	require.NoError(t, harness.PostJSON(client, eps.RewriteHTTP+"/v1/chat/completions", chat, &completion))
	choices, isList := completion["choices"].([]any)
	require.True(t, isList, "completion response: %v", completion)
	require.NotEmpty(t, choices)

	// The strip runs on a real socket here, not on a buffer handed between two functions.
	for _, internal := range []string{"logprobs", "token_ids", "prompt_logprobs"} {
		require.NotContains(t, completion, internal, "internal field reached the client")
	}

	// Admin routes carry a key. Without one configured they answer 404 rather than 401, so the
	// surface is invisible instead of merely locked; a configured key makes an unauthenticated call a
	// 401, which is what an unkeyed request gets here.
	unauthenticated, _, err := harness.PostJSONStatus(client, eps.RewriteHTTP+"/v1/admin/escrows", "", map[string]any{}, nil)
	require.NoError(t, err)
	require.Equal(t, 401, unauthenticated, "an admin route served an unkeyed caller")

	// A keyed call reaches the handler: an unknown escrow answers 404 where an unkeyed one gets 401.
	refused, refusedBody, err := harness.PostJSONStatus(client, eps.RewriteHTTP+"/v1/admin/devshards/999999/settle", config.DefaultAdminAPIKey, map[string]any{}, nil)
	require.NoError(t, err)
	require.Equal(t, 404, refused, "settle of an unknown escrow: %s", refusedBody)

	// A refused operator action has to reach the log stream. auditAdmin records only the successful
	// path, so before adminFailure a failed operator action left nothing behind to read.
	require.Eventually(t, func() bool {
		logs, err := stack.ComposeLogs("gateway")
		return err == nil && strings.Contains(logs, "admin request refused") && strings.Contains(logs, "status=404")
	}, 30*time.Second, time.Second, "the refused admin call left no line in the gateway log")

	// The money path end to end: the gateway signs a create, broadcasts it, waits for the escrow id
	// from the commit event, then settles that escrow and waits for the settle to commit.
	created := map[string]any{"model": "test-model", "amount": 500_000, "private_key_env": "DEVSHARD_PRIVATE_KEY", "activate": true}
	var escrow map[string]any
	createdStatus, createdBody, err := harness.PostJSONStatus(client, eps.RewriteHTTP+"/v1/admin/escrows", config.DefaultAdminAPIKey, created, &escrow)
	require.NoError(t, err)
	require.Equal(t, 200, createdStatus, "create escrow: %s", createdBody)
	createdID, isNumber := escrow["escrow_id"].(float64)
	require.True(t, isNumber, "create escrow returned %v", escrow)
	require.NotZero(t, createdID)
	escrowID := strconv.FormatUint(uint64(createdID), 10)
	t.Logf("rewrite created escrow %s on the mock chain", escrowID)

	settleStatus, settleBody, err := harness.PostJSONStatus(client, eps.RewriteHTTP+"/v1/admin/devshards/"+escrowID+"/settle", config.DefaultAdminAPIKey, map[string]any{}, nil)
	require.NoError(t, err)
	require.Equal(t, 200, settleStatus, "settle escrow %s: %s", escrowID, settleBody)
	t.Logf("rewrite settled escrow %s: %s", escrowID, settleBody)
}
