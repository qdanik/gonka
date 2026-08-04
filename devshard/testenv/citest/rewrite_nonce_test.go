package citest

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"devshard/testenv/citest/harness"
	"devshard/testenv/config"

	"github.com/stretchr/testify/require"
)

// TestRewriteNonceBindsOneHostPerNonce drives a four-slot escrow and checks the rule the whole
// settlement model rests on: the host that serves a nonce is slot nonce%len(slots), so a nonce is
// spent against exactly one participant and the chain can price it. A single-slot stand cannot see
// this -- every nonce maps to slot 0 whether the rule holds or not.
func TestRewriteNonceBindsOneHostPerNonce(t *testing.T) {
	if os.Getenv("TESTENV_GATEWAY_SMOKE") != "1" {
		t.Skip("set TESTENV_GATEWAY_SMOKE=1 to run the rewrite against the full stack (Docker)")
	}

	stack := harness.NewStack(t, "citest-rewrite-nonce-*")
	harness.RequireLinuxDevshardd(t, stack.TestenvDir)

	// Four hosts, not two: under versiond mode multi a two-host stand is an HA pair sharing one key, so
	// one participant owns every slot and nonce%len(slots) resolves to the same host for every nonce.
	harness.WriteMultiConfig(t, stack.WorkDir, harness.MultiConfigOpts{Hosts: 4, EscrowSlots: 4})
	stack.RunGencompose(t)
	stack.UpBuild(t)

	cfg := stack.LoadConfig(t)
	eps := stack.Endpoints(t, cfg)
	client := harness.HTTPClient()

	t.Cleanup(func() {
		if t.Failed() {
			harness.DumpComposeLogs(t, stack, "gateway", "versiond-0", "versiond-1", "mock-openai", "mock-chain")
		}
	})

	harness.WaitGETOK(t, client, eps.RewriteHTTP+"/v1/status", 3*time.Minute, "rewrite /v1/status")

	var participants map[string]any
	require.NoError(t, harness.GetJSONAuth(client, eps.RewriteHTTP+"/v1/admin/devshards/1/participants", config.DefaultAdminAPIKey, &participants))
	rawSlots, isList := participants["slots"].([]any)
	require.True(t, isList, "participants: %v", participants)
	require.Len(t, rawSlots, 4, "the stand must publish a four-slot escrow or the rule is untestable")
	slots := make([]string, len(rawSlots))
	for index, raw := range rawSlots {
		slots[index], _ = raw.(string)
	}
	distinct := make(map[string]struct{}, len(slots))
	for _, slot := range slots {
		distinct[slot] = struct{}{}
	}
	require.Greater(t, len(distinct), 1, "one participant holds every slot, so this test passes whether the rule holds or not: %v", slots)
	t.Logf("escrow 1 slots: %v", slots)

	for request := range 8 {
		chat := map[string]any{
			"model":      "test-model",
			"messages":   []map[string]string{{"role": "user", "content": fmt.Sprintf("nonce binding %d", request)}},
			"max_tokens": 16,
		}
		requestID, status, body, err := harness.PostJSONRequestID(client, eps.RewriteHTTP+"/v1/chat/completions", chat)
		require.NoError(t, err)
		require.Equal(t, 200, status, "completion %d: %s", request, body)
		require.NotEmpty(t, requestID, "the gateway must return a request id to reconcile against")

		var record map[string]any
		require.NoError(t, harness.GetJSONAuth(client, eps.RewriteHTTP+"/v1/requests/"+requestID, config.DefaultAdminAPIKey, &record))
		nonce, isNumber := record["winner_nonce"].(float64)
		require.True(t, isNumber, "request %s has no winner nonce: %v", requestID, record)
		participant, _ := record["winner_participant"].(string)
		require.NotEmpty(t, participant)

		t.Logf("nonce %d served by %s", uint64(nonce), participant)
		expected := slots[uint64(nonce)%uint64(len(slots))]
		require.Equal(t, expected, participant,
			"nonce %s went to %s, but slot %s owns it", strconv.FormatUint(uint64(nonce), 10), participant, strconv.FormatUint(uint64(nonce)%uint64(len(slots)), 10))
	}
}
