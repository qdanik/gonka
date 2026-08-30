package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"devshard/e2e/testutil"
)

// gatewayTimeoutActions counts the timeout rounds the ledger holds, by what became of each one.
func gatewayTimeoutActions(t *testing.T, client *http.Client, statsURL string) map[string]uint64 {
	t.Helper()
	body := testutil.GetJSON(t, client, statsURL+"/api/v1/epochs/current/participants")
	participants, _ := body["participants"].([]any)
	actions := map[string]uint64{}
	for _, rawParticipant := range participants {
		participant, ok := rawParticipant.(map[string]any)
		if !ok {
			continue
		}
		counters, _ := participant["counters"].([]any)
		for _, rawCounter := range counters {
			counter, ok := rawCounter.(map[string]any)
			if !ok {
				continue
			}
			action, _ := counter["timeout_action"].(string)
			if action == "" {
				continue
			}
			reason, _ := counter["timeout_reason"].(string)
			actions[action+"/"+reason] += testutil.NumericField(t, counter, "count")
		}
	}
	return actions
}

// The shape production showed on 2026-08-30: verify-timeout answered "session not found", "inference not
// found" or nothing at all, and the running weight never reached the threshold. A host restarted without
// its storage forgets the session, which is the same answer from the gateway's side.
//
// What must hold is the money invariant: the vote fails, and the nonce still reaches a recorded outcome
// rather than disappearing with the round that could not decide it.
func TestE2E_GatewayAccountsATimeoutItsVerifiersCouldNotDecide(t *testing.T) {
	requireSlowE2E(t)
	env, client := startGatewayEnv(t, e2eEnvOptions{
		mockChainParams: map[string]any{"refusal_timeout": 5, "execution_timeout": 10},
		hostEnvOverrides: map[int]map[string]string{
			0: shortDeadlines(nil),
			1: shortDeadlines(nil),
			2: shortDeadlines(nil),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)

	for request := range 6 {
		testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("before the verifiers forget %d", request), testutil.AdminAPIKey)
	}

	// One host stops answering, so its nonces need a timeout vote; the other two lose their storage, so
	// they cannot answer the vote when it comes.
	env.stopHost(ctx, t, 1)
	env.restartHost(ctx, t, 0)
	env.restartHost(ctx, t, 2)

	for request := range 6 {
		testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("after the verifiers forget %d", request), testutil.AdminAPIKey)
	}

	deadline := time.Now().Add(3 * time.Minute)
	for {
		actions := gatewayTimeoutActions(t, client, env.statsURL)
		if failedRounds(actions) > 0 {
			t.Logf("timeout rounds by outcome: %v", actions)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no timeout round was recorded as failed; the rounds that landed were %v", actions)
		}
		time.Sleep(3 * time.Second)
	}

	ledger := gatewayLedger(t, client, env.statsURL)
	var recorded uint64
	for _, disposition := range []string{"finished_used", "finished_unused", "finished_usage_unknown",
		"unfinished_refused", "unfinished_execution", "ghost"} {
		recorded += testutil.AccountingDispositionCount(ledger, disposition)
	}
	if recorded == 0 {
		t.Error("a round the verifiers could not decide took its nonces off the books entirely")
	}
}

func failedRounds(actions map[string]uint64) uint64 {
	var failed uint64
	for outcome, count := range actions {
		if len(outcome) >= 6 && outcome[:6] == "failed" {
			failed += count
		}
	}
	return failed
}

// A host that comes back without its storage must be caught up rather than left answering "session not
// found" forever: the group re-syncs it and serving continues.
func TestE2E_GatewayServesAfterAHostForgetsTheSession(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "before the amnesia", testutil.AdminAPIKey))

	env.restartHost(ctx, t, 1)

	awaitServed(t, client, env.clientURL, 90*time.Second)
	if codes := gatewayFindings(t, client, env.statsURL); len(codes) > 0 {
		t.Errorf("a host losing its storage raised findings %v, want none", codes)
	}
}
