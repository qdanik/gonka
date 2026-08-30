package e2e

import (
	"fmt"

	"testing"

	"devshard/e2e/testutil"
)

// Ordinary traffic must land in the ledger as delivered work and raise nothing. This is the baseline
// every other accounting scenario is read against: if it drifts, none of the others mean anything.
func TestE2E_GatewayAccountsServedNonces(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	const served = 4
	for request := range served {
		testutil.RequireOpenAINonStreamingCompletion(t,
			testutil.SendCompletionRaw(t, client, env.clientURL, fmt.Sprintf("served %d", request), testutil.AdminAPIKey))
	}

	ledger := gatewayLedger(t, client, env.statsURL)
	if used := testutil.AccountingDispositionCount(ledger, "finished_used"); used < served {
		t.Errorf("finished_used = %d after %d served requests, want at least %d", used, served, served)
	}
	if codes := gatewayFindings(t, client, env.statsURL); len(codes) > 0 {
		t.Errorf("ordinary traffic raised findings %v, want none", codes)
	}
}

// The streamed path spends its nonce the same way the buffered one does. The two differ in how the
// answer reaches the client, not in what the escrow paid for, and the ledger must say so.
func TestE2E_GatewayAccountsAStreamedNonceLikeABufferedOne(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	before := testutil.AccountingDispositionCount(gatewayLedger(t, client, env.statsURL), "finished_used")
	testutil.RequireOpenAIStream(t, testutil.SendStreamingCompletion(t, client, env.clientURL, "streamed"))
	after := testutil.AccountingDispositionCount(gatewayLedger(t, client, env.statsURL), "finished_used")

	if after <= before {
		t.Fatalf("finished_used stayed at %d through a streamed request: the stream spent a nonce nothing recorded", before)
	}
	if codes := gatewayFindings(t, client, env.statsURL); len(codes) > 0 {
		t.Errorf("a streamed request raised findings %v, want none", codes)
	}
}

// A cache hit is answered without reaching a host, so it must spend no nonce. Serving one from cache and
// still paying for it would be the worst of both.
func TestE2E_GatewayCacheHitSpendsNoNonce(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "cached exactly", testutil.AdminAPIKey))
	afterFirst := gatewaySessionNonce(t, client, env.clientURL)

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "cached exactly", testutil.AdminAPIKey))
	afterSecond := gatewaySessionNonce(t, client, env.clientURL)

	if afterSecond != afterFirst {
		t.Fatalf("nonce moved %d -> %d on a repeated request: the cache hit still spent one", afterFirst, afterSecond)
	}
}

// A race with a loser: the second answer is spent work nobody used, not a failure. Speculation
// produces losers unaided -- an injected delay was measured and changed nothing.
func TestE2E_GatewayAccountsAnAnswerThatLostItsRace(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	for request := range 6 {
		testutil.RequireOpenAINonStreamingCompletion(t,
			testutil.SendCompletionRaw(t, client, env.clientURL,
				fmt.Sprintf("lost race %d", request), testutil.AdminAPIKey))
	}

	settled := testutil.WaitAccountingParticipants(t, client, env.statsURL, "",
		func(resp testutil.AccountingParticipantsResponse) bool {
			return testutil.AccountingDispositionCount(resp, "finished_unused") > 0
		})
	t.Logf("unused=%d used=%d",
		testutil.AccountingDispositionCount(settled, "finished_unused"),
		testutil.AccountingDispositionCount(settled, "finished_used"))
}
