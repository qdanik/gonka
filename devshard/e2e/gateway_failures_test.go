package e2e

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"devshard/internal/e2econfig"

	"devshard/e2e/testutil"
)

// brokenHost answers every dispatch with an HTTP error: reachable host, unreachable upstream.
func brokenHost(status, message string) map[string]string {
	return map[string]string{
		e2econfig.StubInferenceHTTPStatusEnv:  status,
		e2econfig.StubInferenceHTTPMessageEnv: message,
	}
}

// A host drops out: the group keeps serving and the escrow walks past the slot nonce%3 binds to it.
func TestE2E_GatewayServesThroughAHostOutage(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	ctx, cancel := context.WithTimeout(context.Background(), testutil.DefaultRequestTimeout)
	t.Cleanup(cancel)

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "before the outage", testutil.AdminAPIKey))
	beforeOutage := gatewaySessionNonce(t, client, env.clientURL)

	env.stopHost(ctx, t, 1)
	for request := range 4 {
		resp := testutil.SendCompletionRaw(t, client, env.clientURL, "during the outage", testutil.AdminAPIKey)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request %d during a one-host outage = %d %s, want the remaining two to serve it",
				request, resp.StatusCode, resp.Body)
		}
	}

	duringOutage := gatewaySessionNonce(t, client, env.clientURL)
	if duringOutage <= beforeOutage {
		t.Fatalf("session nonce %d did not advance past %d: the escrow stalled on the absent host",
			duringOutage, beforeOutage)
	}
}

// One host 502s and the rest are healthy: the group serves on, and its nonce still reaches the ledger.
func TestE2E_GatewayServesAroundAHostAnswering502(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{
		hostEnvOverrides: map[int]map[string]string{1: brokenHost("502", "upstream connect error")},
	})

	served := 0
	for request := range 6 {
		resp := testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("around a broken host %d", request), testutil.AdminAPIKey)
		if resp.StatusCode == http.StatusOK {
			served++
		}
	}

	if served == 0 {
		t.Fatal("every request failed: one host of three answering 502 must not take the model down")
	}
	if assigned := testutil.AccountingAssignedTotal(gatewayLedger(t, client, env.statsURL)); assigned == 0 {
		t.Fatal("the ledger assigned no nonces at all: nothing was accounted")
	}
}

// The money invariant: with every host refusing, each committed nonce still ends in one recorded outcome.
// It settles slowly -- no disposition lands until the refusal deadline passes, so an early read looks short.
func TestE2E_GatewayAccountsEveryNonceWhenEveryHostFails(t *testing.T) {
	broken := brokenHost("502", "upstream connect error")
	env, client := startGatewayEnv(t, e2eEnvOptions{
		hostEnvOverrides: map[int]map[string]string{0: broken, 1: broken, 2: broken},
	})

	for request := range 3 {
		resp := testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("nobody can serve %d", request), testutil.AdminAPIKey)
		testutil.LogRawResponse(t, "every host failing", resp)
	}

	settled := testutil.WaitAccountingParticipants(t, client, env.statsURL, "",
		func(resp testutil.AccountingParticipantsResponse) bool {
			assigned := testutil.AccountingAssignedTotal(resp)
			return assigned > 0 && testutil.AccountingDispositionTotal(resp) == assigned
		})

	assigned := testutil.AccountingAssignedTotal(settled)
	t.Logf("every nonce accounted: assigned=%d refused=%d", assigned,
		testutil.AccountingDispositionCount(settled, "unfinished_refused"))
	if codes := gatewayFindings(t, client, env.statsURL); slices.Contains(codes, "ledger_overcounted") {
		t.Errorf("findings %v report more nonces classified than the chain assigned", codes)
	}
}

// A diverging post-state-root earns one replay, then a permanent block: its nonces burn rather than send.
func TestE2E_GatewayBlocksAHostThatDivergesAndBurnsItsNonces(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{
		hostEnvOverrides: map[int]map[string]string{
			1: brokenHost("500", testutil.StateRootDivergenceMessage),
		},
	})

	// The first disagreement only spends the replay credit, so the slot has to come up more than once.
	for request := range 3 * len(env.hostURLs) {
		testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("state divergence %d", request), testutil.AdminAPIKey)
	}

	ledger := gatewayLedger(t, client, env.statsURL)
	burned := gatewayGhostBurns(t, client, env.statsURL, "participant_state_diverged_no_send")
	t.Logf("diverged burns=%d assigned=%d dispositions=%d",
		burned, testutil.AccountingAssignedTotal(ledger), testutil.AccountingDispositionTotal(ledger))
	if burned == 0 {
		t.Fatal("no nonce was burned for state divergence: the diverging host was still being sent work")
	}
	// No finding is asserted: the 20-nonce floor is far above what this spends, so the burn is the fact.
}

// A restart must not lose the books: what was counted before it is still counted after.
// It does NOT guard the cross-check restore -- both sides are zero here, so it passes without it.
func TestE2E_GatewayRestartInventsNoDisagreementWithTheChain(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{
		gatewayVolumeName: fmt.Sprintf("devshard-e2e-%s-gateway", strings.ToLower(t.Name())),
		gatewayEnvOverrides: map[string]string{
			// Snapshot often enough that the restart has something to restore beyond the shutdown write.
			"GATEWAY_NONCE_ACCOUNTING_SNAPSHOT_SECONDS": "2",
		},
		hostEnvOverrides: map[int]map[string]string{1: brokenHost("502", "upstream connect error")},
	})

	ctx, cancel := context.WithTimeout(context.Background(), testutil.DefaultRequestTimeout)
	t.Cleanup(cancel)

	for request := range 6 {
		testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("before the restart %d", request), testutil.AdminAPIKey)
	}
	before := gatewayLedger(t, client, env.statsURL)
	t.Logf("before restart: assigned=%d classified=%d errors=%d",
		testutil.AccountingAssignedTotal(before), testutil.AccountingDispositionTotal(before),
		crossCheckErrors(before))
	time.Sleep(3 * time.Second) // let one snapshot land before the process goes away

	env.restartGateway(ctx, t)

	after := testutil.WaitAccountingParticipants(t, client, env.statsURL, "",
		func(resp testutil.AccountingParticipantsResponse) bool { return len(resp.Participants) > 0 })
	t.Logf("after restart: assigned=%d classified=%d errors=%d",
		testutil.AccountingAssignedTotal(after), testutil.AccountingDispositionTotal(after),
		crossCheckErrors(after))

	if codes := gatewayFindings(t, client, env.statsURL); slices.Contains(codes, "ledger_disagrees_with_chain") {
		t.Fatalf("findings %v report a disagreement the restart invented", codes)
	}
}

// crossCheckErrors is the size of the disagreement between this gateway's counts and the chain's.
func crossCheckErrors(resp testutil.AccountingParticipantsResponse) uint64 {
	var total uint64
	for _, participant := range resp.Participants {
		total += participant.CrossChecks.ErrorCount
	}
	return total
}

// A host that receipts then goes silent must not stall the group, nor be filed as the cheap failure.
// The execution disposition is not asserted: it is due only past the chain's deadline, 1200s here.
func TestE2E_GatewayIsNotStalledByAHostThatReceiptsThenGoesSilent(t *testing.T) {
	stalling := map[string]string{e2econfig.StubInferenceDelayMillisEnv: "60000"}
	env, client := startGatewayEnv(t, e2eEnvOptions{
		hostEnvOverrides: map[int]map[string]string{1: stalling},
	})

	served := 0
	for request := range 6 {
		resp := testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("receipt then silence %d", request), testutil.AdminAPIKey)
		if resp.StatusCode == http.StatusOK {
			served++
		}
	}
	if served == 0 {
		t.Fatal("no request was served: one stalling host of three must not take the model down")
	}

	ledger := gatewayLedger(t, client, env.statsURL)
	if refused := testutil.AccountingDispositionCount(ledger, "unfinished_refused"); refused > 0 {
		t.Errorf("a stalled nonce was filed as a refusal %d time(s): the two failures cost different money",
			refused)
	}
	t.Logf("served=%d used=%d refused=%d",
		served, testutil.AccountingDispositionCount(ledger, "finished_used"),
		testutil.AccountingDispositionCount(ledger, "unfinished_refused"))
}

// A host that receipts then goes silent for good. Its warmup probe is the nonce that reaches a verdict,
// and a receipted probe is an execution failure: filing it as a refusal names the cheap failure.
func TestE2E_GatewayAccountsAStalledNonceEvenWhenTheGroupCallsItARefusal(t *testing.T) {
	requireSlowE2E(t)
	env, client := startGatewayEnv(t, e2eEnvOptions{
		gatewayEnvOverrides: map[string]string{
			"DEVSHARD_E2E":                          "1",
			e2econfig.StreamingHardTimeoutMillisEnv: "5000",
		},
		mockChainParams: map[string]any{"refusal_timeout": 5, "execution_timeout": 10},
		hostEnvOverrides: map[int]map[string]string{
			0: shortDeadlines(nil),
			1: shortDeadlines(map[string]string{e2econfig.StubInferenceDelayMillisEnv: "600000"}),
			2: shortDeadlines(nil),
		},
	})

	for request := range 6 {
		testutil.SendCompletionRaw(t, client, env.clientURL,
			fmt.Sprintf("stalled into a verdict %d", request), testutil.AdminAPIKey)
	}

	settled := awaitDisposition(t, client, env.statsURL, "unfinished_execution", 6*time.Minute)
	refused := testutil.AccountingDispositionCount(settled, "unfinished_refused")
	t.Logf("refused=%d execution=%d used=%d", refused,
		testutil.AccountingDispositionCount(settled, "unfinished_execution"),
		testutil.AccountingDispositionCount(settled, "finished_used"))
	if refused > 0 {
		t.Errorf("%d receipted nonce(s) were filed as refusals: the cheap failure names the expensive one", refused)
	}
}

// shortDeadlines gives a host the protocol deadlines this stand runs on, keeping whatever else the
// scenario asked of it.
func shortDeadlines(extra map[string]string) map[string]string {
	host := map[string]string{
		e2econfig.RefusalTimeoutSecondsEnv:   "5",
		e2econfig.ExecutionTimeoutSecondsEnv: "10",
	}
	maps.Copy(host, extra)
	return host
}

// awaitDisposition polls the ledger until one disposition appears, and fails naming what it saw instead.
func awaitDisposition(t *testing.T, client *http.Client, statsURL, disposition string, within time.Duration) testutil.AccountingParticipantsResponse {
	t.Helper()
	deadline := time.Now().Add(within)
	var last testutil.AccountingParticipantsResponse
	for time.Now().Before(deadline) {
		last = gatewayLedger(t, client, statsURL)
		if testutil.AccountingDispositionCount(last, disposition) > 0 {
			return last
		}
		time.Sleep(10 * time.Second)
	}
	t.Fatalf("no nonce reached %s within %s: refused=%d used=%d", disposition, within,
		testutil.AccountingDispositionCount(last, "unfinished_refused"),
		testutil.AccountingDispositionCount(last, "finished_used"))
	return last
}
