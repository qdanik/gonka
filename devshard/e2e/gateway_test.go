package e2e

import (
	"context"
	"net/http"
	"testing"
	"time"

	"devshard/e2e/testutil"
)

// startGatewayEnv drives the same three-host stack the devshardctl scenarios use, with cmd/gateway as
// the binary under test. Everything below it -- mock chain, hosts, real HTTP between them -- is
// unchanged, so a difference in outcome is a difference in the gateway.
func startGatewayEnv(t *testing.T, opts e2eEnvOptions) (*e2eEnv, *http.Client) {
	t.Helper()
	requireE2EEnabled(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	opts.runGateway = true
	images := requireGatewayImage(t, requiredImages(t))
	env := startE2EEnv(ctx, t, images, opts)
	return env, &http.Client{Timeout: testutil.DefaultRequestTimeout}
}

// The stack boots and serves. Everything else in this file assumes it, so it is asserted on its own:
// a failure here is a wiring fault, not a behaviour one.
func TestE2E_GatewayServesTheStack(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	resp := testutil.SendCompletionRaw(t, client, env.clientURL, "gateway smoke", testutil.AdminAPIKey)
	testutil.LogRawResponse(t, "gateway smoke completion", resp)
	testutil.RequireOpenAINonStreamingCompletion(t, resp)
}

// gatewaySessionNonce reads the escrow's own state through the gateway's recovery surface. Two things
// differ from devshardctl: single-escrow mode is gone, so the route is addressed by id and gated on the
// admin key, and the state is returned flat rather than nested under a session object.
func gatewaySessionNonce(t *testing.T, client *http.Client, clientURL string) uint64 {
	t.Helper()
	state := testutil.GetJSON(t, client, clientURL+"/devshard/"+defaultEscrowID+"/v1/state")
	return testutil.NumericField(t, state, "latest_nonce")
}

// The production shape from issue #1660 and from the staging logs: a host drops out, the group keeps
// serving without it, and the escrow must keep advancing rather than stalling on the absent slot.
// nonce%3 binds every third nonce to the host that is gone, so this also exercises the walk past it.
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
		testutil.LogRawResponse(t, "during the outage", resp)
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
