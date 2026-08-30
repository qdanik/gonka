package e2e

import (
	"net/http"
	"testing"

	"devshard/e2e/testutil"
)

// Creating an escrow takes the NAME of the environment variable holding the signing key, never the key.
// A surface that accepted the key itself would put it in every request log and proxy buffer on the way.
func TestE2E_GatewayTakesAKeyByEnvNameOrNotAtAll(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	unnamed := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/admin/escrows",
		map[string]any{"model": "stub-model", "amount": 1000}, testutil.AdminAPIKey)
	if unnamed.StatusCode != http.StatusBadRequest {
		t.Errorf("creating an escrow without naming a key env = %d %s, want 400", unnamed.StatusCode, unnamed.Body)
	}
	incomplete := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/admin/escrows",
		map[string]any{"private_key_env": "SOME_KEY_ENV"}, testutil.AdminAPIKey)
	if incomplete.StatusCode != http.StatusBadRequest {
		t.Errorf("creating an escrow with no model or amount = %d %s, want 400", incomplete.StatusCode, incomplete.Body)
	}
}

// Lifting a quarantine names a participant, and an unknown one is refused rather than silently accepted:
// an operator has to know the key they typed did nothing.
func TestE2E_GatewayRefusesToUnquarantineAParticipantItDoesNotKnow(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	unknown := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/admin/participants/unquarantine",
		map[string]any{"participant_key": "gonka1nobody"}, testutil.AdminAPIKey)
	if unknown.StatusCode != http.StatusNotFound {
		t.Errorf("unquarantining an unknown participant = %d %s, want 404", unknown.StatusCode, unknown.Body)
	}
	blank := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/admin/participants/unquarantine",
		map[string]any{}, testutil.AdminAPIKey)
	if blank.StatusCode != http.StatusBadRequest {
		t.Errorf("unquarantining nobody = %d %s, want 400", blank.StatusCode, blank.Body)
	}
}

// Epoch 0 means "unconstrained" inside the ledger, so resetting it would clear every epoch at once. The
// route refuses it by name rather than by accident.
func TestE2E_GatewayRefusesToResetTheUnconstrainedEpoch(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	if reset := gatewayPost(t, client, env.clientURL+"/v1/admin/accounting/reset/1"); reset.StatusCode != http.StatusOK {
		t.Errorf("resetting epoch 1 = %d %s, want 200", reset.StatusCode, reset.Body)
	}
	zero := gatewayPost(t, client, env.clientURL+"/v1/admin/accounting/reset/0")
	if zero.StatusCode == http.StatusOK {
		t.Errorf("resetting epoch 0 was accepted, which would clear every epoch: %s", zero.Body)
	}
}

// An import names a file to read the escrow from; without one there is nothing to import.
func TestE2E_GatewayRefusesAnImportWithNothingToImport(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	empty := testutil.PostJSONRaw(t, client, env.clientURL+"/v1/admin/devshards/import",
		map[string]any{"escrow_id": defaultEscrowID}, testutil.AdminAPIKey)
	if empty.StatusCode != http.StatusBadRequest {
		t.Errorf("importing with no source path = %d %s, want 400", empty.StatusCode, empty.Body)
	}
}

// Finalize is the operator's way to close an escrow's outstanding work by hand. It is admin-gated and
// stays up under the kill switch, so what is checked is that it answers and refuses the public.
func TestE2E_GatewayAnswersOnFinalize(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	// 409 until the escrow is actually finalized: the route answers the question, and "not finalized" is
	// an answer. What must not happen is a 5xx or an anonymous read.
	finalize := env.clientURL + "/devshard/" + defaultEscrowID + "/v1/finalize"
	if status, body := gatewayGet(t, client, finalize, testutil.AdminAPIKey); status >= http.StatusInternalServerError {
		t.Errorf("GET finalize = %d %s, want an answer rather than a failure", status, body)
	}
	if status, _ := gatewayGet(t, client, finalize, ""); status == http.StatusOK {
		t.Error("finalize answered without the admin key")
	}
}
