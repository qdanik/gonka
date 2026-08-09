package api

import (
	"net/http"
	"strings"
	"testing"

	"devshard/cmd/gateway/store"
)

// The state endpoint speaks snake_case everywhere else in the same document; a storage row rendered
// straight to JSON spells its fields the way Go declares them and breaks that in one block. The
// assertion runs against the served body, not the view builders, so removing the conversion fails it.
func TestAdminStateSpellsStorageRowsInSnakeCase(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = []store.DevshardRecord{{EscrowID: "47452", PrivateKeyEnv: "GATEWAY_PRIVATE_KEY"}}
	live.control.rotation = []store.RotationStatus{{Model: "model-a", Stage: "prepared"}}

	body := live.request(t, http.MethodGet, "/v1/admin/state", "", adminHeaders()).Body.String()

	for _, want := range []string{
		`"escrow_id":"47452"`,
		`"private_key_env":"GATEWAY_PRIVATE_KEY"`,
		`"rotation_epoch":0`,
		`"stage":"prepared"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("state body %s is missing %s", body, want)
		}
	}
	for _, goFieldName := range []string{"EscrowID", "PrivateKeyEnv", "RotationEpoch", "Stage"} {
		if strings.Contains(body, goFieldName) {
			t.Fatalf("state body %s still carries the Go field name %s", body, goFieldName)
		}
	}
}
