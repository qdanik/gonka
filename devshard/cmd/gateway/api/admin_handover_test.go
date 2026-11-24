package api

import (
	"net/http"
	"slices"
	"testing"

	"devshard/cmd/gateway/store"
)

// Test flow:
//  1. Register escrow "7".
//  2. POST its deactivate route with the handover query, then once without it.
//  3. Assert both answer 200 and the operations ran as a handover first and a plain deactivation second.
func TestOnlyTheHandoverQueryHandsAnEscrowOver(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = []store.DevshardRecord{{EscrowID: "7", Model: "qwen"}}

	for _, path := range []string{"/v1/admin/devshards/7/deactivate?handover=true", "/v1/admin/devshards/7/deactivate"} {
		if response := live.request(t, http.MethodPost, path, "", adminHeaders()); response.Code != http.StatusOK {
			t.Fatalf("POST %s = %d (%s), want 200", path, response.Code, response.Body.String())
		}
	}

	if calls := live.operations.recordedCalls(); !slices.Equal(calls, []string{"hand over", "deactivate"}) {
		t.Fatalf("operations ran %v, want [hand over deactivate]", calls)
	}
}
