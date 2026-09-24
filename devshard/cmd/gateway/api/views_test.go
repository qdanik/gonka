package api

import (
	"net/http"
	"strings"
	"testing"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/store"
)

// Test flow:
//  1. Seed the harness's admin control with a devshard record and a rotation status.
//  2. Request the /v1/admin/state endpoint.
//  3. Assert the response body carries the expected fields in snake_case.
//  4. Assert the body does not carry the underlying Go struct field names.
func TestAdminStateSpellsStorageRowsInSnakeCase(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = []store.DevshardRecord{{EscrowID: "47452", PrivateKeyEnv: "GATEWAY_PRIVATE_KEY", OnHold: true}}
	live.control.rotation = []store.RotationStatus{{Model: "model-a", Stage: "prepared"}}

	body := live.request(t, http.MethodGet, "/v1/admin/state", "", adminHeaders()).Body.String()

	for _, want := range []string{
		`"escrow_id":"47452"`,
		`"private_key_env":"GATEWAY_PRIVATE_KEY"`,
		`"rotation_epoch":0`,
		`"on_hold":true`,
		`"stage":"prepared"`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("state body %s is missing %s", body, want)
		}
	}
	for _, goFieldName := range []string{"EscrowID", "PrivateKeyEnv", "RotationEpoch", "OnHold", "Stage"} {
		if strings.Contains(body, goFieldName) {
			t.Fatalf("state body %s still carries the Go field name %s", body, goFieldName)
		}
	}
}

// Test flow:
//  1. For each case, varying ForceUpstreamStreaming across on and rolled back, build filter options from that config.
//  2. Assert KeepClientStream is the opposite of ForceUpstreamStreaming in each case.
func TestTheForcedStreamingSwitchReachesTheFilters(t *testing.T) {
	tests := []struct {
		name                 string
		forceUpstream        bool
		wantKeepClientStream bool
	}{
		{name: "forcing on, as it ships", forceUpstream: true, wantKeepClientStream: false},
		{name: "forcing rolled back", forceUpstream: false, wantKeepClientStream: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			options := filterOptions(config.Limits{ForceUpstreamStreaming: testCase.forceUpstream}, false)

			if options.KeepClientStream != testCase.wantKeepClientStream {
				t.Errorf("KeepClientStream = %v, want %v", options.KeepClientStream, testCase.wantKeepClientStream)
			}
		})
	}
}
