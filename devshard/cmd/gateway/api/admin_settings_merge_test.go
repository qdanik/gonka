package api

import (
	"net/http"
	"testing"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
)

func storedAccessAndKillSwitch(live *harness) {
	disabled, open := true, config.ModelAccessOpen
	live.control.overrides = config.Overrides{Tunables: env.Tunables{Disabled: &disabled}, Models: map[string]config.ModelSettings{"qwen": {Access: &open}}}
}

// Test flow:
//  1. Store overrides that open a model and switch the gateway off.
//  2. POST a settings body naming only a request cap.
//  3. Assert the applied overrides still open the model and keep the kill switch on, beside the new cap: a partial write changes only what it names.
func TestAPostedSettingsPatchKeepsWhatItDoesNotName(t *testing.T) {
	live := newHarness(t)
	storedAccessAndKillSwitch(live)

	recorder := live.request(t, http.MethodPost, "/v1/admin/settings", `{"max_concurrent_requests":250}`, adminHeaders())

	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d (%s), want 200", recorder.Code, recorder.Body.String())
	}
	applied := live.operations.appliedOverrides()
	if applied.Models["qwen"].Access == nil || *applied.Models["qwen"].Access != config.ModelAccessOpen || applied.Disabled == nil || !*applied.Disabled {
		t.Fatalf("applied models %v disabled %v, want both kept", applied.Models, applied.Disabled)
	}
	if applied.MaxConcurrentRequests == nil || *applied.MaxConcurrentRequests != 250 {
		t.Fatalf("applied request cap %v, want 250", applied.MaxConcurrentRequests)
	}
}

// Test flow:
//  1. Store overrides that open a model and switch the gateway off.
//  2. PUT a settings body naming only a request cap.
//  3. Assert the applied overrides are that body alone: PUT writes the whole document.
func TestAPutSettingsDocumentReplacesTheStoredOne(t *testing.T) {
	live := newHarness(t)
	storedAccessAndKillSwitch(live)

	recorder := live.request(t, http.MethodPut, "/v1/admin/settings", `{"max_concurrent_requests":250}`, adminHeaders())

	if recorder.Code != http.StatusOK {
		t.Fatalf("status %d (%s), want 200", recorder.Code, recorder.Body.String())
	}
	applied := live.operations.appliedOverrides()
	if applied.Models != nil || applied.Disabled != nil || *applied.MaxConcurrentRequests != 250 {
		t.Fatalf("applied models %v disabled %v, want only the request cap", applied.Models, applied.Disabled)
	}
}
