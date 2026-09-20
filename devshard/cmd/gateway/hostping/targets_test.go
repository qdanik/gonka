package hostping

import (
	"testing"

	"devshard/cmd/gateway/registry"
)

type dialsHeld []registry.HostDial

func (d dialsHeld) HostDials() []registry.HostDial { return d }

// A probe reaches a host on its own route prefix, and asks the clock first because that is the only
// answer carrying the host's own time; liveness is the fallback when the host has no clock.
func TestATargetIsBuiltFromTheHostsOwnPrefix(t *testing.T) {
	source := Targets{Live: dialsHeld{{ParticipantKey: "gonka1abc", BaseURL: "http://a:8080/", RoutePrefix: "/api"}}}

	targets := source.Targets()

	if len(targets) != 1 {
		t.Fatalf("Targets() = %d, want 1", len(targets))
	}
	if targets[0].Key != "gonka1abc" {
		t.Errorf("key = %q, want the participant the metric is labelled by", targets[0].Key)
	}
	if targets[0].ClockURL != "http://a:8080/api/clock" {
		t.Errorf("clock URL = %q", targets[0].ClockURL)
	}
	if targets[0].FallbackURL != "http://a:8080/api/healthz" {
		t.Errorf("fallback URL = %q", targets[0].FallbackURL)
	}
}

// A host that named no prefix still has to be reachable, so the dial takes the one every host serves.
func TestAHostWithoutAPrefixTakesTheDefaultOne(t *testing.T) {
	source := Targets{Live: dialsHeld{{ParticipantKey: "gonka1abc", BaseURL: "http://a:8080"}}}

	targets := source.Targets()

	if len(targets) != 1 {
		t.Fatalf("Targets() = %d, want 1", len(targets))
	}
	if got := targets[0].ClockURL; got == "http://a:8080/clock" {
		t.Errorf("clock URL = %q, want it under the default route prefix", got)
	}
}

// Nothing live means nothing to ping, rather than one probe against an empty address.
func TestNothingLiveIsNothingToPing(t *testing.T) {
	source := Targets{Live: dialsHeld(nil)}

	if got := source.Targets(); len(got) != 0 {
		t.Fatalf("Targets() = %+v, want none", got)
	}
}
