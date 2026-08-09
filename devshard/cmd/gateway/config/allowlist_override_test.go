package config

import (
	"slices"
	"testing"

	"devshard/cmd/gateway/env"
)

func TestAllowlistOverrideReachesTheSnapshot(t *testing.T) {
	t.Parallel()
	wanted := []string{"gonka1scskt", "gonka1f0u3y", "gonka1z6xwd"}

	built, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &wanted})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !slices.Equal(built.Scheduler.ParticipantAllowlist, wanted) {
		t.Errorf("allowlist = %v, want %v", built.Scheduler.ParticipantAllowlist, wanted)
	}
}

// An override the operator did not send must not clear a list already in force.
func TestAllowlistSurvivesAnUnrelatedOverride(t *testing.T) {
	t.Parallel()
	wanted := []string{"gonka1scskt"}
	built, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &wanted})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(built.Scheduler.ParticipantAllowlist) != 1 {
		t.Fatalf("setup: allowlist = %v", built.Scheduler.ParticipantAllowlist)
	}

	empty := []string{}
	cleared, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &empty})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(cleared.Scheduler.ParticipantAllowlist) != 0 {
		t.Errorf("an explicit empty list must clear the narrowing, got %v", cleared.Scheduler.ParticipantAllowlist)
	}
}

// The snapshot must not alias the caller's slice, which Build clones for every other map and slice.
func TestAllowlistIsClonedFromTheOverride(t *testing.T) {
	t.Parallel()
	source := []string{"gonka1scskt"}

	built, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &source})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	source[0] = "gonka1other"

	if built.Scheduler.ParticipantAllowlist[0] != "gonka1scskt" {
		t.Error("the snapshot aliased the caller's slice")
	}
}

func TestBlankAllowlistEntryIsRefused(t *testing.T) {
	t.Parallel()
	blank := []string{"gonka1scskt", "  "}

	if _, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &blank}); err == nil {
		t.Error("a blank entry must be refused rather than silently narrowing dispatch")
	}
}
