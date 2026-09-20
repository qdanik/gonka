package config

import (
	"slices"
	"strings"
	"testing"

	"devshard/cmd/gateway/env"
)

func TestUnthrottledListReachesTheSnapshot(t *testing.T) {
	t.Parallel()
	wanted := []string{"gonka1scskt", "gonka1f0u3y"}

	built, err := Build(env.Values{}, Overrides{UnthrottledParticipants: &wanted})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !slices.Equal(built.Scheduler.UnthrottledParticipants, wanted) {
		t.Errorf("unthrottled = %v, want %v", built.Scheduler.UnthrottledParticipants, wanted)
	}
}

func TestUnthrottledListIsClonedFromTheOverride(t *testing.T) {
	t.Parallel()
	source := []string{"gonka1scskt"}

	built, err := Build(env.Values{}, Overrides{UnthrottledParticipants: &source})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	source[0] = "gonka1other"

	if built.Scheduler.UnthrottledParticipants[0] != "gonka1scskt" {
		t.Error("the snapshot aliased the caller's slice")
	}
}

func TestUnthrottledListIsIndependentOfTheAllowlist(t *testing.T) {
	t.Parallel()
	allowlist := []string{"gonka1scskt"}

	built, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &allowlist})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if len(built.Scheduler.UnthrottledParticipants) != 0 {
		t.Errorf("unthrottled = %v, want empty: naming an allowlist must waive nothing", built.Scheduler.UnthrottledParticipants)
	}
}

// Every log line names a host by its last eight characters, and that tail matches no participant.
func TestParticipantEntryMustLookLikeAnAddress(t *testing.T) {
	t.Parallel()
	tailFromALogLine := []string{"zalyrmkx"}

	_, err := Build(env.Values{}, Overrides{ParticipantAllowlist: &tailFromALogLine})

	if err == nil {
		t.Fatal("a short host label must be refused rather than narrowing dispatch to nobody")
	}
	if !strings.Contains(err.Error(), "participant_allowlist[0]") {
		t.Errorf("the complaint must name the entry, got %v", err)
	}
}

func TestUnthrottledEntryMustLookLikeAnAddress(t *testing.T) {
	t.Parallel()
	tailFromALogLine := []string{"zalyrmkx"}

	if _, err := Build(env.Values{}, Overrides{UnthrottledParticipants: &tailFromALogLine}); err == nil {
		t.Error("a short host label must be refused here too")
	}
}
