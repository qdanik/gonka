package config

import (
	"slices"
	"strings"
	"testing"

	"devshard/cmd/gateway/env"
)

// Test flow:
//  1. Build a config with a `Participants.Shadow` override.
//  2. Assert the built scheduler's shadow list matches it and the other two lists stay empty.
func TestShadowListReachesTheSnapshot(t *testing.T) {
	t.Parallel()
	wanted := []string{"gonka1scskt"}

	built, err := Build(env.Values{}, Overrides{Participants: &ParticipantOverrides{Shadow: &wanted}})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if !slices.Equal(built.Scheduler.Participants.Shadow, wanted) {
		t.Errorf("shadow = %v, want %v", built.Scheduler.Participants.Shadow, wanted)
	}
	if len(built.Scheduler.Participants.Trust) != 0 || len(built.Scheduler.Participants.Unthrottled) != 0 {
		t.Errorf("trust = %v, unthrottled = %v, want both empty: naming a shadow list must route and waive nothing", built.Scheduler.Participants.Trust, built.Scheduler.Participants.Unthrottled)
	}
}

// Test flow:
//  1. Build a config whose shadow list names a participant that `trust` also names.
//  2. Assert `Build` refuses it and the error names the participant and both lists.
//  3. Repeat with `unthrottled` in place of `trust`.
//  4. Repeat with the shadow entry padded with space and assert it is still refused.
func TestShadowOverlappingARoutedListIsRefused(t *testing.T) {
	t.Parallel()
	overlap := []string{"gonka1scskt"}
	padded := []string{"  gonka1scskt "}
	cases := map[string]ParticipantOverrides{
		"trust":       {Shadow: &overlap, Trust: &overlap},
		"unthrottled": {Shadow: &overlap, Unthrottled: &overlap},
		"padded":      {Shadow: &padded, Trust: &overlap},
	}
	for name, participants := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := Build(env.Values{}, Overrides{Participants: &participants})

			if err == nil {
				t.Fatal("a participant both probed only and routed client traffic must be refused")
			}
			if !strings.Contains(err.Error(), "gonka1scskt") || !strings.Contains(err.Error(), "participants.shadow") {
				t.Errorf("the complaint must name the participant and the shadow list, got %v", err)
			}
		})
	}
}

// Test flow:
//  1. Build a config naming one participant in both `trust` and `unthrottled`.
//  2. Assert `Build` accepts it, as `trust` plus `unthrottled` is allowed.
func TestTrustAndUnthrottledMayNameTheSameParticipant(t *testing.T) {
	t.Parallel()
	both := []string{"gonka1scskt"}

	if _, err := Build(env.Values{}, Overrides{Participants: &ParticipantOverrides{Trust: &both, Unthrottled: &both}}); err != nil {
		t.Errorf("Build: %v, want nil", err)
	}
}

// Test flow:
//  1. Build a config with a `Participants.Shadow` entry that is a log line's host tail.
//  2. Assert `Build` refuses it and the error names the entry by its list and index.
func TestShadowEntryMustLookLikeAnAddress(t *testing.T) {
	t.Parallel()
	tailFromALogLine := []string{"zalyrmkx"}

	_, err := Build(env.Values{}, Overrides{Participants: &ParticipantOverrides{Shadow: &tailFromALogLine}})

	if err == nil || !strings.Contains(err.Error(), "participants.shadow[0]") {
		t.Errorf("err = %v, want a complaint naming participants.shadow[0]", err)
	}
}

// Test flow:
//  1. Parse a stored document written before `participants` existed, carrying both retired keys beside a tunable.
//  2. Assert the lists arrive as `Participants.Trust` and `Participants.Unthrottled` and the tunable is kept.
//  3. Build a config from it and assert the scheduler lists equal the retired ones.
func TestStoredLegacyKeysBecomeParticipants(t *testing.T) {
	t.Parallel()
	stored := []byte(`{"participant_allowlist":["gonka1scskt"],"unthrottled_participants":["gonka1f0u3y"],"max_concurrent_requests":7}`)

	overrides, err := ParseStoredOverrides(stored)
	if err != nil {
		t.Fatalf("ParseStoredOverrides: %v", err)
	}
	built, err := Build(env.Values{}, overrides)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if overrides.MaxConcurrentRequests == nil || *overrides.MaxConcurrentRequests != 7 {
		t.Errorf("max_concurrent_requests = %v, want 7 kept", overrides.MaxConcurrentRequests)
	}
	if !slices.Equal(built.Scheduler.Participants.Trust, []string{"gonka1scskt"}) || !slices.Equal(built.Scheduler.Participants.Unthrottled, []string{"gonka1f0u3y"}) {
		t.Errorf("participants = %+v, want the retired lists as trust and unthrottled", built.Scheduler.Participants)
	}
	if len(built.Scheduler.Participants.Shadow) != 0 {
		t.Errorf("shadow = %v, want empty", built.Scheduler.Participants.Shadow)
	}
}

// Test flow:
//  1. Parse a stored document carrying `participants.trust` and the retired `participant_allowlist`.
//  2. Assert the list under `participants` wins and the retired unthrottled key still folds in.
func TestStoredParticipantsWinOverRetiredKeys(t *testing.T) {
	t.Parallel()
	stored := []byte(`{"participants":{"trust":["gonka1new"]},"participant_allowlist":["gonka1old"],"unthrottled_participants":["gonka1f0u3y"]}`)

	overrides, err := ParseStoredOverrides(stored)
	if err != nil {
		t.Fatalf("ParseStoredOverrides: %v", err)
	}

	if !slices.Equal(*overrides.Participants.Trust, []string{"gonka1new"}) || !slices.Equal(*overrides.Participants.Unthrottled, []string{"gonka1f0u3y"}) {
		t.Errorf("participants = %v / %v, want the new trust list and the folded unthrottled one", *overrides.Participants.Trust, *overrides.Participants.Unthrottled)
	}
}

// Test flow:
//  1. Parse an admin document naming a retired key.
//  2. Assert `ParseOverrides` refuses it, as the admin API accepts only `participants`.
func TestAdminDocumentRefusesTheRetiredKeys(t *testing.T) {
	t.Parallel()
	for _, retired := range []string{"participant_allowlist", "unthrottled_participants"} {
		if _, err := ParseOverrides([]byte(`{"` + retired + `":["gonka1scskt"]}`)); err == nil {
			t.Errorf("ParseOverrides accepted the retired key %q", retired)
		}
	}
}

// Test flow:
//  1. Start from stored participants naming trust and shadow, and patch only `shadow`.
//  2. Assert trust stays and shadow is replaced.
//  3. Patch `trust` to null and assert it is cleared while shadow stays.
//  4. Patch with an empty `participants` object and assert nothing changes.
//  5. Patch `participants` itself to null and assert the whole key is cleared.
func TestMergingParticipantsWorksPerSubKey(t *testing.T) {
	t.Parallel()
	trust, shadow := []string{"gonka1trust"}, []string{"gonka1shadow"}
	stored := Overrides{Participants: &ParticipantOverrides{Trust: &trust, Shadow: &shadow}}

	replaced, err := stored.Merged([]byte(`{"participants":{"shadow":["gonka1other"]}}`))
	if err != nil {
		t.Fatalf("Merged: %v", err)
	}
	if !slices.Equal(*replaced.Participants.Trust, trust) || !slices.Equal(*replaced.Participants.Shadow, []string{"gonka1other"}) {
		t.Fatalf("replaced = %v / %v, want trust kept and shadow replaced", *replaced.Participants.Trust, *replaced.Participants.Shadow)
	}

	cleared, err := replaced.Merged([]byte(`{"participants":{"trust":null}}`))
	if err != nil {
		t.Fatalf("Merged: %v", err)
	}
	if cleared.Participants.Trust != nil || cleared.Participants.Shadow == nil {
		t.Fatalf("cleared = %+v, want trust cleared and shadow kept", cleared.Participants)
	}

	untouched, err := cleared.Merged([]byte(`{"participants":{}}`))
	if err != nil {
		t.Fatalf("Merged: %v", err)
	}
	if untouched.Participants.Shadow == nil {
		t.Fatalf("untouched = %+v, want shadow kept", untouched.Participants)
	}

	wiped, err := untouched.Merged([]byte(`{"participants":null}`))
	if err != nil {
		t.Fatalf("Merged: %v", err)
	}
	if wiped.Participants != nil {
		t.Errorf("wiped = %+v, want participants cleared", wiped.Participants)
	}
}

// Test flow:
//  1. Merge a patch naming `participants` with an unknown sub-key into empty overrides.
//  2. Assert it is refused: a typo inside `participants` must not pass as a no-op write.
func TestMergingParticipantsRefusesAnUnknownSubKey(t *testing.T) {
	t.Parallel()
	if _, err := (Overrides{}).Merged([]byte(`{"participants":{"shadows":["gonka1scskt"]}}`)); err == nil {
		t.Fatal("Merged() = nil, want an unknown-field error")
	}
}

// Test flow:
//  1. Merge a `participants` patch into empty overrides.
//  2. Assert the sub-keys it names are set and the others stay nil.
func TestMergingParticipantsIntoNothingSetsTheNamedSubKeys(t *testing.T) {
	t.Parallel()
	merged, err := (Overrides{}).Merged([]byte(`{"participants":{"unthrottled":["gonka1scskt"]}}`))
	if err != nil {
		t.Fatalf("Merged: %v", err)
	}

	if merged.Participants == nil || merged.Participants.Unthrottled == nil || merged.Participants.Trust != nil || merged.Participants.Shadow != nil {
		t.Errorf("participants = %+v, want only unthrottled set", merged.Participants)
	}
}
