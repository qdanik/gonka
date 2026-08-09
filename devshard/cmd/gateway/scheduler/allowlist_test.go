package scheduler

import "testing"

func TestAllowlistEmptyAdmitsEveryone(t *testing.T) {
	t.Parallel()
	allowed := allowedParticipants(nil)

	for _, participant := range []string{"gonka1a", "gonka1b", ""} {
		if !allowed(participant) {
			t.Errorf("%q must be admitted when no allowlist is configured", participant)
		}
	}
}

func TestAllowlistAdmitsOnlyWhatItNames(t *testing.T) {
	t.Parallel()
	allowed := allowedParticipants([]string{"gonka1scskt", "gonka1f0u3y"})

	for _, participant := range []string{"gonka1scskt", "gonka1f0u3y"} {
		if !allowed(participant) {
			t.Errorf("%q is on the list and must be admitted", participant)
		}
	}
	for _, participant := range []string{"gonka1other", ""} {
		if allowed(participant) {
			t.Errorf("%q is outside the list and must be refused", participant)
		}
	}
}

// An operator pasting keys from a console leaves spaces around them.
func TestAllowlistIgnoresSurroundingSpace(t *testing.T) {
	t.Parallel()
	allowed := allowedParticipants([]string{"  gonka1scskt \t"})

	if !allowed("gonka1scskt") {
		t.Error("a key padded with space must still admit its participant")
	}
}

// The allowlist is the first rung: a host outside it is not asked whether it is throttled or ejected,
// and its burn names the allowlist rather than a fleet problem the operator does not have.
func TestAllowlistBurnNamesItself(t *testing.T) {
	t.Parallel()
	blocked := availability{
		notAllowed:  func(string) bool { return true },
		pocRequired: func(string) bool { return true },
		throttled:   func(string) bool { return true },
		ejected:     func(string) bool { return true },
	}

	reason := blocked.participantBlocked("gonka1other")

	if reason != blockNotAllowed {
		t.Fatalf("reason = %v, want blockNotAllowed", reason)
	}
	if got := ghostFor[reason].reason(); got != "participant_outside_allowlist" {
		t.Errorf("burn reason = %q, want participant_outside_allowlist", got)
	}
}

func TestAllowlistLetsTheOtherRungsSpeakForAnAdmittedHost(t *testing.T) {
	t.Parallel()
	throttled := availability{
		notAllowed:  func(string) bool { return false },
		pocRequired: func(string) bool { return false },
		throttled:   func(string) bool { return true },
		ejected:     func(string) bool { return false },
	}

	if reason := throttled.participantBlocked("gonka1scskt"); reason != blockThrottled {
		t.Errorf("reason = %v, want blockThrottled", reason)
	}
}
