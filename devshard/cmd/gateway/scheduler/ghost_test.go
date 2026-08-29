package scheduler

import "testing"

func TestGhostKindReason(t *testing.T) {
	tests := []struct {
		name string
		kind GhostKind
		want string
	}{
		{name: "poc", kind: ghostPoC, want: "poc_unavailable_host"},
		{name: "throttled", kind: ghostThrottled, want: "participant_throttled_no_send"},
		{name: "state diverged", kind: ghostStateDiverged, want: "participant_state_diverged_no_send"},
		{name: "exclude", kind: ghostExclude, want: "no_compatible_request_after_stale"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.kind.reason(); got != tt.want {
				t.Errorf("reason() = %q, want %q", got, tt.want)
			}
		})
	}
}

// An operator reads the reason to decide what to do, so two causes must never share one name.
func TestEveryGhostKindNamesItselfDistinctly(t *testing.T) {
	kinds := []GhostKind{
		ghostPoC, ghostThrottled, ghostEjected, ghostNotAllowed,
		ghostStateDiverged, ghostExclude, ghostAbandoned,
	}
	seen := make(map[string]GhostKind, len(kinds))
	for _, kind := range kinds {
		reason := kind.reason()
		if reason == "" {
			t.Errorf("GhostKind(%d) has no reason: a burn nothing can name reaches no counter", kind)
			continue
		}
		if other, taken := seen[reason]; taken {
			t.Errorf("GhostKind(%d) and GhostKind(%d) both report %q", other, kind, reason)
		}
		seen[reason] = kind
	}
}

var (
	_ Decision = serve{}
	_ Decision = burn{}
	_ Decision = hold{}
)
