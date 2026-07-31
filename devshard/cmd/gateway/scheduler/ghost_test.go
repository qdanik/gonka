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
		{name: "capability", kind: ghostCapability, want: "participant_capability_no_send"},
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

var (
	_ Decision = serve{}
	_ Decision = burn{}
	_ Decision = hold{}
)
