package perf

import (
	"fmt"
	"sync"
	"testing"
)

func TestCapabilityTrackerRecordContextLimitBlocksAboveStoredValue(t *testing.T) {
	c := newCapabilityTracker()
	c.recordContextLimit("participant-a", 4096)

	if reason, blocked := c.cannotServe("participant-a", false, 5000); !blocked || reason != "context_limit_exceeded" {
		t.Fatalf("cannotServe() over the stored limit = (%q, %v), want (context_limit_exceeded, true)", reason, blocked)
	}
	if _, blocked := c.cannotServe("participant-a", false, 100); blocked {
		t.Fatal("cannotServe() under the stored limit = true, want false")
	}
}

func TestCapabilityTrackerRecordContextLimitIgnoresZero(t *testing.T) {
	c := newCapabilityTracker()
	c.recordContextLimit("participant-a", 0)

	if _, blocked := c.cannotServe("participant-a", false, 999999); blocked {
		t.Fatal("cannotServe() after a zero-value record = true, want false (no limit stored)")
	}
}

func TestCapabilityTrackerRecordContextLimitUpdatesOnChange(t *testing.T) {
	c := newCapabilityTracker()
	c.recordContextLimit("participant-a", 4096)
	c.recordContextLimit("participant-a", 8192)

	if _, blocked := c.cannotServe("participant-a", false, 8000); blocked {
		t.Fatal("cannotServe() under the updated limit = true, want false")
	}
	if reason, blocked := c.cannotServe("participant-a", false, 8193); !blocked || reason != "context_limit_exceeded" {
		t.Fatalf("cannotServe() over the updated limit = (%q, %v), want (context_limit_exceeded, true)", reason, blocked)
	}
}

func TestCapabilityTrackerRecordToolUnsupportedIsIdempotent(t *testing.T) {
	c := newCapabilityTracker()
	c.recordToolUnsupported("participant-a")
	c.recordToolUnsupported("participant-a")

	if reason, blocked := c.cannotServe("participant-a", true, 0); !blocked || reason != "tool_choice_unsupported" {
		t.Fatalf("cannotServe() after two recordToolUnsupported calls = (%q, %v), want (tool_choice_unsupported, true)", reason, blocked)
	}
}

func TestCapabilityTrackerCannotServe(t *testing.T) {
	tests := []struct {
		name            string
		toolUnsupported bool
		contextLimit    uint64
		requiresTools   bool
		contextHint     uint64
		wantReason      string
		wantBlocked     bool
	}{
		{
			name:            "requires tools and participant is tool unsupported blocks",
			toolUnsupported: true,
			requiresTools:   true,
			wantReason:      "tool_choice_unsupported",
			wantBlocked:     true,
		},
		{
			name:          "requires tools but participant supports tools does not block",
			requiresTools: true,
			wantBlocked:   false,
		},
		{
			name:         "context hint over the known limit blocks",
			contextLimit: 1000,
			contextHint:  1001,
			wantReason:   "context_limit_exceeded",
			wantBlocked:  true,
		},
		{
			name:         "context hint equal to the known limit does not block",
			contextLimit: 1000,
			contextHint:  1000,
			wantBlocked:  false,
		},
		{
			name:         "context hint under the known limit does not block",
			contextLimit: 1000,
			contextHint:  500,
			wantBlocked:  false,
		},
		{
			name:        "no known capability limits does not block",
			contextHint: 999999,
			wantBlocked: false,
		},
		{
			name:            "tool_choice_unsupported takes precedence over an exceeded context limit",
			toolUnsupported: true,
			contextLimit:    100,
			requiresTools:   true,
			contextHint:     500,
			wantReason:      "tool_choice_unsupported",
			wantBlocked:     true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			c := newCapabilityTracker()
			if testCase.toolUnsupported {
				c.recordToolUnsupported("participant-a")
			}
			if testCase.contextLimit > 0 {
				c.recordContextLimit("participant-a", testCase.contextLimit)
			}

			reason, blocked := c.cannotServe("participant-a", testCase.requiresTools, testCase.contextHint)
			if reason != testCase.wantReason || blocked != testCase.wantBlocked {
				t.Fatalf("cannotServe() = (%q, %v), want (%q, %v)", reason, blocked, testCase.wantReason, testCase.wantBlocked)
			}
		})
	}
}

// TestCapabilityTrackerConcurrentAccessIsRaceFree must run with -race: it
// pins goroutine-safety, not any particular interleaving outcome.
func TestCapabilityTrackerConcurrentAccessIsRaceFree(t *testing.T) {
	c := newCapabilityTracker()
	const participantCount = 8
	const iterations = 200

	var wg sync.WaitGroup
	for i := range participantCount {
		participant := fmt.Sprintf("participant-%d", i)
		wg.Add(3)
		go func() {
			defer wg.Done()
			for j := range iterations {
				c.recordContextLimit(participant, uint64(1000+j))
			}
		}()
		go func() {
			defer wg.Done()
			for range iterations {
				c.recordToolUnsupported(participant)
			}
		}()
		go func() {
			defer wg.Done()
			for range iterations {
				_, _ = c.cannotServe(participant, true, 500)
			}
		}()
	}
	wg.Wait()

	for i := range participantCount {
		participant := fmt.Sprintf("participant-%d", i)
		if reason, blocked := c.cannotServe(participant, true, 0); !blocked || reason != "tool_choice_unsupported" {
			t.Fatalf("cannotServe(%s) after concurrent recordToolUnsupported = (%q, %v), want (tool_choice_unsupported, true)", participant, reason, blocked)
		}
	}
}

// The point of recording it: routing must skip the host with no timer, unlike an ejection that expires.
func TestAVersionRefusalBlocksTheHostForEveryRequestShape(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	if _, blocked := tracker.cannotServe("host-0", false, 0); blocked {
		t.Fatal("a host with no recorded refusal was blocked")
	}
	tracker.recordVersionUnsupported("host-0")

	for _, shape := range []struct {
		name          string
		requiresTools bool
		contextHint   uint64
	}{
		{name: "a plain request"},
		{name: "a request needing tools", requiresTools: true},
		{name: "a large request", contextHint: 100_000},
	} {
		t.Run(shape.name, func(t *testing.T) {
			reason, blocked := tracker.cannotServe("host-0", shape.requiresTools, shape.contextHint)
			if !blocked {
				t.Fatal("a host whose build cannot serve the protocol version was admitted")
			}
			if reason != CapabilityVersionUnsupported {
				t.Fatalf("reason = %q, want %q", reason, CapabilityVersionUnsupported)
			}
		})
	}

	if _, blocked := tracker.cannotServe("host-1", false, 0); blocked {
		t.Error("the refusal of one host blocked another")
	}
}
