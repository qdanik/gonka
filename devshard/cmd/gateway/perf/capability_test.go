package perf

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

var (
	capabilityModel = "model-a"
	capabilityNow   = time.Unix(1_700_000_000, 0)
)

const capabilityWindow = time.Hour

func TestCapabilityTrackerRecordContextLimitBlocksAboveStoredValue(t *testing.T) {
	c := newCapabilityTracker()
	c.recordContextLimit("participant-a", capabilityModel, 4096, capabilityNow)

	if reason, blocked := c.cannotServe("participant-a", capabilityModel, false, 5000, capabilityNow, capabilityWindow); !blocked || reason != "context_limit_exceeded" {
		t.Fatalf("cannotServe() over the stored limit = (%q, %v), want (context_limit_exceeded, true)", reason, blocked)
	}
	if _, blocked := c.cannotServe("participant-a", capabilityModel, false, 100, capabilityNow, capabilityWindow); blocked {
		t.Fatal("cannotServe() under the stored limit = true, want false")
	}
}

func TestCapabilityTrackerRecordContextLimitIgnoresZero(t *testing.T) {
	c := newCapabilityTracker()
	c.recordContextLimit("participant-a", capabilityModel, 0, capabilityNow)

	if _, blocked := c.cannotServe("participant-a", capabilityModel, false, 999999, capabilityNow, capabilityWindow); blocked {
		t.Fatal("cannotServe() after a zero-value record = true, want false (no limit stored)")
	}
}

func TestCapabilityTrackerRecordContextLimitUpdatesOnChange(t *testing.T) {
	c := newCapabilityTracker()
	c.recordContextLimit("participant-a", capabilityModel, 4096, capabilityNow)
	c.recordContextLimit("participant-a", capabilityModel, 8192, capabilityNow)

	if _, blocked := c.cannotServe("participant-a", capabilityModel, false, 8000, capabilityNow, capabilityWindow); blocked {
		t.Fatal("cannotServe() under the updated limit = true, want false")
	}
	if reason, blocked := c.cannotServe("participant-a", capabilityModel, false, 8193, capabilityNow, capabilityWindow); !blocked || reason != "context_limit_exceeded" {
		t.Fatalf("cannotServe() over the updated limit = (%q, %v), want (context_limit_exceeded, true)", reason, blocked)
	}
}

func TestCapabilityTrackerRecordToolUnsupportedIsIdempotent(t *testing.T) {
	c := newCapabilityTracker()
	c.recordToolUnsupported("participant-a", capabilityModel, capabilityNow)
	c.recordToolUnsupported("participant-a", capabilityModel, capabilityNow)

	if reason, blocked := c.cannotServe("participant-a", capabilityModel, true, 0, capabilityNow, capabilityWindow); !blocked || reason != "tool_choice_unsupported" {
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
				c.recordToolUnsupported("participant-a", capabilityModel, capabilityNow)
			}
			if testCase.contextLimit > 0 {
				c.recordContextLimit("participant-a", capabilityModel, testCase.contextLimit, capabilityNow)
			}

			reason, blocked := c.cannotServe("participant-a", capabilityModel, testCase.requiresTools, testCase.contextHint, capabilityNow, capabilityWindow)
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
				c.recordContextLimit(participant, capabilityModel, uint64(1000+j), capabilityNow)
			}
		}()
		go func() {
			defer wg.Done()
			for range iterations {
				c.recordToolUnsupported(participant, capabilityModel, capabilityNow)
			}
		}()
		go func() {
			defer wg.Done()
			for range iterations {
				_, _ = c.cannotServe(participant, capabilityModel, true, 500, capabilityNow, capabilityWindow)
			}
		}()
	}
	wg.Wait()

	for i := range participantCount {
		participant := fmt.Sprintf("participant-%d", i)
		if reason, blocked := c.cannotServe(participant, capabilityModel, true, 0, capabilityNow, capabilityWindow); !blocked || reason != "tool_choice_unsupported" {
			t.Fatalf("cannotServe(%s) after concurrent recordToolUnsupported = (%q, %v), want (tool_choice_unsupported, true)", participant, reason, blocked)
		}
	}
}

func TestAVersionRefusalBlocksEveryRequestShapeWhileItIsFresh(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	if _, blocked := tracker.cannotServe("host-0", capabilityModel, false, 0, capabilityNow, capabilityWindow); blocked {
		t.Fatal("a host with no recorded refusal was blocked")
	}
	tracker.recordVersionUnsupported("host-0", capabilityNow)

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
			reason, blocked := tracker.cannotServe("host-0", capabilityModel, shape.requiresTools, shape.contextHint, capabilityNow, capabilityWindow)
			if !blocked {
				t.Fatal("a host whose build cannot serve the protocol version was admitted")
			}
			if reason != CapabilityVersionUnsupported {
				t.Fatalf("reason = %q, want %q", reason, CapabilityVersionUnsupported)
			}
		})
	}

	if _, blocked := tracker.cannotServe("host-1", capabilityModel, false, 0, capabilityNow, capabilityWindow); blocked {
		t.Error("the refusal of one host blocked another")
	}
}

func TestCapabilityVerdictsStopBlockingOnceStale(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()
	tracker.recordVersionUnsupported("host-0", capabilityNow)
	tracker.recordToolUnsupported("host-1", capabilityModel, capabilityNow)
	tracker.recordContextLimit("host-2", capabilityModel, 1000, capabilityNow)

	later := capabilityNow.Add(capabilityWindow + time.Second)
	for _, probe := range []struct {
		participant   string
		requiresTools bool
		contextHint   uint64
	}{
		{participant: "host-0"},
		{participant: "host-1", requiresTools: true},
		{participant: "host-2", contextHint: 5000},
	} {
		if reason, blocked := tracker.cannotServe(probe.participant, capabilityModel, probe.requiresTools, probe.contextHint, later, capabilityWindow); blocked {
			t.Errorf("%s still blocked past the window with reason %q", probe.participant, reason)
		}
	}
}

func TestCapabilityEvictStaleDropsForgottenVerdicts(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()
	tracker.recordVersionUnsupported("gone", capabilityNow)
	tracker.recordVersionUnsupported("current", capabilityNow.Add(capabilityWindow))

	tracker.evictStale(capabilityNow.Add(capabilityWindow+time.Second), capabilityWindow)

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	if _, held := tracker.versionUnsupported["gone"]; held {
		t.Error("a verdict past the window survived the sweep")
	}
	if _, held := tracker.versionUnsupported["current"]; !held {
		t.Error("the sweep dropped a verdict that is still fresh")
	}
}

func TestACapabilityRefusalIsScopedToTheModelThatEarnedIt(t *testing.T) {
	const other = "model-b"
	tests := []struct {
		name        string
		record      func(*capabilityTracker)
		query       func(*capabilityTracker, string) bool
		blocksOther bool
	}{
		{
			name:   "tool refusal",
			record: func(c *capabilityTracker) { c.recordToolUnsupported("participant-a", capabilityModel, capabilityNow) },
			query: func(c *capabilityTracker, model string) bool {
				_, blocked := c.cannotServe("participant-a", model, true, 0, capabilityNow, capabilityWindow)
				return blocked
			},
		},
		{
			name: "context limit",
			record: func(c *capabilityTracker) {
				c.recordContextLimit("participant-a", capabilityModel, 4096, capabilityNow)
			},
			query: func(c *capabilityTracker, model string) bool {
				_, blocked := c.cannotServe("participant-a", model, false, 5000, capabilityNow, capabilityWindow)
				return blocked
			},
		},
		{
			name:   "version refusal",
			record: func(c *capabilityTracker) { c.recordVersionUnsupported("participant-a", capabilityNow) },
			query: func(c *capabilityTracker, model string) bool {
				_, blocked := c.cannotServe("participant-a", model, false, 0, capabilityNow, capabilityWindow)
				return blocked
			},
			blocksOther: true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			tracker := newCapabilityTracker()
			testCase.record(tracker)

			if !testCase.query(tracker, capabilityModel) {
				t.Fatal("the model that earned the refusal must be blocked")
			}
			if got := testCase.query(tracker, other); got != testCase.blocksOther {
				t.Errorf("another model blocked = %v, want %v", got, testCase.blocksOther)
			}
		})
	}
}
