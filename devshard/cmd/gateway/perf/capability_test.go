package perf

import (
	"fmt"
	"sync"
	"testing"
)

const capabilityModel = "model-a"

func TestARecordedContextLimitIsWhatTheHostAdmittedTo(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	tracker.recordContextLimit("host-0", capabilityModel, 4096)

	limit, _, _, refusals := tracker.capability("host-0", capabilityModel)
	if limit != 4096 || refusals != 1 {
		t.Errorf("limit/refusals = %d/%d, want 4096 and one refusal", limit, refusals)
	}
}

// Zero is what a host sends when it will not say how large its context is, and storing it would report
// a limit of nothing rather than an unknown one.
func TestAZeroContextLimitIsNotRecorded(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	tracker.recordContextLimit("host-0", capabilityModel, 0)

	limit, _, _, refusals := tracker.capability("host-0", capabilityModel)
	if limit != 0 || refusals != 0 {
		t.Errorf("limit/refusals = %d/%d, want nothing recorded", limit, refusals)
	}
}

func TestTheLatestContextLimitReplacesTheOneBefore(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	tracker.recordContextLimit("host-0", capabilityModel, 8192)
	tracker.recordContextLimit("host-0", capabilityModel, 2048)

	limit, _, _, refusals := tracker.capability("host-0", capabilityModel)
	if limit != 2048 || refusals != 2 {
		t.Errorf("limit/refusals = %d/%d, want the newer 2048 and both refusals counted", limit, refusals)
	}
}

// Nothing here withholds a host from routing, so the only question a reader can ask is how often each
// refusal happened -- and a repeat is a build that refuses everything, not a one-off.
func TestRefusalsAreCountedRatherThanJudged(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	tracker.recordVersionUnsupported("host-0")
	tracker.recordVersionUnsupported("host-0")
	tracker.recordToolUnsupported("host-0", capabilityModel)

	_, versionRefusals, toolRefusals, _ := tracker.capability("host-0", capabilityModel)
	if versionRefusals != 2 || toolRefusals != 1 {
		t.Errorf("version/tool refusals = %d/%d, want 2 and 1", versionRefusals, toolRefusals)
	}
}

// A tool call and a context length belong to the model; a protocol version belongs to the build, so a
// refusal on one model must not be reported against another.
func TestAModelsRefusalIsNotReportedAgainstAnotherModel(t *testing.T) {
	t.Parallel()
	tracker := newCapabilityTracker()

	tracker.recordToolUnsupported("host-0", capabilityModel)
	tracker.recordContextLimit("host-0", capabilityModel, 4096)
	tracker.recordVersionUnsupported("host-0")

	limit, versionRefusals, toolRefusals, contextRefusals := tracker.capability("host-0", "model-b")
	if limit != 0 || toolRefusals != 0 || contextRefusals != 0 {
		t.Errorf("model-b reports limit %d, %d tool and %d context refusals, want none",
			limit, toolRefusals, contextRefusals)
	}
	if versionRefusals != 1 {
		t.Errorf("model-b reports %d version refusals, want the build's own 1", versionRefusals)
	}
}

func TestCapabilityTrackerConcurrentAccessIsRaceFree(t *testing.T) {
	tracker := newCapabilityTracker()
	const participantCount = 8
	const iterations = 200

	var waiting sync.WaitGroup
	for index := range participantCount {
		participant := fmt.Sprintf("participant-%d", index)
		waiting.Add(3)
		go func() {
			defer waiting.Done()
			for iteration := range iterations {
				tracker.recordContextLimit(participant, capabilityModel, uint64(1000+iteration))
			}
		}()
		go func() {
			defer waiting.Done()
			for range iterations {
				tracker.recordToolUnsupported(participant, capabilityModel)
			}
		}()
		go func() {
			defer waiting.Done()
			for range iterations {
				_, _, _, _ = tracker.capability(participant, capabilityModel)
			}
		}()
	}
	waiting.Wait()

	for index := range participantCount {
		participant := fmt.Sprintf("participant-%d", index)
		if _, _, toolRefusals, _ := tracker.capability(participant, capabilityModel); toolRefusals != iterations {
			t.Fatalf("%s counted %d tool refusals, want %d", participant, toolRefusals, iterations)
		}
	}
}
