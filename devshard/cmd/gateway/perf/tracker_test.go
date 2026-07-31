package perf

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
)

func testPerf() config.Perf {
	return config.Perf{
		EWMAHalfLifeSeconds:      600,
		ConsecutiveFailThreshold: 3,
		FailureRateThreshold:     0.5,
		FailureRateMinVolume:     5,
		EjectionBaseSeconds:      30,
		EjectionMaxSeconds:       600,
		MaxEjectionFraction:      0.5,
		MinAvailableHosts:        1,
		HostStalenessSeconds:     60,
	}
}

func newTestTracker(perf config.Perf, now func() time.Time) *Tracker {
	return NewTracker(config.NewHolder(&config.Config{Perf: perf}), now)
}

func fixedNow(instant time.Time) func() time.Time {
	return func() time.Time { return instant }
}

func failAllConsecutive(tracker *Tracker, participant, model string, count int64) {
	for i := int64(0); i < count; i++ {
		tracker.RecordSample(Sample{ParticipantKey: participant, Model: model, Responsive: false})
	}
}

// noCap disables the max-ejection cap (see the dedicated Ejected*Cap* tests)
// so a lone host's own trigger can be observed in isolation: with only one
// known host, any fraction < 1 or MinAvailableHosts >= 1 floors the cap's
// "allowed" count to 0 and would otherwise mask the trigger under test.
func noCap(perf config.Perf) config.Perf {
	perf.MaxEjectionFraction = 1.0
	perf.MinAvailableHosts = 0
	return perf
}

func TestTrackerEjectedTrueAfterConsecutiveFailThreshold(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold)

	if !tracker.Ejected("participant-a", "model-a") {
		t.Fatal("Ejected() after reaching the consecutive-fail threshold = false, want true")
	}
}

func TestTrackerEjectedFalseBeforeConsecutiveFailThreshold(t *testing.T) {
	perf := noCap(testPerf())
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	failAllConsecutive(tracker, "participant-a", "model-a", perf.ConsecutiveFailThreshold-1)

	if tracker.Ejected("participant-a", "model-a") {
		t.Fatal("Ejected() below the consecutive-fail threshold = true, want false")
	}
}

func TestTrackerEjectedFalseForNeverRecordedHost(t *testing.T) {
	tracker := newTestTracker(testPerf(), fixedNow(testEpoch))

	if tracker.Ejected("never-seen", "model-a") {
		t.Fatal("Ejected() for a never-recorded host = true, want false")
	}
}

func TestTrackerEjectedRespectsConfiguredEjectionBaseSeconds(t *testing.T) {
	perf := noCap(testPerf())
	perf.EjectionBaseSeconds = 10
	perf.ConsecutiveFailThreshold = 1
	current := testEpoch
	tracker := newTestTracker(perf, func() time.Time { return current })

	tracker.RecordSample(Sample{ParticipantKey: "participant-a", Model: "model-a", Responsive: false})
	if !tracker.Ejected("participant-a", "model-a") {
		t.Fatal("Ejected() right after the trigger = false, want true")
	}

	current = current.Add(9 * time.Second)
	if !tracker.Ejected("participant-a", "model-a") {
		t.Fatal("Ejected() 9s into a 10s ejection base = false, want true (still within window)")
	}

	current = current.Add(2 * time.Second) // 11s total, past the configured 10s base
	if tracker.Ejected("participant-a", "model-a") {
		t.Fatal("Ejected() 11s into a 10s ejection base = true, want false (window elapsed)")
	}
}

func TestTrackerEjectedCapLimitsFractionAcrossManyFailingHosts(t *testing.T) {
	perf := testPerf()
	perf.MaxEjectionFraction = 0.5
	perf.MinAvailableHosts = 1
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	participants := []string{"p0", "p1", "p2", "p3"}
	for _, participant := range participants {
		failAllConsecutive(tracker, participant, "model-a", perf.ConsecutiveFailThreshold)
	}

	if !tracker.Ejected("p0", "model-a") || !tracker.Ejected("p1", "model-a") {
		t.Fatal("expected p0 and p1 (lexicographically first) to remain reported as ejected")
	}
	if tracker.Ejected("p2", "model-a") || tracker.Ejected("p3", "model-a") {
		t.Fatal("expected p2 and p3 to be pardoned by the ejection cap")
	}
}

// The cap pardons a host for routing only; the detector's own verdict must stay visible, because that is
// what the race hedges on once a correlated outage puts failing hosts back in rotation.
func TestTrackerReportsCapPardonedHostsAsDegradedButNotEjected(t *testing.T) {
	perf := testPerf()
	perf.MaxEjectionFraction = 0.5
	perf.MinAvailableHosts = 1
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	participants := []string{"p0", "p1", "p2", "p3"}
	for _, participant := range participants {
		failAllConsecutive(tracker, participant, "model-a", perf.ConsecutiveFailThreshold)
	}

	for _, participant := range participants {
		if !tracker.Degraded(participant, "model-a") {
			t.Fatalf("Degraded(%q) = false, want the detector's verdict before the cap", participant)
		}
	}
	if tracker.Ejected("p3", "model-a") {
		t.Fatal("p3 was withheld from routing, so the cap did not pardon it")
	}
	if tracker.Degraded("p0", "other-model") {
		t.Fatal("Degraded is not keyed by model")
	}
}

func TestTrackerEjectedCapBoundByMinAvailableHosts(t *testing.T) {
	perf := testPerf()
	perf.MaxEjectionFraction = 1.0 // fraction alone would allow all 3 ejected
	perf.MinAvailableHosts = 2
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	participants := []string{"p0", "p1", "p2"}
	for _, participant := range participants {
		failAllConsecutive(tracker, participant, "model-a", perf.ConsecutiveFailThreshold)
	}

	if !tracker.Ejected("p0", "model-a") {
		t.Fatal("expected p0 (lexicographically first) to remain reported as ejected")
	}
	if tracker.Ejected("p1", "model-a") || tracker.Ejected("p2", "model-a") {
		t.Fatal("expected p1 and p2 to be pardoned: MinAvailableHosts=2 of 3 known hosts allows only 1 ejected")
	}
}

func TestTrackerEjectedCapNeverEjectsTheOnlyKnownHost(t *testing.T) {
	perf := testPerf()
	perf.MinAvailableHosts = 1
	tracker := newTestTracker(perf, fixedNow(testEpoch))

	failAllConsecutive(tracker, "lone-host", "model-a", perf.ConsecutiveFailThreshold)

	if tracker.Ejected("lone-host", "model-a") {
		t.Fatal("Ejected() for the only known host = true, want false (MinAvailableHosts floor)")
	}
}

func TestTrackerCannotServeDelegatesToCapability(t *testing.T) {
	tracker := newTestTracker(testPerf(), fixedNow(testEpoch))

	tracker.RecordToolUnsupported("participant-a")
	tracker.RecordContextLimit("participant-b", 100)

	if reason, blocked := tracker.CannotServe("participant-a", true, 0); !blocked || reason != "tool_choice_unsupported" {
		t.Fatalf("CannotServe(tool-unsupported participant) = (%q, %v), want (tool_choice_unsupported, true)", reason, blocked)
	}
	if reason, blocked := tracker.CannotServe("participant-b", false, 101); !blocked || reason != "context_limit_exceeded" {
		t.Fatalf("CannotServe(over context limit) = (%q, %v), want (context_limit_exceeded, true)", reason, blocked)
	}
	if _, blocked := tracker.CannotServe("participant-c", true, 999999); blocked {
		t.Fatal("CannotServe(unknown participant) blocked = true, want false")
	}
}

func TestTrackerAcquireReleaseTracksInflight(t *testing.T) {
	tracker := newTestTracker(testPerf(), fixedNow(testEpoch))

	tracker.Acquire("participant-a")
	tracker.Acquire("participant-a")
	if got := tracker.Inflight("participant-a"); got != 2 {
		t.Fatalf("Inflight() after 2 acquires = %d, want 2", got)
	}

	tracker.Release("participant-a")
	if got := tracker.Inflight("participant-a"); got != 1 {
		t.Fatalf("Inflight() after 1 release = %d, want 1", got)
	}
}

func TestTrackerRecordSampleLazilyEvictsHostsUnseenPastStaleness(t *testing.T) {
	perf := testPerf()
	perf.HostStalenessSeconds = 60
	current := testEpoch
	tracker := newTestTracker(perf, func() time.Time { return current })

	tracker.RecordSample(Sample{ParticipantKey: "stale-host", Model: "model-a", Responsive: true})
	if got := len(tracker.hosts); got != 1 {
		t.Fatalf("hosts map len after the first sample = %d, want 1", got)
	}

	current = current.Add(61 * time.Second) // past HostStalenessSeconds
	tracker.RecordSample(Sample{ParticipantKey: "fresh-host", Model: "model-a", Responsive: true})

	if got := len(tracker.hosts); got != 1 {
		t.Fatalf("hosts map len after the stale sweep = %d, want 1 (only fresh-host)", got)
	}
	if _, exists := tracker.hosts[hostKey{participant: "fresh-host", model: "model-a"}]; !exists {
		t.Fatal("fresh-host missing from the hosts map after the sweep")
	}
	if _, exists := tracker.ejections[hostKey{participant: "stale-host", model: "model-a"}]; exists {
		t.Fatal("stale-host's ejection state was not evicted alongside its hostPerf")
	}
}

func TestTrackerConcurrentRecordAndQueryNoRace(t *testing.T) {
	tracker := newTestTracker(testPerf(), fixedNow(testEpoch))

	var wg sync.WaitGroup
	for i := range 50 {
		participant := fmt.Sprintf("participant-%d", i%5)
		wg.Add(4)
		go func() {
			defer wg.Done()
			tracker.RecordSample(Sample{ParticipantKey: participant, Model: "model-a", Responsive: true})
		}()
		go func() {
			defer wg.Done()
			tracker.Inflight(participant)
		}()
		go func() {
			defer wg.Done()
			tracker.Ejected(participant, "model-a")
		}()
		go func() {
			defer wg.Done()
			tracker.Acquire(participant)
			tracker.Inflight(participant)
			tracker.Release(participant)
		}()
	}
	wg.Wait()
}
