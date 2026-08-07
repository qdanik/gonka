package scheduler

import (
	"testing"
	"time"

	"devshard/cmd/gateway/perf"
)

const (
	hostA = "participant-a"
	hostB = "participant-b"
)

var soleHost = []string{hostA}

var (
	baseTime  = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	staleHold = 200 * time.Millisecond
)

func queuedWaiter(enqueued time.Time, model string, excluded ...string) *waiter {
	queued := &waiter{
		profile:  RequestProfile{Model: model},
		exclude:  make(map[string]bool, len(excluded)),
		enqueued: enqueued,
	}
	for _, participant := range excluded {
		queued.exclude[participant] = true
	}
	return queued
}

func openAvailability() availability {
	return availability{
		pocRequired:  func(string) bool { return false },
		throttled:    func(string) bool { return false },
		ejected:      func(string) bool { return false },
		capability:   func(string, RequestProfile) (string, bool) { return "", false },
		stateBlocked: func(string) bool { return false },
	}
}

func always(blocked bool) func(string) bool {
	return func(string) bool { return blocked }
}

func capabilityBlocksModels(models ...string) func(string, RequestProfile) (string, bool) {
	blocked := make(map[string]bool, len(models))
	for _, model := range models {
		blocked[model] = true
	}
	return func(_ string, profile RequestProfile) (string, bool) {
		return perf.CapabilityToolsUnsupported, blocked[profile.Model]
	}
}

// servable is the sweep's fast answer and match is the per-nonce decision, and they must be exactly
// as strict as each other: a stricter servable fails a request match would have served, and a laxer
// one keeps a waiter that every drain can only answer by burning a chain-costed nonce.
func TestServableAgreesWithMatchOverEveryFilterCombination(t *testing.T) {
	t.Parallel()
	participants := []string{hostA, hostB}

	for combination := 0; combination < 1<<6; combination++ {
		bit := func(index int) bool { return combination&(1<<index) != 0 }
		availability := availability{
			pocRequired:  func(participant string) bool { return bit(0) && participant == hostA },
			throttled:    func(participant string) bool { return bit(1) && participant == hostB },
			ejected:      func(participant string) bool { return bit(2) && participant == hostA },
			stateBlocked: func(participant string) bool { return bit(3) && participant == hostB },
			capability: func(participant string, _ RequestProfile) (string, bool) {
				return perf.CapabilityToolsUnsupported, bit(4) && participant == hostA
			},
		}
		var excluded []string
		if bit(5) {
			excluded = []string{hostB}
		}
		queued := queuedWaiter(baseTime, "model-a", excluded...)

		canServe, _, _, _ := servable(queued, participants, availability)
		matchWouldServe := false
		for _, participant := range participants {
			binding := HostBinding{Nonce: 1, Participant: participant}
			if _, serving := match(binding, []*waiter{queued}, soleHost, availability, baseTime, staleHold).(serve); serving {
				matchWouldServe = true
			}
		}

		if canServe != matchWouldServe {
			t.Fatalf("combination %06b: servable=%v but a match serve was %v", combination, canServe, matchWouldServe)
		}
	}
}

func decisionKind(decision Decision) string {
	switch decision.(type) {
	case serve:
		return "serve"
	case burn:
		return "burn"
	case hold:
		return "hold"
	case decline:
		return "decline"
	default:
		return ""
	}
}

func wantBurn(t *testing.T, decision Decision, kind GhostKind) {
	t.Helper()
	burned, ok := decision.(burn)
	if !ok {
		t.Fatalf("decision = %T (%v), want burn{%s}", decision, decision, kind.reason())
	}
	if burned.kind != kind {
		t.Fatalf("burn kind = %s, want %s", burned.kind.reason(), kind.reason())
	}
}

func wantServe(t *testing.T, decision Decision, expected *waiter) {
	t.Helper()
	served, ok := decision.(serve)
	if !ok {
		t.Fatalf("decision = %T (%v), want serve", decision, decision)
	}
	if served.waiter != expected {
		t.Fatalf("served waiter model %q, want %q", served.waiter.profile.Model, expected.profile.Model)
	}
}

func TestMatchHostLevelFilters(t *testing.T) {
	t.Parallel()
	binding := HostBinding{Nonce: 7, HostIdx: 1, Participant: hostA}

	tests := []struct {
		name        string
		pocRequired bool
		throttled   bool
		wantServed  bool
		wantKind    GhostKind
	}{
		{name: "poc required burns as poc ghost", pocRequired: true, wantKind: ghostPoC},
		{name: "throttled burns as throttled ghost", throttled: true, wantKind: ghostThrottled},
		{name: "poc wins over throttle", pocRequired: true, throttled: true, wantKind: ghostPoC},
		{name: "neither filter serves the waiter", wantServed: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			queued := queuedWaiter(baseTime, "model-a")
			availability := openAvailability()
			availability.pocRequired = always(testCase.pocRequired)
			availability.throttled = always(testCase.throttled)

			decision := match(binding, []*waiter{queued}, soleHost, availability, baseTime, staleHold)

			if testCase.wantServed {
				wantServe(t, decision, queued)
				return
			}
			wantBurn(t, decision, testCase.wantKind)
		})
	}
}

func TestMatchServesOldestCompatibleWaiter(t *testing.T) {
	t.Parallel()
	oldest := queuedWaiter(baseTime, "model-a")
	middle := queuedWaiter(baseTime.Add(10*time.Millisecond), "model-b")
	newest := queuedWaiter(baseTime.Add(20*time.Millisecond), "model-c")
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}

	decision := match(binding, []*waiter{oldest, middle, newest}, soleHost, openAvailability(), baseTime, staleHold)

	wantServe(t, decision, oldest)
}

func TestMatchConservesNonceForLaterWaiter(t *testing.T) {
	t.Parallel()
	excluding := queuedWaiter(baseTime, "model-a", hostA)
	compatible := queuedWaiter(baseTime.Add(10*time.Millisecond), "model-b")
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}

	decision := match(binding, []*waiter{excluding, compatible}, soleHost, openAvailability(), baseTime, staleHold)

	wantServe(t, decision, compatible)
}

func TestMatchHoldsInsideStaleWindow(t *testing.T) {
	t.Parallel()
	oldest := queuedWaiter(baseTime, "model-a", hostA)
	newer := queuedWaiter(baseTime.Add(50*time.Millisecond), "model-b", hostA)
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}

	tests := []struct {
		name     string
		now      time.Time
		wantHold bool
	}{
		{name: "well inside the window holds", now: baseTime.Add(50 * time.Millisecond), wantHold: true},
		{name: "one nanosecond before expiry holds", now: baseTime.Add(staleHold - time.Nanosecond), wantHold: true},
		{name: "exactly at expiry serves the excluded waiter", now: baseTime.Add(staleHold)},
		{name: "past expiry serves the excluded waiter", now: baseTime.Add(staleHold + time.Millisecond)},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			decision := match(binding, []*waiter{oldest, newer}, soleHost, openAvailability(), testCase.now, staleHold)

			if !testCase.wantHold {
				outcome, served := decision.(serve)
				if !served || !outcome.despiteExclusion {
					t.Fatalf("decision = %T, want the excluded waiter served once the window expired", decision)
				}
				return
			}
			held, ok := decision.(hold)
			if !ok {
				t.Fatalf("decision = %T (%v), want hold", decision, decision)
			}
			if want := baseTime.Add(staleHold); !held.until.Equal(want) {
				t.Fatalf("hold until = %v, want the oldest waiter's deadline %v", held.until, want)
			}
		})
	}
}

func TestMatchBurnKindPastStaleWindow(t *testing.T) {
	t.Parallel()
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}
	expired := baseTime.Add(staleHold)

	tests := []struct {
		name       string
		waiting    []*waiter
		capability func(string, RequestProfile) (string, bool)
		state      func(string) bool
		wantKind   GhostKind
	}{
		{
			name:       "every waiter excludes the host and the host cannot serve it anyway",
			waiting:    []*waiter{queuedWaiter(baseTime, "model-a", hostA)},
			capability: capabilityBlocksModels("model-a"),
			wantKind:   ghostExclude,
		},
		{
			name:       "a scanned waiter is capability blocked",
			waiting:    []*waiter{queuedWaiter(baseTime, "model-a")},
			capability: capabilityBlocksModels("model-a"),
			wantKind:   ghostCapability,
		},
		{
			name:     "a scanned waiter is state blocked",
			waiting:  []*waiter{queuedWaiter(baseTime, "model-a")},
			state:    always(true),
			wantKind: ghostCapability,
		},
		{
			name:     "an excluded waiter the host cannot serve either way",
			waiting:  []*waiter{queuedWaiter(baseTime, "model-a", hostA)},
			state:    always(true),
			wantKind: ghostExclude,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			availability := openAvailability()
			if testCase.capability != nil {
				availability.capability = testCase.capability
			}
			if testCase.state != nil {
				availability.stateBlocked = testCase.state
			}

			decision := match(binding, testCase.waiting, soleHost, availability, expired, staleHold)

			wantBurn(t, decision, testCase.wantKind)
		})
	}
}

func TestMatchKeysExclusionByParticipantNotSlot(t *testing.T) {
	t.Parallel()
	excluding := queuedWaiter(baseTime, "model-a", hostA)
	siblingSlots := []HostBinding{
		{Nonce: 4, HostIdx: 0, Participant: hostA},
		{Nonce: 9, HostIdx: 5, Participant: hostA},
	}

	for _, binding := range siblingSlots {
		inside := match(binding, []*waiter{excluding}, soleHost, openAvailability(), baseTime, staleHold)
		if _, held := inside.(hold); !held {
			t.Fatalf("slot %d gave %T inside the stale window, want the nonce held", binding.HostIdx, inside)
		}

		past := match(binding, []*waiter{excluding}, soleHost, openAvailability(), baseTime.Add(staleHold), staleHold)
		outcome, served := past.(serve)
		if !served || !outcome.despiteExclusion {
			t.Fatalf("slot %d gave %T past the stale window, want the excluded waiter served", binding.HostIdx, past)
		}
	}
}

// A hold parks a nonce for a waiter to come back to, so an empty queue must never produce one. The
// host filters still burn: they are facts about the host, decided before the queue is consulted at all.
func TestMatchEmptyQueueNeverHolds(t *testing.T) {
	t.Parallel()
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}

	tests := []struct {
		name         string
		availability availability
		wantBurnKind GhostKind
		wantDeclined bool
	}{
		{name: "no filters", availability: openAvailability(), wantDeclined: true},
		{name: "poc required", availability: func() availability {
			availability := openAvailability()
			availability.pocRequired = always(true)
			return availability
		}(), wantBurnKind: ghostPoC},
		{name: "state blocked", availability: func() availability {
			availability := openAvailability()
			availability.stateBlocked = always(true)
			return availability
		}(), wantDeclined: true},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			decision := match(binding, nil, soleHost, testCase.availability, baseTime, staleHold)

			if testCase.wantDeclined {
				if _, declined := decision.(decline); !declined {
					t.Fatalf("decision = %T (%v), want the nonce declined", decision, decision)
				}
				return
			}
			wantBurn(t, decision, testCase.wantBurnKind)
		})
	}
}

func TestMatchDoesNotMutateItsInputs(t *testing.T) {
	t.Parallel()
	first := queuedWaiter(baseTime, "model-a", hostA)
	second := queuedWaiter(baseTime.Add(time.Millisecond), "model-b")
	waiting := []*waiter{first, second}

	match(HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}, waiting, soleHost, openAvailability(), baseTime, staleHold)

	if len(waiting) != 2 || waiting[0] != first || waiting[1] != second {
		t.Fatalf("waiting queue was reordered or resized: %v", waiting)
	}
	if !first.enqueued.Equal(baseTime) || !first.exclude[hostA] || len(first.exclude) != 1 {
		t.Fatalf("first waiter mutated: enqueued=%v exclude=%v", first.enqueued, first.exclude)
	}
	if len(second.exclude) != 0 || second.profile.Model != "model-b" {
		t.Fatalf("second waiter mutated: exclude=%v profile=%+v", second.exclude, second.profile)
	}
}

// TestMatchIsTotal walks every predicate combination against every queue/clock shape and asserts a
// single non-nil Decision plus the invariant each kind claims -- the exhaustiveness that makes every
// committed nonce accountable to exactly one party.
func TestMatchIsTotal(t *testing.T) {
	t.Parallel()
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}
	blockedModel := "model-blocked"

	queues := []struct {
		name    string
		waiting []*waiter
	}{
		{name: "empty queue"},
		{name: "compatible waiter", waiting: []*waiter{queuedWaiter(baseTime, "model-a")}},
		{name: "excluding waiter", waiting: []*waiter{queuedWaiter(baseTime, "model-a", hostA)}},
		{name: "capability blocked waiter", waiting: []*waiter{queuedWaiter(baseTime, blockedModel)}},
		{name: "blocked then compatible", waiting: []*waiter{
			queuedWaiter(baseTime, blockedModel),
			queuedWaiter(baseTime.Add(time.Millisecond), "model-a"),
		}},
	}
	clocks := []struct {
		name string
		now  time.Time
	}{
		{name: "inside stale window", now: baseTime.Add(staleHold / 2)},
		{name: "past stale window", now: baseTime.Add(staleHold)},
	}

	for predicates := 0; predicates < 32; predicates++ {
		pocRequired := predicates&1 != 0
		throttled := predicates&2 != 0
		capabilityBlocked := predicates&4 != 0
		stateBlocked := predicates&8 != 0
		ejected := predicates&16 != 0

		availability := availability{
			pocRequired:  always(pocRequired),
			throttled:    always(throttled),
			ejected:      always(ejected),
			stateBlocked: always(stateBlocked),
			capability: func(_ string, profile RequestProfile) (string, bool) {
				return perf.CapabilityToolsUnsupported, capabilityBlocked || profile.Model == blockedModel
			},
		}

		for _, queue := range queues {
			for _, clock := range clocks {
				name := queue.name + "/" + clock.name +
					"/poc=" + boolLabel(pocRequired) + ",throttled=" + boolLabel(throttled) +
					",capability=" + boolLabel(capabilityBlocked) + ",state=" + boolLabel(stateBlocked) +
					",ejected=" + boolLabel(ejected)
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					decision := match(binding, queue.waiting, soleHost, availability, clock.now, staleHold)

					if decision == nil {
						t.Fatal("match returned a nil Decision")
					}
					if decisionKind(decision) == "" {
						t.Fatalf("match returned an unknown Decision type %T", decision)
					}
					switch outcome := decision.(type) {
					case serve:
						if pocRequired || throttled || stateBlocked || capabilityBlocked {
							t.Fatalf("served through an active filter: %+v", availability)
						}
						if outcome.waiter.profile.Model == blockedModel {
							t.Fatalf("served an incompatible waiter %+v", outcome.waiter.profile)
						}
						if outcome.waiter.exclude[hostA] && !outcome.despiteExclusion {
							t.Fatalf("served an excluded waiter without saying so %+v", outcome.waiter.profile)
						}
					case hold:
						if pocRequired || throttled {
							t.Fatal("held a nonce past a host-level filter")
						}
						if len(queue.waiting) == 0 {
							t.Fatal("held with no waiter to hold for")
						}
						if want := queue.waiting[0].enqueued.Add(staleHold); !outcome.until.Equal(want) {
							t.Fatalf("hold until = %v, want %v", outcome.until, want)
						}
						if !clock.now.Before(outcome.until) {
							t.Fatalf("held past the deadline: now=%v until=%v", clock.now, outcome.until)
						}
					case burn:
						switch {
						case pocRequired && outcome.kind != ghostPoC:
							t.Fatalf("burn kind = %s, want %s", outcome.kind.reason(), ghostPoC.reason())
						case !pocRequired && throttled && outcome.kind != ghostThrottled:
							t.Fatalf("burn kind = %s, want %s", outcome.kind.reason(), ghostThrottled.reason())
						}
					}
				})
			}
		}
	}
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func TestMatchSkipsAbandonedWaiters(t *testing.T) {
	t.Parallel()
	binding := HostBinding{Nonce: 4, HostIdx: 0, Participant: hostA}

	t.Run("an abandoned waiter is never served", func(t *testing.T) {
		t.Parallel()
		abandoned := queuedWaiter(baseTime, "model-a")
		abandoned.abandoned.Store(true)
		live := queuedWaiter(baseTime.Add(time.Millisecond), "model-b")

		decision := match(binding, []*waiter{abandoned, live}, soleHost, openAvailability(), baseTime, staleHold)

		wantServe(t, decision, live)
	})

	t.Run("the hold deadline comes from the oldest live waiter", func(t *testing.T) {
		t.Parallel()
		abandoned := queuedWaiter(baseTime, "model-a", hostA)
		abandoned.abandoned.Store(true)
		live := queuedWaiter(baseTime.Add(150*time.Millisecond), "model-b", hostA)
		past := baseTime.Add(250 * time.Millisecond)

		decision := match(binding, []*waiter{abandoned, live}, soleHost, openAvailability(), past, staleHold)

		held, ok := decision.(hold)
		if !ok {
			t.Fatalf("decision = %T (%v), want a hold on the live waiter's deadline", decision, decision)
		}
		if want := live.enqueued.Add(staleHold); !held.until.Equal(want) {
			t.Fatalf("hold until = %v, want the oldest live waiter's deadline %v", held.until, want)
		}
	})

	// A nonce costs the escrow whether or not anyone is served by it, so a queue with nobody left in
	// it must give the nonce back rather than spend one recording that nobody wanted it.
	t.Run("an entirely abandoned queue neither holds nor burns", func(t *testing.T) {
		t.Parallel()
		abandoned := queuedWaiter(baseTime, "model-a", hostA)
		abandoned.abandoned.Store(true)

		decision := match(binding, []*waiter{abandoned}, soleHost, openAvailability(), baseTime, staleHold)

		if _, declined := decision.(decline); !declined {
			t.Fatalf("decision = %T (%v), want the nonce declined", decision, decision)
		}
	})
}
