package engine

import (
	"testing"
	"time"

	"devshard/cmd/gateway/scheduler"
)

// tracedCoordinator is a paused coordinator on escrow-1, for the Trace hooks below.
func tracedCoordinator(attempts ...*liveAttempt) *raceCoordinator {
	coordinator := pausedCoordinator(newRaceFixture(settledPolicy(), 4), 4, attempts...)
	coordinator.escrowID = "escrow-1"
	return coordinator
}

// awaitTracedAttemptDone waits for the attempt goroutine a launch started, so no test leaves it running.
func awaitTracedAttemptDone(t *testing.T, events <-chan AttemptEvent) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Kind == AttemptDone {
				return
			}
		case <-deadline:
			t.Fatal("the launched attempt never reported that it finished")
		}
	}
}

// TraceNonceCommitted launches one attempt on a host that answers nothing.
func TraceNonceCommitted(t *testing.T) {
	t.Helper()
	coordinator := tracedCoordinator()
	coordinator.launch(scheduler.Assignment{
		Escrow: "escrow-1",
		Host:   "host-2",
		Nonce:  fakePrepared{nonce: 12, hostIdx: 2},
	}, RolePrimary, StartPrimary)
	awaitTracedAttemptDone(t, coordinator.events)
}

// TraceEscalationUnfilled reports an escalation the scheduler could not fill.
func TraceEscalationUnfilled(t *testing.T) {
	t.Helper()
	coordinator := tracedCoordinator()
	coordinator.pickReason = "receipt_timeout"
	coordinator.reportUnfilledPick(scheduler.ErrNoAvailableHost)
}

// TraceAttemptCrowned crowns the first attempt to claim the client.
func TraceAttemptCrowned(t *testing.T) {
	t.Helper()
	attempt := &liveAttempt{nonce: 13, participant: "host-3", cancel: func() {}}
	coordinator := tracedCoordinator(attempt)
	coordinator.crownWinner(attempt, crownFirstClaim)
}

// TraceAttemptFinished completes a losing attempt that carries every delivery field a finish line reports.
func TraceAttemptFinished(t *testing.T) {
	t.Helper()
	outcome := widestFinishedOutcome()
	outcome.Terminal = TerminalLost
	attempt := &liveAttempt{nonce: 77, participant: "host-3", cancel: func() {}}
	coordinator := tracedCoordinator(attempt)
	coordinator.complete(attempt, AttemptEvent{Kind: AttemptDone, Nonce: 77, At: testEpoch, Outcome: &outcome})
}

// TraceAttemptFinishedWithNoOutcome completes an attempt whose goroutine reported nothing.
func TraceAttemptFinishedWithNoOutcome(t *testing.T) {
	t.Helper()
	attempt := &liveAttempt{nonce: 78, participant: "host-4", cancel: func() {}}
	coordinator := tracedCoordinator(attempt)
	coordinator.complete(attempt, AttemptEvent{Kind: AttemptDone, Nonce: 78, At: testEpoch})
}

// TraceHostDiverged diverges one host twice: the first spends its replay, the second blocks it.
func TraceHostDiverged(t *testing.T) {
	t.Helper()
	attempt := &liveAttempt{nonce: 79, hostIdx: 1, participant: "host-5", cancel: func() {}}
	coordinator := tracedCoordinator(attempt)
	coordinator.stateDiverged(attempt)
	coordinator.stateDiverged(attempt)
}

// TraceNonceStranded strands a committed nonce the race can no longer spend.
func TraceNonceStranded(t *testing.T) {
	t.Helper()
	coordinator := tracedCoordinator()
	coordinator.strand(scheduler.Assignment{
		Escrow: "escrow-1",
		Host:   "host-6",
		Nonce:  fakePrepared{nonce: 91, hostIdx: 2},
	}, RoleSpeculative)
}
