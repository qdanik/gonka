package scheduler

import (
	"sync"
	"testing"
)

func TestAHandedOffNonceIsReportedWithItsRequest(t *testing.T) {
	test := newHarness(t, harnessConfig{})

	queued := test.submit(t, test.clock.Now())
	result := awaitReply(t, queued)

	if result.err != nil {
		t.Fatalf("submit: %v", result.err)
	}
	assigned := test.observer.assignments()
	if len(assigned) != 1 {
		t.Fatalf("reported %+v, want the one nonce handed off", assigned)
	}
	if assigned[0].nonce != result.assignment.Nonce.Nonce() {
		t.Fatalf("reported nonce %d, want the %d that was handed off", assigned[0].nonce, result.assignment.Nonce.Nonce())
	}
	if assigned[0].requestID != queued.profile.RequestID {
		t.Fatalf("reported request %q, want %q", assigned[0].requestID, queued.profile.RequestID)
	}
}

func TestAnAbandonedNonceIsNotReportedAsHandedOff(t *testing.T) {
	var abandonOnce sync.Once
	var lost *waiter
	test := newHarness(t, harnessConfig{afterDecide: func(HostBinding) {
		abandonOnce.Do(func() { lost.abandoned.Store(true) })
	}})

	lost = newWaiter(RequestProfile{Model: modelA, RequestID: "req-lost"}, test.clock.Now())
	if outcome := test.dispatcher.submitWaiter(lost); outcome != submitAccepted {
		t.Fatalf("submitWaiter = %v, want submitAccepted", outcome)
	}
	next := test.submit(t, test.clock.Now())
	awaitReply(t, next)

	for _, assigned := range test.observer.assignments() {
		if assigned.requestID == "req-lost" {
			t.Fatalf("reported %+v for a nonce nobody received", assigned)
		}
	}
}
