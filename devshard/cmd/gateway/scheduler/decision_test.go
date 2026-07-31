package scheduler

import "testing"

// The assignment carries a committed nonce and an admitted concurrency slot, so a cancellation landing
// in the same instant as the handoff must leave it owned by exactly one side.
func TestWaiterHandsOffToExactlyOneSide(t *testing.T) {
	t.Run("a caller that leaves after the handoff takes the assignment with it", func(t *testing.T) {
		queued := newWaiter(RequestProfile{Model: modelA}, baseTime)
		assignment := Assignment{Escrow: escrowA, Host: hostA}

		if !queued.deliver(pickResult{assignment: assignment}) {
			t.Fatal("deliver to a live waiter = false, want true")
		}
		delivered, wasDelivered := queued.abandon()

		if !wasDelivered || delivered.assignment.Escrow != assignment.Escrow || delivered.assignment.Host != assignment.Host {
			t.Fatalf("abandon = %+v/%v, want the assignment already handed over", delivered, wasDelivered)
		}
	})

	t.Run("a caller that leaves first refuses the handoff", func(t *testing.T) {
		queued := newWaiter(RequestProfile{Model: modelA}, baseTime)

		if _, wasDelivered := queued.abandon(); wasDelivered {
			t.Fatal("abandon on an unanswered waiter reported a reply")
		}

		if queued.deliver(pickResult{assignment: Assignment{Escrow: escrowA, Host: hostA}}) {
			t.Fatal("deliver to an abandoned waiter = true, want false")
		}
	})
}
