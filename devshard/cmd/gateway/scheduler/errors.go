package scheduler

import (
	"errors"
	"fmt"
)

// What each of these means, and whether waiting clears it, is in README, "Failure vocabulary".
var (
	ErrNoAvailableHost = errors.New("no available host")

	// ErrHostsBusy is kept apart because a busy pool passes on its own and a broken host does not.
	ErrHostsBusy = errors.New("all hosts are at capacity")

	// ErrAllowlistUnreachable is kept apart from ErrNoAvailableHost because waiting cannot fix it.
	ErrAllowlistUnreachable = errors.New("no escrow holds an allowed participant")

	// ErrNoEscrowCapacity deliberately does not name a host.
	ErrNoEscrowCapacity = errors.New("no escrow capacity")

	ErrEscrowBusy = errors.New("escrow dispatch queue full")

	ErrDispatcherStopped = errors.New("escrow dispatcher stopped")

	ErrEscrowGone = errors.New("pinned escrow gone")
)

// EscrowsOutOfFundsError counts the escrows that refused to pay for one request before the fleet ran out. See routing.md, "Past every escrow that cannot pay".
type EscrowsOutOfFundsError struct {
	Refused int
	wrapped error
}

func (e *EscrowsOutOfFundsError) Error() string {
	return fmt.Sprintf("no escrow can fund this request (%d refused): %v", e.Refused, e.wrapped)
}

func (e *EscrowsOutOfFundsError) Unwrap() error { return e.wrapped }
