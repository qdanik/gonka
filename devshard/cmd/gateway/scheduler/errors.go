package scheduler

import "errors"

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
