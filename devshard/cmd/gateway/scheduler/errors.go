package scheduler

import "errors"

// ErrNoAvailableHost reports that a request's exclude set already covers every available participant.
var ErrNoAvailableHost = errors.New("no available host")

// ErrNoEscrowCapacity reports that every candidate escrow is at zero spare weight; it deliberately
// does not name a host.
var ErrNoEscrowCapacity = errors.New("no escrow capacity")

// ErrEscrowBusy reports that an escrow's dispatch queue is full. It is rate-limit class, like
// ErrNoEscrowCapacity: the escrow is sound, the caller arrived faster than it can serve.
var ErrEscrowBusy = errors.New("escrow dispatch queue full")

// ErrDispatcherStopped reports that the escrow's dispatcher shut down before the request was
// assigned a nonce; the request is retryable.
var ErrDispatcherStopped = errors.New("escrow dispatcher stopped")

// ErrEscrowGone reports that a request's pinned escrow no longer accepts new inferences.
var ErrEscrowGone = errors.New("pinned escrow gone")
