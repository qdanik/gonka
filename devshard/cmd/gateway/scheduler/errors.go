package scheduler

import "errors"

var (
	// ErrNoAvailableHost reports that no participant can take the request right now -- excluded, in PoC,
	// throttled, ejected, state-blocked, or the drain's burn budget tripped. Waiting may fix it.
	ErrNoAvailableHost = errors.New("no available host")

	// ErrHostsBusy separates "every host is at capacity right now" from a host that is broken or
	// excluded: the first passes on its own, the second does not, and a client retries them differently.
	ErrHostsBusy = errors.New("all hosts are at capacity")

	// ErrToolsUnsupported reports the one exhaustion waiting cannot fix: every host has already said it
	// does not implement tools, so a retry buys the same refusal at the same price.
	ErrToolsUnsupported = errors.New("no host supports tool calling")

	// ErrAllowlistUnreachable reports that no escrow this gateway serves holds a participant the
	// allowlist admits. It is separated from ErrNoAvailableHost because waiting cannot fix it: the
	// operator narrowed routing to participants none of these escrow groups contains.
	ErrAllowlistUnreachable = errors.New("no escrow holds an allowed participant")

	// ErrNoEscrowCapacity reports that every candidate escrow is at zero spare weight; it deliberately
	// does not name a host.
	ErrNoEscrowCapacity = errors.New("no escrow capacity")

	// ErrEscrowBusy reports that an escrow's dispatch queue is full. It is rate-limit class, like
	// ErrNoEscrowCapacity: the escrow is sound, the caller arrived faster than it can serve.
	ErrEscrowBusy = errors.New("escrow dispatch queue full")

	// ErrDispatcherStopped reports that the escrow's dispatcher shut down before the request was
	// assigned a nonce; the request is retryable.
	ErrDispatcherStopped = errors.New("escrow dispatcher stopped")

	// ErrEscrowGone reports that a request's pinned escrow no longer accepts new inferences.
	ErrEscrowGone = errors.New("pinned escrow gone")
)
