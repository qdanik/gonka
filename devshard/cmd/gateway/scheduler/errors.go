package scheduler

import "errors"

// ErrNoAvailableHost reports that a request's exclude set already covers every available participant.
var ErrNoAvailableHost = errors.New("no available host")

// ErrAllHostsExcluded reports that the exclude set covers every distinct participant in the group.
var ErrAllHostsExcluded = errors.New("all hosts excluded")

// ErrNoEscrowCapacity reports that every candidate escrow is at zero spare weight; it deliberately
// does not name a host.
var ErrNoEscrowCapacity = errors.New("no escrow capacity")
