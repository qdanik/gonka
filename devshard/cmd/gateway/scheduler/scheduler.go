// Package scheduler picks the escrow, host, and nonce that serve a chat-completions request.
package scheduler

import (
	"context"

	"devshard/cmd/gateway/chain"
)

// Scheduler picks a request's escrow/host/nonce and takes the engine's state-divergence reports.
type Scheduler interface {
	Pick(ctx context.Context, profile RequestProfile) (Assignment, error)
	// BlockHost permanently excludes participant from real dispatches on escrowID, fed by the
	// engine on a post-state-root mismatch; never cleared.
	BlockHost(escrowID, participant string)
}

// RequestProfile describes one request, or one escalation attempt within the same request.
type RequestProfile struct {
	Model         string
	Escrow        string // pinned escrow for escalation; "" = pick one
	InputTokens   int
	RequiresTools bool
	ContextHint   uint64
	Exclude       []string // participant keys already raced for this request
	Params        any      // opaque real-dispatch payload forwarded to session.Advance
	AffinityHint  *AffinityHint
}

type Assignment struct {
	Escrow string
	Host   string
	Nonce  Prepared
}

// AffinityHint is the KV-affinity extension point; no ranking logic yet.
type AffinityHint struct{}

// escrowSource is the candidate-escrow registry; api wires it over the live runtime map.
type escrowSource interface {
	// Candidates returns the escrows that could serve model, each with its Session handle, in a
	// stable order, with active/phase already filtered to accepts-new-inferences==true.
	Candidates(model string) []Escrow
}

type Escrow struct {
	ID          string
	Model       string
	Session     session
	ActiveUsers int // in-flight user requests, for the W(e) load score
}

// session is the narrow view of devshard/user.Session the scheduler needs; api wires an adapter.
type session interface {
	// Advance is the one atomic nonce-peek->decide->commit unit: it computes the next candidate
	// binding, calls decide(binding), and commits the nonce (composing the real-or-ghost diff) IFF
	// decide returns serve or burn; on hold it leaves the nonce untouched. Returns decide's
	// Decision and the committed Prepared (zero on hold).
	Advance(decide func(HostBinding) Decision) (Prepared, Decision, error)
	ParticipantKeys() []string // distinct participants (slots deduped) -- the exclusion universe
	GroupSize() int            // len(group); nonce % GroupSize == hostIdx
	LatestNonce() uint64       // for the nonce-cap gate
}

type HostBinding struct {
	Nonce       uint64
	HostIdx     int
	Participant string // participant key for HostIdx (slot-deduped)
}

// Prepared is the opaque committed-nonce payload the engine dispatches; fields land with the api
// adapter over devshard/user.Session.
type Prepared struct{}

// snapshotSource is satisfied by *chain.PhaseObserver.
type snapshotSource interface{ Snapshot() chain.PhaseSnapshot }
