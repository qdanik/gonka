package api

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"devshard/cmd/gateway/registry"
	"devshard/host"
	"devshard/types"
	"devshard/user"
)

// fixedEscrows publishes one escrow. retired takes it out of the acquirable set while leaving it
// settling, which is the state a rotation between the end of a race and its votes leaves behind.
type fixedEscrows struct {
	escrowID string
	session  registry.EscrowSession
	retired  bool

	holds    atomic.Int64
	releases atomic.Int64
}

func (f *fixedEscrows) Acquire(escrowID string) (registry.EscrowSession, func(), bool) {
	if f.retired {
		return nil, nil, false
	}
	session, held := f.SettlementSession(escrowID)
	if !held {
		return nil, nil, false
	}
	f.holds.Add(1)
	var once sync.Once
	return session, func() { once.Do(func() { f.releases.Add(1) }) }, true
}

func (f *fixedEscrows) SettlementSession(escrowID string) (registry.EscrowSession, bool) {
	if escrowID != f.escrowID || f.session == nil {
		return nil, false
	}
	return f.session, true
}

func TestTimeoutPayloadIsRebuiltFromTheCommittedRecord(t *testing.T) {
	t.Parallel()
	committed := types.EscrowState{Inferences: map[uint64]*types.InferenceRecord{
		4: {Model: liveModel, InputLength: 512, MaxTokens: 64, StartedAt: 1_700_000_000},
	}}
	cases := []struct {
		name  string
		state types.EscrowState
		nonce uint64
		want  *host.InferencePayload
	}{
		{
			name:  "a committed nonce carries the record's own numbers",
			state: committed,
			nonce: 4,
			want: &host.InferencePayload{
				Prompt:      livePrompt,
				Model:       liveModel,
				InputLength: 512,
				MaxTokens:   64,
				StartedAt:   1_700_000_000,
			},
		},
		{
			name:  "a nonce the escrow never committed has no payload to verify",
			state: committed,
			nonce: 5,
		},
		{
			name:  "an empty escrow has none either",
			state: types.EscrowState{},
			nonce: 4,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			payload := timeoutPayload(testCase.state, testCase.nonce, livePrompt)

			if !reflect.DeepEqual(payload, testCase.want) {
				t.Errorf("timeoutPayload = %+v, want %+v", payload, testCase.want)
			}
		})
	}
}

func TestPosterRefusesAnEscrowThisProcessNoLongerHolds(t *testing.T) {
	t.Parallel()
	session, machine := newLiveSession(t)
	sessions := NewSessions(&fixedEscrows{escrowID: liveEscrowID, session: registry.NewSessionHandle(session, machine)})

	if _, ok := sessions.Poster("8", liveParams()); ok {
		t.Error("Poster(8) resolved a poster for an escrow that is gone, want not found")
	}
	if _, ok := sessions.Poster(liveEscrowID, liveParams()); !ok {
		t.Errorf("Poster(%q) = not found, want the live escrow's poster", liveEscrowID)
	}
}

// A retired escrow keeps every nonce its last race committed, and those nonces have no other settlement
// path: routing must lose it, the poster must not.
func TestPosterSettlesARetiredEscrowThatRoutingHasLost(t *testing.T) {
	t.Parallel()
	session, machine := newLiveSession(t)
	retired := &fixedEscrows{
		escrowID: liveEscrowID,
		session:  registry.NewSessionHandle(session, machine),
		retired:  true,
	}
	sessions := NewSessions(retired)

	if _, _, routable := sessions.Target(liveEscrowID); routable {
		t.Error("Target resolved a retired escrow, want not found: it must take no further request")
	}
	if _, settling := sessions.Poster(liveEscrowID, liveParams()); !settling {
		t.Errorf("Poster(%q) on a retired escrow = not found: every vote its last race owed is dropped", liveEscrowID)
	}
}

// A params value the dispatch adapter could not commit never committed a nonce either, so there is
// nothing for a poster to settle.
func TestPosterRefusesParamsNoNonceCouldHaveCommitted(t *testing.T) {
	t.Parallel()
	session, machine := newLiveSession(t)
	sessions := NewSessions(&fixedEscrows{escrowID: liveEscrowID, session: registry.NewSessionHandle(session, machine)})

	if _, ok := sessions.Poster(liveEscrowID, livePrompt); ok {
		t.Error("Poster resolved a poster for a raw request body, want not found")
	}
}

func TestSettleTimeoutReportsACancelledWaitAsNoVote(t *testing.T) {
	t.Parallel()
	session, machine := newLiveSession(t)
	prepared := prepareNonce(t, session)
	sessions := NewSessions(&fixedEscrows{escrowID: liveEscrowID, session: registry.NewSessionHandle(session, machine)})
	poster, ok := sessions.Poster(liveEscrowID, liveParams())
	if !ok {
		t.Fatalf("Poster(%q) = not found, want the live escrow's poster", liveEscrowID)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	vote, err := poster.SettleTimeout(cancelled, prepared.Nonce(), time.Now())

	if !errors.Is(err, context.Canceled) {
		t.Errorf("SettleTimeout error = %v, want it to match context.Canceled", err)
	}
	if vote.Kind != "" {
		t.Errorf("vote = %q, want empty: the protocol deadline was never reached", vote)
	}
}

// An insufficient tally is a timeout the group never applied, and it reaches the poster looking like a
// posted vote: same populated result, same non-nil error. Only the handler's sentinel separates them.
func TestSettleTimeoutReportsAnInsufficientVoteTallyAsAFailure(t *testing.T) {
	t.Parallel()
	session, machine := newLiveSession(t)
	prepared := prepareNonce(t, session)
	sessions := NewSessions(&fixedEscrows{escrowID: liveEscrowID, session: registry.NewSessionHandle(session, machine)})
	poster, ok := sessions.Poster(liveEscrowID, liveParams())
	if !ok {
		t.Fatalf("Poster(%q) = not found, want the live escrow's poster", liveEscrowID)
	}
	// No host client is a verifier, so the tally is empty and cannot clear the threshold.
	elapsedDeadline := time.Now().Add(-24 * time.Hour)

	vote, err := poster.SettleTimeout(context.Background(), prepared.Nonce(), elapsedDeadline)

	if !errors.Is(err, user.ErrTimeoutNotApplied) {
		t.Errorf("SettleTimeout error = %v, want it to match user.ErrTimeoutNotApplied", err)
	}
	if got, want := vote.Kind, "refused"; got != want {
		t.Errorf("vote kind = %q, want %q", got, want)
	}
}
