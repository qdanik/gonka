package user

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/host"
	"devshard/internal/statetest"
	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/types"
)

// sessionHoldingDiffs is the smallest session catchUpDiffsForVerifier reads: the log, the cursors and the group.
func sessionHoldingDiffs(upToNonce uint64, cursors map[int]uint64) *Session {
	diffs := make([]types.Diff, 0, upToNonce)
	for nonce := uint64(1); nonce <= upToNonce; nonce++ {
		diffs = append(diffs, types.Diff{Nonce: nonce})
	}
	return &Session{
		diffs:         diffs,
		hostSyncNonce: cursors,
		group:         make([]types.SlotAssignment, len(cursors)),
	}
}

// A voter only has to be caught up to the record it is judging, never to the whole log.
func TestAVoteCarriesOnlyWhatEachVerifierIsMissing(t *testing.T) {
	session := sessionHoldingDiffs(100, map[int]uint64{0: 98, 1: 40, 2: 100})

	twoBehind := session.catchUpDiffsForVerifier(0)
	sixtyBehind := session.catchUpDiffsForVerifier(1)
	caughtUp := session.catchUpDiffsForVerifier(2)

	require.Len(t, twoBehind, 2, "a verifier two diffs behind gets two")
	require.Equal(t, uint64(99), twoBehind[0].Nonce)
	require.Len(t, sixtyBehind, 60, "a verifier sixty diffs behind gets sixty")
	require.Equal(t, uint64(41), sixtyBehind[0].Nonce)
	require.Empty(t, caughtUp, "a verifier that has the whole log gets nothing")
}

// A verifier the gateway has never sent to has no cursor, so it is owed the history the session holds.
func TestAVerifierWithNoCursorIsOwedTheWholeLog(t *testing.T) {
	session := sessionHoldingDiffs(5, map[int]uint64{0: 0})

	catchUp := session.catchUpDiffsForVerifier(0)

	require.Len(t, catchUp, 5)
	require.Equal(t, uint64(1), catchUp[0].Nonce)
}

// A vote is turned away for being early, never for being late. See cmd/gateway/docs/race.md, "The timeout-vote queue".
func TestARefusedVoteIsTurnedAwayOnlyForBeingEarly(t *testing.T) {
	userKey := testutil.MustGenerateKey(t)
	hosts := []*signing.Secp256k1Signer{testutil.MustGenerateKey(t), testutil.MustGenerateKey(t)}
	group := testutil.MakeGroup(hosts)
	config := types.SessionConfig{RefusalTimeout: 60, ExecutionTimeout: 1200, TokenPrice: 1, VoteThreshold: 1}
	machine := statetest.MustStateMachine(t, "escrow-1", config, group, 100000, userKey.Address(),
		signing.NewSecp256k1Verifier())
	session := &Session{sm: machine}
	sentAt := time.Now().Unix()

	_, _, tooEarly := session.refusalDeadlineUnreachable(
		types.TimeoutReason_TIMEOUT_REASON_REFUSED, &host.InferencePayload{StartedAt: sentAt})
	_, _, longPast := session.refusalDeadlineUnreachable(
		types.TimeoutReason_TIMEOUT_REASON_REFUSED, &host.InferencePayload{StartedAt: sentAt - 86_400})

	require.True(t, tooEarly, "a vote before its refusal deadline must still be turned away")
	require.False(t, longPast, "a vote a day past its refusal deadline must still be allowed")
}
