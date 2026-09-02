package user

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A restart empties the in-memory map while the committed record keeps the stamp. Reading the map
// alone named the timeout refused, and the chain rejects a refused timeout against a started record.
func TestTimeoutDeadlineReadsTheConfirmStampFromTheRecordAfterARestart(t *testing.T) {
	session, hosts, _ := setupSession(t, 3, 1_000_000, 0)
	confirmedAt := time.Now().Unix()
	nonce := startedNonce(t, session, hosts, confirmedAt)

	session.mu.Lock()
	delete(session.nonceStates, nonce)
	session.mu.Unlock()

	reason, deadline := session.TimeoutDeadline(nonce, time.Now())

	require.Equal(t, "execution", reason, "the record carries the receipt this session forgot")
	want := time.Unix(confirmedAt, 0).
		Add(time.Duration(session.sm.Config().ExecutionTimeout)*time.Second + TimeoutBuffer)
	require.Equal(t, want, deadline, "the deadline is counted from the record's own stamp")
}

// A record without the stamp stays refused: that is the reason the chain admits for it.
func TestTimeoutDeadlineKeepsARecordWithoutTheStampRefused(t *testing.T) {
	session, _, _ := setupSession(t, 3, 1_000_000, 0)
	nonce := prepareNonce(t, session)
	sendTime := time.Now()

	reason, deadline := session.TimeoutDeadline(nonce, sendTime)

	require.Equal(t, "refused", reason)
	want := sendTime.Add(time.Duration(session.sm.Config().RefusalTimeout)*time.Second + TimeoutBuffer)
	require.Equal(t, want, deadline)
}

// A nonce no record tracks is judged by the send time, as it was before.
func TestTimeoutDeadlineJudgesAnUntrackedNonceByTheSendTime(t *testing.T) {
	session, _, _ := setupSession(t, 3, 1_000_000, 0)
	sendTime := time.Now()

	reason, deadline := session.TimeoutDeadline(9999, sendTime)

	require.Equal(t, "refused", reason)
	want := sendTime.Add(time.Duration(session.sm.Config().RefusalTimeout)*time.Second + TimeoutBuffer)
	require.Equal(t, want, deadline)
}

// The map still answers for a nonce whose confirm-start has not reached the record yet.
func TestTimeoutDeadlineFallsBackToTheSessionMemory(t *testing.T) {
	session, _, _ := setupSession(t, 3, 1_000_000, 0)
	nonce := prepareNonce(t, session)
	confirmedAt := time.Now().Unix()

	session.mu.Lock()
	session.nonceStates[nonce] = &nonceOutcome{confirmedAt: confirmedAt}
	session.mu.Unlock()

	reason, deadline := session.TimeoutDeadline(nonce, time.Now())

	require.Equal(t, "execution", reason)
	want := time.Unix(confirmedAt, 0).
		Add(time.Duration(session.sm.Config().ExecutionTimeout)*time.Second + TimeoutBuffer)
	require.Equal(t, want, deadline)
}
