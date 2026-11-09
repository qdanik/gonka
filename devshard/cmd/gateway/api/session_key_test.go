package api

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/affinity"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/user"
)

const issuedSessionLength = 26

const echoedChatBody = `{"model":"qwen","prompt_cache_key":"%s","messages":[{"role":"user","content":"hi"}]}`

func sessionHarness(t *testing.T) *harness {
	t.Helper()
	return newHarness(t, func(configuration *config.Config) {
		configuration.Scheduler.AffinityEnabled = true
		configuration.Cache.ChatCacheMaxBytes = 0
	})
}

func sendChat(t *testing.T, live *harness, body, bearer string) (engine.Request, string) {
	t.Helper()
	live.inference.raced.Store(nil)
	recorder := live.request(t, http.MethodPost, "/v1/chat/completions", body, map[string]string{"Authorization": "Bearer " + bearer})
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	raced := live.inference.raced.Load()
	require.NotNil(t, raced, "the race was never handed a request")
	return *raced, recorder.Header().Get(SessionKeyHeader)
}

func hostSessionKey(t *testing.T, raced engine.Request) string {
	t.Helper()
	params, isInference := raced.Params.(user.InferenceParams)
	require.True(t, isInference, "the race was handed %T, not inference params", raced.Params)
	return params.SessionKey
}

// Test flow:
//  1. Send a chat completion without a prompt_cache_key under a client key, with affinity on.
//  2. Expect a session key in the reply's header, the race looked up by the derived key, and the host handed the hex of the session's digest.
//  3. Send the next turn echoing that key as prompt_cache_key.
//  4. Expect no header, the race looked up by the first turn's session digest, and the host handed the same key.
//  5. Send the chat completion with `Bearer EMPTY` on the open model and expect no session key at all.
func TestAMintedSessionKeyComesBackAndItsEchoKeysTheSameSession(t *testing.T) {
	live := sessionHarness(t)

	first, issued := sendChat(t, live, chatBody, clientKey)

	require.NotEmpty(t, issued)
	require.Equal(t, issued, first.IssuedSession)
	require.NotEqual(t, affinity.NoKey, first.AffinityKey)
	require.NotEqual(t, first.AffinityKey, first.SessionDigest)
	require.Equal(t, first.SessionDigest.Hex(), hostSessionKey(t, first))
	require.Len(t, hostSessionKey(t, first), 64)

	echo, header := sendChat(t, live, fmt.Sprintf(echoedChatBody, issued), clientKey)

	require.Empty(t, header)
	require.Equal(t, first.SessionDigest, echo.AffinityKey)
	require.Equal(t, first.SessionDigest, echo.SessionDigest)
	require.Equal(t, affinity.NoSession, echo.IssuedSession)
	require.Equal(t, hostSessionKey(t, first), hostSessionKey(t, echo))

	anonymous, header := sendChat(t, live, chatBody, "EMPTY")
	require.Empty(t, header)
	require.Equal(t, affinity.NoKey, anonymous.SessionDigest)
	require.Empty(t, hostSessionKey(t, anonymous))
}

// Test flow:
//  1. Send the same prompt_cache_key under the client key and under the admin key.
//  2. Expect two different lookup keys and two different keys handed to the host, so neither caller reaches the other's session.
func TestTwoCallersEchoingOneSessionKeyDoNotShareIt(t *testing.T) {
	live := sessionHarness(t)

	client, _ := sendChat(t, live, fmt.Sprintf(echoedChatBody, "shared-session"), clientKey)
	admin, _ := sendChat(t, live, fmt.Sprintf(echoedChatBody, "shared-session"), adminKey)

	require.NotEqual(t, client.AffinityKey, admin.AffinityKey)
	require.NotEqual(t, hostSessionKey(t, client), hostSessionKey(t, admin))
}

// Test flow:
//  1. Wire a real tracker as the issued sessions and send a key the gateway never issued as prompt_cache_key.
//  2. Expect it to key the race as a session of its own, with no header and nothing issued.
//  3. Send a turn without a key, record its winner under its lookup key as the race does, send the same turn again without a key, then a different conversation without one.
//  4. Expect the repeat to get the first turn's session key back, keyed by the same digest, and the other conversation a different key.
func TestAnUnknownKeyIsANewSessionAndARepeatWithoutOneGetsItsSessionBack(t *testing.T) {
	live := sessionHarness(t)
	tracker := affinity.NewTracker(func() affinity.Limits { return affinity.Limits{TTL: time.Minute, MaxEntries: 16} }, func() time.Time { return harnessClock })
	live.server.issuedSessions = tracker

	unknown, header := sendChat(t, live, fmt.Sprintf(echoedChatBody, "never-issued"), clientKey)

	require.Empty(t, header)
	require.NotEqual(t, affinity.NoKey, unknown.AffinityKey)
	require.Equal(t, unknown.AffinityKey, unknown.SessionDigest)
	require.Equal(t, affinity.NoSession, unknown.IssuedSession)

	first, issued := sendChat(t, live, chatBody, clientKey)
	tracker.Record(first.AffinityKey, "host-a", first.IssuedSession)
	repeat, reissued := sendChat(t, live, chatBody, clientKey)
	_, other := sendChat(t, live, `{"model":"qwen","messages":[{"role":"user","content":"another conversation"}]}`, clientKey)

	require.Len(t, issued, issuedSessionLength)
	require.Equal(t, issued, reissued)
	require.Len(t, other, issuedSessionLength)
	require.NotEqual(t, issued, other)
	require.Equal(t, first.SessionDigest, repeat.SessionDigest)
	require.Equal(t, hostSessionKey(t, first), hostSessionKey(t, repeat))
}
