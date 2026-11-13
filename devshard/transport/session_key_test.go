package transport

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/host"
)

// Test flow:
//  1. Encode a host request carrying a session key and one carrying none, through JSON and back.
//  2. Expect the key to survive the wire, and a request with none to carry no session_key field.
func TestTheSessionKeySurvivesTheWire(t *testing.T) {
	for _, sessionKey := range []string{"session-digest", ""} {
		encoded, err := HostRequestToJSON(host.HostRequest{Nonce: 7, SessionKey: sessionKey})
		require.NoError(t, err)
		body, err := json.Marshal(encoded)
		require.NoError(t, err)
		if sessionKey == "" {
			require.NotContains(t, string(body), "session_key")
		}

		var received InferenceRequest
		require.NoError(t, json.Unmarshal(body, &received))
		restored, err := HostRequestFromJSON(received)
		require.NoError(t, err)
		require.Equal(t, sessionKey, restored.SessionKey)
	}
}
