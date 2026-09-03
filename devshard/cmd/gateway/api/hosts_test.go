package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
)

func TestAdminHostsReportsRoutingStateAndWindow(t *testing.T) {
	live := newHarness(t)
	live.hosts.states = []perf.HostState{{
		Participant: "gonka1aaaa", Model: "qwen", Ejected: true, Inflight: 2,
		TimePerOutputToken: 25 * time.Millisecond,
	}}
	live.hosts.degraded = map[string]bool{"gonka1aaaa|qwen": true}
	live.hosts.windows = []limits.HostWindow{{
		Participant: "gonka1aaaa", Model: "qwen", Window: 8, Inflight: 2,
		Cutoff: limits.CutoffOpen, BackoffCount: 3, Available: false,
	}}

	recorder := live.request(t, http.MethodGet, "/v1/admin/hosts", "", adminHeaders())

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var answer struct {
		Hosts []map[string]any `json:"hosts"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
	require.Len(t, answer.Hosts, 1)
	host := answer.Hosts[0]
	require.Equal(t, "gonka1aaaa", host["participant_key"])
	require.Equal(t, "qwen", host["model"])
	require.Equal(t, true, host["ejected"])
	require.Equal(t, true, host["degraded"])
	require.Equal(t, float64(8), host["window"])
	require.Equal(t, string(limits.CutoffOpen), host["cutoff"])
	require.Equal(t, float64(3), host["backoff_count"])
	require.Equal(t, false, host["available"])
}

func TestAdminHostsJoinsTheTwoSourcesByPair(t *testing.T) {
	live := newHarness(t)
	live.hosts.states = []perf.HostState{{Participant: "gonka1aaaa", Model: "qwen"}}
	live.hosts.windows = []limits.HostWindow{{Participant: "gonka1bbbb", Model: "qwen", Window: 4}}

	recorder := live.request(t, http.MethodGet, "/v1/admin/hosts", "", adminHeaders())

	var answer struct {
		Hosts []map[string]any `json:"hosts"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &answer))
	require.Len(t, answer.Hosts, 2, "a pair known to either source is a host the operator can ask about")
}

func TestAdminHostsNeedsTheAdminKey(t *testing.T) {
	live := newHarness(t)

	recorder := live.request(t, http.MethodGet, "/v1/admin/hosts", "", nil)

	require.Equal(t, http.StatusUnauthorized, recorder.Code)
}

func TestAdminHostsWithoutSourcesAnswersEmpty(t *testing.T) {
	live := newHarness(t)
	live.hosts.states = nil
	live.hosts.windows = nil

	recorder := live.request(t, http.MethodGet, "/v1/admin/hosts", "", adminHeaders())

	require.Equal(t, http.StatusOK, recorder.Code)
	require.JSONEq(t, `{"hosts":[]}`, recorder.Body.String())
}
