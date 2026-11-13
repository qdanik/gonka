package main

import (
	"testing"
	"time"

	"devshard/testenv/sessionload"

	"github.com/stretchr/testify/require"
)

// Test flow:
// 1. Build executions of two conversations: one whose second turn stays on host 0, one whose second turn moves from host 1 to host 2.
// 2. Add a second execution of the first conversation's first turn on host 3 and a first turn with no follow-up.
// 3. landing counts the two follow-ups, one on the previous host, and one duplicate; the duplicate does not move the turn's host.
func TestLanding_CountsFollowUpsOnThePreviousHost(t *testing.T) {
	all := []execution{
		{host: 0, msgs: 2, at: 1, fp: "a"},
		{host: 0, msgs: 4, at: 5, fp: "a"},
		{host: 1, msgs: 2, at: 2, fp: "b"},
		{host: 2, msgs: 4, at: 6, fp: "b"},
		{host: 3, msgs: 2, at: 3, fp: "a"},
		{host: 4, msgs: 2, at: 4, fp: "c"},
	}

	followUps, sameHost, duplicates := landing(all)

	require.Equal(t, []int{2, 1, 1}, []int{followUps, sameHost, duplicates})
}

// Test flow:
// 1. Parse a scrape with labelled and bare samples and a comment line.
// 2. sum filters by a label value and byLabel groups by one label.
func TestParseMetrics_SumsAndGroupsByLabel(t *testing.T) {
	set := parseMetrics(`# HELP x
devshard_gateway_requests_total{model="m",outcome="success",reason=""} 7
devshard_gateway_requests_total{model="m",outcome="failed",reason="x"} 2
devshard_gateway_affinity_exceptions_total{model="m",reason="max_burns"} 4
devshard_gateway_affinity_hits_total 9
`)

	require.Equal(t, 7.0, set.sum("devshard_gateway_requests_total", "outcome", "success"))
	require.Equal(t, 9.0, set.sum("devshard_gateway_affinity_hits_total", "", ""))
	require.Equal(t, map[string]float64{"max_burns": 4}, set.byLabel("devshard_gateway_affinity_exceptions_total", "reason"))
}

// Test flow:
// 1. Strip the version prefix from a session route, a bare route and a path without a prefix.
// 2. Only a leading /devshard/<version> goes.
func TestStripVersionPrefix(t *testing.T) {
	require.Equal(t, "/sessions/1/rpc/x", stripVersionPrefix("/devshard/dev/sessions/1/rpc/x"))
	require.Equal(t, "/", stripVersionPrefix("/devshard/dev"))
	require.Equal(t, "/v1/chat/completions", stripVersionPrefix("/v1/chat/completions"))
}

// Test flow:
// 1. Summarize 20 served turns with first-token times of 1 to 20 ms, one failed turn and one turn cancelled at the run end.
// 2. The percentiles are nearest-rank over the served turns, the failure counts as an error and the cancelled turn is left out.
func TestSummarizeTurns_NearestRankAndCancelledTurnsLeftOut(t *testing.T) {
	var turns []sessionload.Turn
	for i := 1; i <= 20; i++ {
		turns = append(turns, sessionload.Turn{TTFT: time.Duration(i) * time.Millisecond, Latency: time.Duration(i) * time.Millisecond})
	}
	turns = append(turns, sessionload.Turn{Error: "status 503"}, sessionload.Turn{Error: sessionload.CancelledAtRunEnd})
	r := &result{}

	summarizeTurns(r, turns)

	require.Equal(t, []float64{10, 19}, []float64{r.TTFTP50, r.TTFTP95})
	require.Equal(t, []int{21, 1}, []int{r.Turns, r.Errors})
}
