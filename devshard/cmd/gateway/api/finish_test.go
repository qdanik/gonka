package api

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/internal/logkey"
)

// Every attempt failing before a first byte still has to say who was asked.
func TestLoggedHostsNamesEveryHostTriedWhenNobodyWon(t *testing.T) {
	first := "gonka1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	second := "gonka1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	outcome := engine.RaceOutcome{Attempts: []engine.AttemptOutcome{
		{Participant: first},
		{Participant: second},
	}}

	got := loggedHosts(outcome)

	require.Contains(t, got, logkey.ShortHost(first))
	require.Contains(t, got, logkey.ShortHost(second))
}

// The reservation is hand-counted, so the widest line the code can build must be measured against it.
func TestARequestFinishedLineFitsWhatItReserves(t *testing.T) {
	outcome := benchOutcome()
	stream := newClientStream(newSinkWriter(), "request-1", true, false, filters.LogprobIntent{}, nil)

	fields := requestFinishedFields("request-1", filters.Result{Model: "qwen", ClientStream: true}, outcome,
		deliveryServed, stream, 3*time.Second, errors.New("race"), errors.New("deliver"))

	require.Len(t, fields, loggedFieldSlots, "the widest finished-request line must fill its reservation exactly")
}
