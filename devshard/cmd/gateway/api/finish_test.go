package api

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/engine"
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
