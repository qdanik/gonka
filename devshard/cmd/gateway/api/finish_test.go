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

// A served answer can leave its nonce open, and the escrow owing a vote for it.
func TestARequestFinishedLineSaysTheWinnersNonceNeverClosed(t *testing.T) {
	outcome := benchOutcome()
	stream := newClientStream(newSinkWriter(), "request-1", true, false, filters.LogprobIntent{}, nil)

	fields := requestFinishedFields("request-1", filters.Result{Model: "qwen"}, outcome,
		deliveryServed, stream, time.Second, nil, nil)

	require.Equal(t, false, fieldValue(fields, logkey.NonceFinished),
		"a served request whose nonce never closed must say so where an operator reads it")
}

func TestARequestFinishedLineSaysTheWinnersNonceClosed(t *testing.T) {
	outcome := benchOutcome()
	outcome.Attempts[0].NonceFinished = true
	stream := newClientStream(newSinkWriter(), "request-1", true, false, filters.LogprobIntent{}, nil)

	fields := requestFinishedFields("request-1", filters.Result{Model: "qwen"}, outcome,
		deliveryServed, stream, time.Second, nil, nil)

	require.Equal(t, true, fieldValue(fields, logkey.NonceFinished))
}

func TestARequestFinishedLineOmitsTheNonceNobodyWon(t *testing.T) {
	outcome := engine.RaceOutcome{Model: "qwen", Attempts: []engine.AttemptOutcome{{Participant: "gonka1a"}}}
	stream := newClientStream(newSinkWriter(), "request-1", true, false, filters.LogprobIntent{}, nil)

	fields := requestFinishedFields("request-1", filters.Result{Model: "qwen"}, outcome,
		deliveryFailedBeforeFirstByte, stream, time.Second, errors.New("race"), nil)

	require.Nil(t, fieldValue(fields, logkey.NonceFinished))
}

// fieldValue reads one keyval out of a built log line.
func fieldValue(fields []any, key string) any {
	for index := 0; index+1 < len(fields); index += 2 {
		if name, ok := fields[index].(string); ok && name == key {
			return fields[index+1]
		}
	}
	return nil
}
