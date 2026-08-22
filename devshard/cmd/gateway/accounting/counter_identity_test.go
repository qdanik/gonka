package accounting

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// Two nonces that differ only in a flag are two counters. A flag the table does not carry collapses
// them onto one primary key, and the whole snapshot write fails on the second row — every fact the
// gateway gathered since the last snapshot goes with it.
func TestCountersThatDifferOnlyInAFlagBothSurviveARestart(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		attempt Attempt
	}{
		{"slow decode", Attempt{Nonce: 8, Sent: true, Finished: true, Usage: UsageWinner, SlowDecode: true}},
		{"decoded logprobs", Attempt{Nonce: 8, Sent: true, Finished: true, Usage: UsageWinner, LogprobsDecoded: true}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			book := newTestBook(t, 4)
			if err := book.ObserveLatestNonce(testEscrow, 8); err != nil {
				t.Fatalf("ObserveLatestNonce(): %v", err)
			}
			// Nonces 4 and 8 share slot 0 in a group of four, so the two counters differ in the flag alone.
			if err := book.RecordRace(testEscrow, []Attempt{
				{Nonce: 4, Sent: true, Finished: true, Usage: UsageWinner},
				testCase.attempt,
			}); err != nil {
				t.Fatalf("RecordRace(): %v", err)
			}

			restored := saveAndReload(t, book, openTestStore(t))

			counters := restored.Query(QueryFilter{Participant: participantFor(0)})[0].Counters
			if len(counters) != 2 {
				t.Fatalf("got %d counters after a restart, want both: %+v", len(counters), counters)
			}
		})
	}
}

// The identity of a counter is its key, and the table's primary key is where that identity is enforced.
// A field added to one and not the other silently merges two counters into one row.
func TestEveryCounterKeyFieldIsPartOfTheStoredIdentity(t *testing.T) {
	store := openTestStore(t)
	rows, err := store.db.QueryContext(context.Background(), `SELECT name FROM pragma_table_info('accounting_counters') WHERE pk > 0`)
	if err != nil {
		t.Fatalf("reading the counters primary key: %v", err)
	}
	defer rows.Close()
	stored := map[string]bool{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scanning a primary-key column: %v", err)
		}
		stored[column] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("reading the counters primary key: %v", err)
	}

	keyType := reflect.TypeFor[CounterKey]()
	for i := range keyType.NumField() {
		column, _, _ := strings.Cut(keyType.Field(i).Tag.Get("json"), ",")
		if column == "" || column == "-" {
			continue
		}
		if !stored[column] {
			t.Errorf("CounterKey.%s is not in the counters primary key: two counters differing only in it "+
				"would collide and fail the snapshot", keyType.Field(i).Name)
		}
	}
}
