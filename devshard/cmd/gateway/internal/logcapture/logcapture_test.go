package logcapture

import (
	"errors"
	"testing"

	"devshard/logging"
)

// Test flow:
//  1. Install a log recorder and log a warn line with a uint64 nonce and an error field.
//  2. Assert Contains reports true for an identical entry.
//  3. Assert Contains reports false when the nonce is given as an int instead of a uint64.
//  4. Assert Contains reports false when the level is given as info instead of warn.
func TestContainsComparesTheLevelTheMessageAndEveryTypedField(t *testing.T) {
	recorder := Install(t)
	refusal := errors.New("refused")

	logging.Warn("nonce burned for nobody", "nonce", uint64(5), "error", refusal)

	if !recorder.Contains(Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{"nonce", uint64(5), "error", refusal}}) {
		t.Fatal("an identical line was not found")
	}
	if recorder.Contains(Entry{Level: "warn", Msg: "nonce burned for nobody", Fields: []any{"nonce", 5, "error", refusal}}) {
		t.Fatal("a nonce logged as uint64 matched an int")
	}
	if recorder.Contains(Entry{Level: "info", Msg: "nonce burned for nobody", Fields: []any{"nonce", uint64(5), "error", refusal}}) {
		t.Fatal("a warn line matched an info expectation")
	}
}
