package logcapture

import (
	"errors"
	"testing"

	"devshard/logging"
)

// A golden line is a contract on the type of every value, not only on how it prints.
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
