package env

import (
	"strings"
	"testing"
)

// Test flow:
//  1. Table-driven: each case sets the log-format environment variable to unset, "json", "text", or a differently-cased and padded spelling.
//  2. For each case, call `LogFormat`.
//  3. Assert the result is JSON by default and text only when explicitly asked for, case-insensitively and trimmed.
func TestTheLogFormatIsJSONUnlessTextIsAskedFor(t *testing.T) {
	testCases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "unset reads as json", raw: "", want: LogFormatJSON},
		{name: "json stays json", raw: "json", want: LogFormatJSON},
		{name: "text keeps the text form", raw: "text", want: LogFormatText},
		{name: "the spelling is not case sensitive and is trimmed", raw: " TEXT ", want: LogFormatText},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Setenv("GATEWAY_LOG_FORMAT", testCase.raw)

			if got := LogFormat(); got != testCase.want {
				t.Fatalf("LogFormat() = %q, want %q", got, testCase.want)
			}
		})
	}
}

// Test flow:
//  1. Set the log-format environment variable to an unrecognized value.
//  2. Call `Load`.
//  3. Assert it returns an error naming the unknown format alongside "json" or "text".
func TestLoadRefusesALogFormatItDoesNotKnow(t *testing.T) {
	t.Setenv("GATEWAY_LOG_FORMAT", "logfmt")

	_, err := Load()

	if err == nil || !strings.Contains(err.Error(), `GATEWAY_LOG_FORMAT: "logfmt" is not "json" or "text"`) {
		t.Fatalf("Load() = %v, want the unknown log format named", err)
	}
}
