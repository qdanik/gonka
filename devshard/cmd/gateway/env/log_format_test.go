package env

import (
	"strings"
	"testing"
)

// The format is applied before anything can log, so it is read apart from Load and defaults to what a collector parses.
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

// A typo must not silently pick a format; Load reports it with every other misconfigured variable.
func TestLoadRefusesALogFormatItDoesNotKnow(t *testing.T) {
	t.Setenv("GATEWAY_LOG_FORMAT", "logfmt")

	_, err := Load()

	if err == nil || !strings.Contains(err.Error(), `GATEWAY_LOG_FORMAT: "logfmt" is not "json" or "text"`) {
		t.Fatalf("Load() = %v, want the unknown log format named", err)
	}
}
