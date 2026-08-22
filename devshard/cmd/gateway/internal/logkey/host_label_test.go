package logkey

import "testing"

// A full address is 45 characters of shared prefix; the tail is what tells two participants apart.
func TestAHostIsNamedByItsTail(t *testing.T) {
	if got := ShortHost("gonka1gvpv7vhk5gyxhmf9u8sc8pw5j8fr6lzalyrmkx"); got != "zalyrmkx" {
		t.Fatalf("ShortHost() = %q, want the last eight characters", got)
	}
	if got := ShortHost("short"); got != "short" {
		t.Fatalf("ShortHost() = %q, want a short address left alone", got)
	}
	if got := ShortHost(""); got != "" {
		t.Fatalf("ShortHost() = %q, want nothing for an unknown host", got)
	}
}
