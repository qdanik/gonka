package filters

import (
	"encoding/json"
	"testing"
)

func TestTableIsNotEmpty(t *testing.T) {
	if len(parameterTable) == 0 {
		t.Fatal("parameterTable must not be empty")
	}
}

func TestTableNamesAreUnique(t *testing.T) {
	seen := make(map[string]bool, len(parameterTable))
	for _, spec := range parameterTable {
		if seen[spec.Name] {
			t.Errorf("parameterTable has a duplicate entry for %q", spec.Name)
		}
		seen[spec.Name] = true
	}
}

// Covers the names the framework itself depends on.
func TestTableKnownParametersContainsFrameworkFields(t *testing.T) {
	for _, name := range []string{"model", "stream", "max_tokens", "max_completion_tokens", "messages"} {
		if _, ok := knownParameterSet[name]; !ok {
			t.Errorf("knownParameterSet missing %q", name)
		}
	}
}

func TestTableKnownParametersDerivedFromTable(t *testing.T) {
	if len(knownParameterSet) != len(parameterTable) {
		t.Fatalf("knownParameterSet has %d entries, want %d (one per parameterTable entry)", len(knownParameterSet), len(parameterTable))
	}
	for _, spec := range parameterTable {
		if _, ok := knownParameterSet[spec.Name]; !ok {
			t.Errorf("knownParameterSet missing table entry %q", spec.Name)
		}
	}
}

func TestTableForcesASingleChoice(t *testing.T) {
	for _, testCase := range []struct {
		name string
		body string
		want any
	}{
		{"present is rewritten", `{"messages":[{"role":"user","content":"hi"}],"n":5}`, json.Number("1")},
		{"absent stays absent", `{"messages":[{"role":"user","content":"hi"}]}`, nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := NormalizeRequest([]byte(testCase.body), Options{DefaultMaxTokens: 3072, MaxTokensCap: 3072})
			if err != nil {
				t.Fatalf("NormalizeRequest() = %v, want acceptance", err)
			}
			document, err := ParseDocument(result.Body)
			if err != nil {
				t.Fatalf("ParseDocument(%s) = %v", result.Body, err)
			}
			got, _ := document.Get("n")
			if got != testCase.want {
				t.Errorf("n = %#v, want %#v (body %s)", got, testCase.want, result.Body)
			}
		})
	}
}
