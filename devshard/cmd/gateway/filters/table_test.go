package filters

import "testing"

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

// Covers the names the framework itself depends on (see table.go's deviation note on "messages").
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
