package accounting

import "testing"

func TestSchemaVersionIsAboveTheLegacyLedger(t *testing.T) {
	t.Parallel()
	const legacy = 6
	if SchemaVersion <= legacy {
		t.Fatalf("SchemaVersion = %d, want above the legacy devshard/accounting %d: the two gateways emit different shapes under one field name", SchemaVersion, legacy)
	}
}
