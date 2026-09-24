package accounting

import "testing"

// Test flow:
//  1. Read the package's `SchemaVersion` constant.
//  2. Assert it is above the legacy devshard/accounting version 6, so the two gateways' shapes are told apart under one field name.
func TestSchemaVersionIsAboveTheLegacyLedger(t *testing.T) {
	t.Parallel()
	const legacy = 6
	if SchemaVersion <= legacy {
		t.Fatalf("SchemaVersion = %d, want above the legacy devshard/accounting %d: the two gateways emit different shapes under one field name", SchemaVersion, legacy)
	}
}
