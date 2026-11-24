package config

import (
	"strings"
	"testing"
)

// Test flow:
//  1. Table-driven: for each affinity setting, take a default config and set the field to its ceiling.
//  2. Assert `Validate` accepts it.
//  3. Set the field one above the ceiling and assert `Validate` refuses it, naming the setting.
func TestValidateAffinityCeilings(t *testing.T) {
	testCases := []struct {
		name    string
		ceiling int64
		set     func(c *Config, value int64)
	}{
		{"affinity_ttl_ms", 600_000, func(c *Config, v int64) { c.Affinity.TTLMS = v }},
		{"affinity_max_entries", 100_000, func(c *Config, v int64) { c.Affinity.MaxEntries = v }},
		{"affinity_max_pin_age_ms", 3_600_000, func(c *Config, v int64) { c.Affinity.MaxPinAgeMS = v }},
		{"affinity_share_cap", 125, func(c *Config, v int64) { c.Affinity.ShareCap = v }},
		{"affinity_max_burns", 8, func(c *Config, v int64) { c.Affinity.MaxBurns = v }},
		{"affinity_min_cached_tokens", 16_384, func(c *Config, v int64) { c.Affinity.MinCachedTokens = v }},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			atCeiling := Defaults()
			testCase.set(&atCeiling, testCase.ceiling)
			if err := atCeiling.Validate(); err != nil {
				t.Fatalf("Validate() with %s at its ceiling %d = %v, want nil", testCase.name, testCase.ceiling, err)
			}

			aboveCeiling := Defaults()
			testCase.set(&aboveCeiling, testCase.ceiling+1)
			err := aboveCeiling.Validate()
			if err == nil {
				t.Fatalf("Validate() accepted %s at %d, one above its ceiling", testCase.name, testCase.ceiling+1)
			}
			if !strings.Contains(err.Error(), testCase.name) {
				t.Fatalf("error %q does not mention %q", err.Error(), testCase.name)
			}
		})
	}
}
