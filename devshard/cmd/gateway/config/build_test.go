package config

import (
	"strings"
	"testing"

	"devshard/cmd/gateway/env"
)

func int64Pointer(value int64) *int64       { return &value }
func float64Pointer(value float64) *float64 { return &value }
func stringPointer(value string) *string    { return &value }
func boolPointer(value bool) *bool          { return &value }

func TestBuildAppliesPrecedenceDefaultsEnvOverrides(t *testing.T) {
	values := env.Values{
		Port:             int64Pointer(9000), // env overrides default 8080
		DefaultMaxTokens: int64Pointer(2000), // env sets 2000...
		ChainREST:        stringPointer("http://chain.test:1317"),
	}
	overrides := Overrides{
		DefaultMaxTokens:                    int64Pointer(1500), // ...but admin override wins over env
		Disabled:                            boolPointer(true),
		MaxConcurrentRequestsPer10000Weight: float64Pointer(2.5), // per-weight is env-diet-removed; overrides is now the only source
	}

	configuration, err := Build(values, overrides)
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	if configuration.Server.Port != 9000 {
		t.Errorf("Server.Port = %d, want env value 9000", configuration.Server.Port)
	}
	if configuration.Limits.DefaultMaxTokens != 1500 {
		t.Errorf("Limits.DefaultMaxTokens = %d, want override value 1500", configuration.Limits.DefaultMaxTokens)
	}
	if configuration.Chain.RESTBaseURL != "http://chain.test:1317" {
		t.Errorf("Chain.RESTBaseURL = %q, want env value", configuration.Chain.RESTBaseURL)
	}
	if !configuration.Modes.Disabled {
		t.Error("Modes.Disabled = false, want override value true")
	}
	if configuration.Limits.MaxTokensCap != 4096 {
		t.Errorf("Limits.MaxTokensCap = %d, want untouched default 4096", configuration.Limits.MaxTokensCap)
	}
	if configuration.Limits.Concurrency.RequestsPer10000Weight != 2.5 {
		t.Errorf("Limits.Concurrency.RequestsPer10000Weight = %v, want override value 2.5", configuration.Limits.Concurrency.RequestsPer10000Weight)
	}
}

func TestBuildSplitsAPIKeys(t *testing.T) {
	values := env.Values{APIKeys: stringPointer("key-one, key-two ,,key-three")}
	configuration, err := Build(values, Overrides{})
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	keys := configuration.Server.APIKeys
	if len(keys) != 3 || keys[0] != "key-one" || keys[1] != "key-two" || keys[2] != "key-three" {
		t.Fatalf("Server.APIKeys = %v, want three trimmed keys", keys)
	}
}

func TestBuildRejectsInvalidMergedConfig(t *testing.T) {
	values := env.Values{MaxTokensCap: int64Pointer(10)} // cap below default 3072
	_, err := Build(values, Overrides{})
	if err == nil || !strings.Contains(err.Error(), "max_tokens_cap") {
		t.Fatalf("Build() with cap<default: want max_tokens_cap validation error, got %v", err)
	}
}

// TestBuildClonesOverridesModelLimits pins the Amendment E no-aliasing
// contract: Build must clone overrides.ModelLimits, never alias it, so
// mutating the caller's map after Build cannot reach the published snapshot.
func TestBuildClonesOverridesModelLimits(t *testing.T) {
	sourceModelLimits := map[string]ModelLimits{
		"model-a": {DefaultMaxTokens: 100, MaxTokensCap: 200},
	}
	overrides := Overrides{ModelLimits: sourceModelLimits}

	configuration, err := Build(env.Values{}, overrides)
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	// Mutate the source map after Build returns.
	sourceModelLimits["model-a"] = ModelLimits{DefaultMaxTokens: 999, MaxTokensCap: 999}
	sourceModelLimits["model-b"] = ModelLimits{DefaultMaxTokens: 1, MaxTokensCap: 2}

	if got := configuration.Limits.ModelLimits["model-a"].DefaultMaxTokens; got != 100 {
		t.Fatalf("Limits.ModelLimits[model-a].DefaultMaxTokens = %d, want untouched 100 after mutating the source map (map was aliased, not cloned)", got)
	}
	if _, present := configuration.Limits.ModelLimits["model-b"]; present {
		t.Fatal("Limits.ModelLimits gained a key added to the source map after Build — map was aliased, not cloned")
	}
}
