package config

import (
	"os"
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

		CaptureSampleRate: float64Pointer(0.1),
		CaptureMaxBytes:   int64Pointer(4096),
	}
	overrides := Overrides{
		DefaultMaxTokens:                    int64Pointer(1500), // ...but admin override wins over env
		Disabled:                            boolPointer(true),
		MaxConcurrentRequestsPer10000Weight: float64Pointer(2.5), // per-weight has no env var; admin override is its only source
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
	if !configuration.Modes.Disabled {
		t.Error("Modes.Disabled = false, want override value true")
	}
	if configuration.Limits.MaxTokensCap != 4096 {
		t.Errorf("Limits.MaxTokensCap = %d, want untouched default 4096", configuration.Limits.MaxTokensCap)
	}
	if configuration.Limits.Concurrency.RequestsPer10000Weight != 2.5 {
		t.Errorf("Limits.Concurrency.RequestsPer10000Weight = %v, want override value 2.5", configuration.Limits.Concurrency.RequestsPer10000Weight)
	}
	if configuration.Capture.SampleRate != 0.1 {
		t.Errorf("Capture.SampleRate = %v, want env value 0.1", configuration.Capture.SampleRate)
	}
	if configuration.Capture.MaxBytes != 4096 {
		t.Errorf("Capture.MaxBytes = %d, want env value 4096", configuration.Capture.MaxBytes)
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
	values := env.Values{MaxTokensCap: int64Pointer(0)}
	_, err := Build(values, Overrides{})
	if err == nil || !strings.Contains(err.Error(), "max_tokens_cap") {
		t.Fatalf("Build() with an unset cap: want max_tokens_cap validation error, got %v", err)
	}
}

func TestBuildClonesOverridesModelLimits(t *testing.T) {
	sourceModelLimits := map[string]ModelLimits{
		"model-a": {DefaultMaxTokens: 100, MaxTokensCap: 200},
	}
	overrides := Overrides{ModelLimits: sourceModelLimits}

	configuration, err := Build(env.Values{}, overrides)
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}

	sourceModelLimits["model-a"] = ModelLimits{DefaultMaxTokens: 999, MaxTokensCap: 999}
	sourceModelLimits["model-b"] = ModelLimits{DefaultMaxTokens: 1, MaxTokensCap: 2}

	if got := configuration.Limits.ModelLimits["model-a"].DefaultMaxTokens; got != 100 {
		t.Fatalf("Limits.ModelLimits[model-a].DefaultMaxTokens = %d, want untouched 100 after mutating the source map (map was aliased, not cloned)", got)
	}
	if _, present := configuration.Limits.ModelLimits["model-b"]; present {
		t.Fatal("Limits.ModelLimits gained a key added to the source map after Build — map was aliased, not cloned")
	}
}

// deployedEnvTemplate is the file an operator sources to start the gateway, read here rather than
// copied so a value that drifts out of what Build accepts fails this test instead of a deploy.
const deployedEnvTemplate = "../../../../deploy/join/config.devshard.env.template"

func TestBuildAcceptsTheShippedEnvTemplate(t *testing.T) {
	template, err := os.ReadFile(deployedEnvTemplate)
	if err != nil {
		t.Fatalf("reading %s: %v", deployedEnvTemplate, err)
	}
	exported := 0
	for line := range strings.SplitSeq(string(template), "\n") {
		assignment, isExport := strings.CutPrefix(strings.TrimSpace(line), "export GATEWAY_")
		if !isExport {
			continue
		}
		name, value, hasValue := strings.Cut(assignment, "=")
		if !hasValue {
			t.Fatalf("malformed export line: %q", line)
		}
		exported++
		t.Setenv("GATEWAY_"+name, value)
	}
	if exported == 0 {
		t.Fatalf("%s exports no GATEWAY_ variable, so this test asserts nothing", deployedEnvTemplate)
	}

	values, err := env.Load()
	if err != nil {
		t.Fatalf("env.Load() = %v, want nil", err)
	}
	if _, err := Build(values, Overrides{}); err != nil {
		t.Fatalf("Build() on the shipped template = %v; the gateway refuses to boot on its own deploy file", err)
	}
}

// The CometBFT RPC endpoint is what every chain read falls back to when the gRPC query path fails.
// Left to derivation it lands on the standard port of the gRPC host, which is right for a default
// deployment and wrong for one that moved it -- and wrong there means a fallback nobody is listening
// on, which fails only when it is needed.
func TestTheChainRPCFallbackIsConfigurable(t *testing.T) {
	configuration, err := Build(env.Values{ChainRPC: stringPointer("http://cometbft.internal:26657")}, Overrides{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if configuration.Chain.RPCEndpoint != "http://cometbft.internal:26657" {
		t.Fatalf("Chain.RPCEndpoint = %q, want the configured endpoint", configuration.Chain.RPCEndpoint)
	}
}

// A rollback lever is worth nothing if it cannot be pulled without a redeploy, and a default-true flag
// is the kind that regresses to false unnoticed.
func TestForcedStreamingIsOnByDefaultAndTurnedOffWithoutARedeploy(t *testing.T) {
	tests := []struct {
		name      string
		values    env.Values
		overrides Overrides
		want      bool
	}{
		{name: "nothing configured", want: true},
		{name: "turned off by env", values: env.Values{ForceUpstreamStreaming: boolPointer(false)}, want: false},
		{name: "turned off at runtime", overrides: Overrides{ForceUpstreamStreaming: boolPointer(false)}, want: false},
		{
			name:      "turned back on at runtime over an env that turned it off",
			values:    env.Values{ForceUpstreamStreaming: boolPointer(false)},
			overrides: Overrides{ForceUpstreamStreaming: boolPointer(true)},
			want:      true,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			configuration, err := Build(testCase.values, testCase.overrides)
			if err != nil {
				t.Fatalf("Build(): %v", err)
			}
			if got := configuration.Limits.ForceUpstreamStreaming; got != testCase.want {
				t.Errorf("force_upstream_streaming = %v, want %v", got, testCase.want)
			}
		})
	}
}
