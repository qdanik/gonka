package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"devshard/cmd/gateway/env"
)

func int64Pointer(value int64) *int64       { return &value }
func float64Pointer(value float64) *float64 { return &value }
func stringPointer(value string) *string    { return &value }
func boolPointer(value bool) *bool          { return &value }

// Test flow:
//  1. Build a config from environment values (port, capture sample rate, capture max bytes) and admin overrides (max tokens, disabled flag, per-weight concurrency) that mix env-only, override-only, and both-set fields.
//  2. Assert each built field takes the right source: an override beats the same env value, an env value beats an untouched default, and an override-only field takes the override.
func TestBuildAppliesPrecedenceDefaultsEnvOverrides(t *testing.T) {
	values := env.Values{
		Port:             int64Pointer(9000),
		DefaultMaxTokens: int64Pointer(2000),

		CaptureSampleRate: float64Pointer(0.1),
		CaptureMaxBytes:   int64Pointer(4096),
	}
	overrides := Overrides{
		DefaultMaxTokens:                    int64Pointer(1500),
		Disabled:                            boolPointer(true),
		MaxConcurrentRequestsPer10000Weight: float64Pointer(2.5),
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

// Test flow:
//  1. Build a config from an `APIKeys` environment value listing keys separated by commas with extra whitespace and an empty entry.
//  2. Assert the built server's API keys are the three keys, trimmed and with the blank entry dropped.
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

// Test flow:
//  1. Build a config with `MaxTokensCap` set to 0 in the environment.
//  2. Assert `Build` returns an error naming `max_tokens_cap`.
func TestBuildRejectsInvalidMergedConfig(t *testing.T) {
	values := env.Values{MaxTokensCap: int64Pointer(0)}
	_, err := Build(values, Overrides{})
	if err == nil || !strings.Contains(err.Error(), "max_tokens_cap") {
		t.Fatalf("Build() with an unset cap: want max_tokens_cap validation error, got %v", err)
	}
}

// Test flow:
//  1. Build a config from an override whose `ModelLimits` map has one entry.
//  2. Mutate the source map's existing entry and add a new one.
//  3. Assert the built config's model limits are unaffected by either mutation, since `Build` clones the map rather than aliasing it.
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

// Test flow:
//  1. Build a config with no overrides.
//  2. Assert the rotation hold defaults to enabled, one per model, with 32 resume answers.
func TestTheHoldIsOnByDefault(t *testing.T) {
	configuration, err := Build(env.Values{}, Overrides{})
	if err != nil {
		t.Fatalf("Build() = %v, want nil", err)
	}
	rotation := configuration.Rotation
	if !rotation.HoldEnabled || rotation.HoldMaxPerModel != 1 || rotation.HoldResumeAnswers != 32 {
		t.Fatalf("rotation = %+v, want hold on, 1 per model, 32 answers", rotation)
	}
}

// Test flow:
//  1. Build a config with `RotationHoldEnabled` overridden to false.
//  2. Assert the built rotation hold is disabled.
func TestAnOverrideTurnsTheHoldOff(t *testing.T) {
	disabled := false
	configuration, err := Build(env.Values{}, Overrides{RotationHoldEnabled: &disabled})
	if err != nil {
		t.Fatalf("Build() = %v, want nil", err)
	}
	if configuration.Rotation.HoldEnabled {
		t.Fatal("HoldEnabled = true, want the override to turn it off")
	}
}

// Test flow:
//  1. Table-driven: each case sets one invalid hold setting — a negative per-model cap or zero resume-answer headroom.
//  2. For each case, call `Build`.
//  3. Assert it returns `ErrInvalid`.
func TestHoldSettingsAreValidated(t *testing.T) {
	negative, zero := int64(-1), int64(0)
	for name, overrides := range map[string]Overrides{
		"negative cap":       {RotationHoldMaxPerModel: &negative},
		"no resume headroom": {RotationHoldResumeAnswers: &zero},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Build(env.Values{}, overrides); !errors.Is(err, ErrInvalid) {
				t.Fatalf("Build() = %v, want ErrInvalid", err)
			}
		})
	}
}

// deployedEnvTemplates are the operator-facing deploy files Build must accept without failing.
var deployedEnvTemplates = []string{
	"../../../../deploy/join/config.devshard.env.template",
	"../deploy/config.devshard-gateway.env.template",
}

// Test flow:
//  1. For each deployed env template file (`deployedEnvTemplates`), export every `GATEWAY_` variable it sets, substituting placeholder secrets, then load and build a config from the real environment via `buildFromTemplate`.
//  2. Assert the gateway builds without error from its own shipped deploy files.
func TestBuildAcceptsTheShippedEnvTemplate(t *testing.T) {
	for _, path := range deployedEnvTemplates {
		t.Run(filepath.Base(filepath.Dir(path)), func(t *testing.T) {
			buildFromTemplate(t, path)
		})
	}
}

// templateSecrets substitutes the templates' placeholder keys so parsing them is not judged as a real secret.
var templateSecrets = map[string]string{
	"API_KEYS":      "sk-template-placeholder",
	"ADMIN_API_KEY": "sk-admin-template-placeholder",
}

func buildFromTemplate(t *testing.T, deployedEnvTemplate string) {
	t.Helper()
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
		if placeholder, isSecret := templateSecrets[name]; isSecret {
			value = placeholder
		}
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
		t.Fatalf("Build() on %s = %v; the gateway refuses to boot on its own deploy file", deployedEnvTemplate, err)
	}
}

// Test flow:
//  1. Build a config with `ChainRPC` set to a custom CometBFT endpoint.
//  2. Assert the built chain config's RPC endpoint matches it, rather than the derived default.
func TestTheChainRPCFallbackIsConfigurable(t *testing.T) {
	configuration, err := Build(env.Values{ChainRPC: stringPointer("http://cometbft.internal:26657")}, Overrides{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if configuration.Chain.RPCEndpoint != "http://cometbft.internal:26657" {
		t.Fatalf("Chain.RPCEndpoint = %q, want the configured endpoint", configuration.Chain.RPCEndpoint)
	}
}

// Test flow:
//  1. Table-driven: each case configures forced upstream streaming from nothing, from the environment, from a runtime override, or from a runtime override overriding an environment value.
//  2. For each case, build a config.
//  3. Assert the forced-streaming flag matches the case's expectation, proving the default-true flag can be turned off, and back on, without a redeploy.
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

// Test flow:
//  1. Table-driven: each case configures the max-consecutive-burns run length from nothing, from the environment, from a runtime override, from a runtime override winning over the environment, or turned off entirely.
//  2. For each case, build a config.
//  3. Assert the scheduler's burn-run length matches the case's expectation.
func TestTheBurnRunIsReachableWithoutARedeploy(t *testing.T) {
	tests := []struct {
		name      string
		values    env.Values
		overrides Overrides
		want      int64
	}{
		{name: "nothing configured", want: 2},
		{name: "set by env", values: env.Values{MaxConsecutiveBurns: int64Pointer(3)}, want: 3},
		{name: "set at runtime", overrides: Overrides{MaxConsecutiveBurns: int64Pointer(3)}, want: 3},
		{
			name:      "runtime wins over env",
			values:    env.Values{MaxConsecutiveBurns: int64Pointer(3)},
			overrides: Overrides{MaxConsecutiveBurns: int64Pointer(12)},
			want:      12,
		},
		{name: "turned off at runtime", overrides: Overrides{MaxConsecutiveBurns: int64Pointer(0)}, want: 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			configuration, err := Build(testCase.values, testCase.overrides)
			if err != nil {
				t.Fatalf("Build(): %v", err)
			}
			if got := configuration.Scheduler.MaxConsecutiveBurns; got != testCase.want {
				t.Errorf("scheduler_max_consecutive_burns = %d, want %d", got, testCase.want)
			}
		})
	}
}

// Test flow:
//  1. Table-driven: each case configures the failure-rate threshold and ejection-max-seconds from nothing, from the environment, from a runtime override, or from a runtime override winning over the environment.
//  2. For each case, build a config.
//  3. Assert the perf config's threshold and max-seconds match the case's expectation.
func TestEjectionThresholdsAreReachableWithoutARedeploy(t *testing.T) {
	tests := []struct {
		name          string
		values        env.Values
		overrides     Overrides
		wantRate      float64
		wantMaxSecond int64
	}{
		{name: "nothing configured", wantRate: 0.15, wantMaxSecond: 600},
		{
			name:          "set by env",
			values:        env.Values{PerfFailureRateThreshold: float64Pointer(0.5), PerfEjectionMaxSeconds: int64Pointer(180)},
			wantRate:      0.5,
			wantMaxSecond: 180,
		},
		{
			name:          "set at runtime",
			overrides:     Overrides{PerfFailureRateThreshold: float64Pointer(0.5), PerfEjectionMaxSeconds: int64Pointer(180)},
			wantRate:      0.5,
			wantMaxSecond: 180,
		},
		{
			name:          "runtime wins over env",
			values:        env.Values{PerfFailureRateThreshold: float64Pointer(0.5), PerfEjectionMaxSeconds: int64Pointer(180)},
			overrides:     Overrides{PerfFailureRateThreshold: float64Pointer(0.7), PerfEjectionMaxSeconds: int64Pointer(300)},
			wantRate:      0.7,
			wantMaxSecond: 300,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			configuration, err := Build(testCase.values, testCase.overrides)
			if err != nil {
				t.Fatalf("Build(): %v", err)
			}
			if got := configuration.Perf.FailureRateThreshold; got != testCase.wantRate {
				t.Errorf("perf_failure_rate_threshold = %v, want %v", got, testCase.wantRate)
			}
			if got := configuration.Perf.EjectionMaxSeconds; got != testCase.wantMaxSecond {
				t.Errorf("perf_ejection_max_seconds = %d, want %d", got, testCase.wantMaxSecond)
			}
		})
	}
}
