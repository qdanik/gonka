package config

import (
	"strings"
	"testing"

	"devshard/cmd/gateway/env"
)

// Test flow:
//  1. Build with each layer alone and with several layers set for the network and for model-a: default, environment, admin, GATEWAY_MODELS_JSON, admin models block.
//  2. Assert the network value is the highest network layer and model-a takes the highest layer overall, while model-b inherits the network value.
func TestHostLoadModeFollowsThePrecedenceOfEveryLayer(t *testing.T) {
	environmentModels := func(mode string) *string {
		document := `{"model-a":{"host_load_mode":"` + mode + `"}}`
		return &document
	}
	tests := []struct {
		name      string
		values    env.Values
		overrides Overrides
		network   string
		modelA    string
	}{
		{name: "nothing configured", network: HostLoadModeOff, modelA: HostLoadModeOff},
		{name: "environment", values: env.Values{Tunables: env.Tunables{HostLoadMode: stringPointer(HostLoadModeWeight)}}, network: HostLoadModeWeight, modelA: HostLoadModeWeight},
		{name: "admin beats environment", values: env.Values{Tunables: env.Tunables{HostLoadMode: stringPointer(HostLoadModeWeight)}}, overrides: Overrides{Tunables: env.Tunables{HostLoadMode: stringPointer(HostLoadModeBalance)}}, network: HostLoadModeBalance, modelA: HostLoadModeBalance},
		{name: "model environment beats network admin", values: env.Values{ModelsJSON: environmentModels(HostLoadModeWeight)}, overrides: Overrides{Tunables: env.Tunables{HostLoadMode: stringPointer(HostLoadModeBalance)}}, network: HostLoadModeBalance, modelA: HostLoadModeWeight},
		{name: "model admin beats model environment", values: env.Values{ModelsJSON: environmentModels(HostLoadModeWeight)}, overrides: Overrides{Models: map[string]ModelSettings{"model-a": {HostLoadMode: stringPointer(HostLoadModeBalance)}}}, network: HostLoadModeOff, modelA: HostLoadModeBalance},
		{name: "model off beats a network mode", values: env.Values{Tunables: env.Tunables{HostLoadMode: stringPointer(HostLoadModeBalance)}}, overrides: Overrides{Models: map[string]ModelSettings{"model-a": {HostLoadMode: stringPointer(HostLoadModeOff)}}}, network: HostLoadModeBalance, modelA: HostLoadModeOff},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			rotation := rotationOfTwoModels
			testCase.overrides.RotationModelsJSON = &rotation
			built, err := Build(testCase.values, testCase.overrides)
			if err != nil {
				t.Fatalf("Build() = %v", err)
			}
			if got := built.Scheduler.HostLoadMode; got != testCase.network {
				t.Errorf("network host_load_mode = %q, want %q", got, testCase.network)
			}
			if got := built.For("model-a").Scheduler.HostLoadMode; got != testCase.modelA {
				t.Errorf("model-a host_load_mode = %q, want %q", got, testCase.modelA)
			}
			if got := built.For("model-b").Scheduler.HostLoadMode; got != testCase.network {
				t.Errorf("model-b host_load_mode = %q, want the network value %q", got, testCase.network)
			}
		})
	}
}

// Test flow:
//  1. Parse an admin document carrying host_load_mode globally and under a model, and read it back.
//  2. Merge a null for the network key and for the model key.
//  3. Assert both values round-trip and both nulls clear them.
func TestHostLoadModeRoundTripsAndNullClearsIt(t *testing.T) {
	parsed, err := ParseOverrides([]byte(`{"host_load_mode":"weight","models":{"model-a":{"host_load_mode":"balance"}}}`))
	if err != nil {
		t.Fatalf("ParseOverrides() = %v", err)
	}
	if parsed.HostLoadMode == nil || *parsed.HostLoadMode != HostLoadModeWeight || *parsed.Models["model-a"].HostLoadMode != HostLoadModeBalance {
		t.Fatalf("parsed = %+v, want weight globally and balance for model-a", parsed)
	}
	cleared, err := parsed.Merged([]byte(`{"host_load_mode":null,"models":{"model-a":{"host_load_mode":null}}}`))
	if err != nil {
		t.Fatalf("Merged() = %v", err)
	}
	if cleared.HostLoadMode != nil || cleared.Models["model-a"].HostLoadMode != nil {
		t.Errorf("after nulls = %+v, want both cleared", cleared)
	}
}

// Test flow:
//  1. Build with a bad value in each layer: the network environment, admin, a model's environment block and a model's admin block.
//  2. Assert each build is refused with the host_load_mode wording, and the model layers carry the models[<id>]. prefix.
//  3. Build with an empty string in admin and assert it is refused too.
func TestHostLoadModeRefusesAnythingButTheThreeValues(t *testing.T) {
	badModelEnvironment := `{"model-a":{"host_load_mode":"Weight"}}`
	tests := []struct {
		name      string
		values    env.Values
		overrides Overrides
		want      string
	}{
		{name: "environment", values: env.Values{Tunables: env.Tunables{HostLoadMode: stringPointer("fast")}}, want: `host_load_mode: "fast" is not off, weight or balance`},
		{name: "admin", overrides: Overrides{Tunables: env.Tunables{HostLoadMode: stringPointer("on")}}, want: `host_load_mode: "on" is not off, weight or balance`},
		{name: "admin empty string", overrides: Overrides{Tunables: env.Tunables{HostLoadMode: stringPointer("")}}, want: `host_load_mode: "" is not off, weight or balance`},
		{name: "model environment is case sensitive", values: env.Values{ModelsJSON: &badModelEnvironment}, want: `models[model-a].host_load_mode: "Weight" is not off, weight or balance`},
		{name: "model admin", overrides: Overrides{Models: map[string]ModelSettings{"model-a": {HostLoadMode: stringPointer("auto")}}}, want: `models[model-a].host_load_mode: "auto" is not off, weight or balance`},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			rotation := rotationOfTwoModels
			testCase.overrides.RotationModelsJSON = &rotation
			_, err := Build(testCase.values, testCase.overrides)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("Build() = %v, want a refusal containing %q", err, testCase.want)
			}
		})
	}
	if _, err := ParseOverrides([]byte(`{"host_load_modes":"weight"}`)); err == nil {
		t.Error("ParseOverrides with a misspelt key = nil, want a refusal")
	}
	if _, err := ParseStoredOverrides([]byte(`{"host_load_modes":"weight"}`)); err != nil {
		t.Errorf("ParseStoredOverrides with an unknown key = %v, want it tolerated", err)
	}
}
