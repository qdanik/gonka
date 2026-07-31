package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestDefaultsAreValid(t *testing.T) {
	configuration := Defaults()
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Defaults() must validate cleanly, got: %v", err)
	}
}

func TestDefaultsMatchSpec(t *testing.T) {
	configuration := Defaults()
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Server.Port", configuration.Server.Port, int64(8080)},
		{"Chain.RESTBaseURL", configuration.Chain.RESTBaseURL, "http://localhost:1317"},
		{"Chain.PublicAPIBaseURL", configuration.Chain.PublicAPIBaseURL, "http://localhost:9000"},
		{"Tx.FeeDenom", configuration.Tx.FeeDenom, "ngonka"},
		{"Tx.FeeAmount", configuration.Tx.FeeAmount, int64(1_000_000)},
		{"Tx.GasLimit", configuration.Tx.GasLimit, int64(500_000)},
		{"Tx.PollIntervalMS", configuration.Tx.PollIntervalMS, int64(2_000)},
		{"Tx.PollTimeoutMS", configuration.Tx.PollTimeoutMS, int64(45_000)},
		{"Limits.DefaultMaxTokens", configuration.Limits.DefaultMaxTokens, int64(3072)},
		{"Limits.MaxTokensCap", configuration.Limits.MaxTokensCap, int64(4096)},
		{"Limits.Concurrency.MaxRequests", configuration.Limits.Concurrency.MaxRequests, int64(512)},
		{"Limits.Concurrency.RequestsPer10000Weight", configuration.Limits.Concurrency.RequestsPer10000Weight, 5.0},
		{"Limits.Concurrency.PoCRequestsPer10000Weight", configuration.Limits.Concurrency.PoCRequestsPer10000Weight, 10.0},
		{"Limits.MaxInputTokensInFlight", configuration.Limits.MaxInputTokensInFlight, int64(0)},
		{"Limits.AcquireWaitMS", configuration.Limits.AcquireWaitMS, int64(500)},
		{"Limits.AIMD.InitialWindow", configuration.Limits.AIMD.InitialWindow, int64(4)},
		{"Limits.AIMD.MaxWindow", configuration.Limits.AIMD.MaxWindow, int64(64)},
		{"Limits.Breaker.TripThreshold", configuration.Limits.Breaker.TripThreshold, int64(3)},
		{"Limits.Breaker.BaseOpenMS", configuration.Limits.Breaker.BaseOpenMS, int64(5_000)},
		{"Limits.Breaker.MaxOpenMS", configuration.Limits.Breaker.MaxOpenMS, int64(300_000)},
		{"Modes.PoCMode", configuration.Modes.PoCMode, "off"},
		{"Rotation.PrePoCBlocks", configuration.Rotation.PrePoCBlocks, int64(300)},
		{"Cache.ChatCacheMaxBytes", configuration.Cache.ChatCacheMaxBytes, int64(268_435_456)},
		{"Server.MaxConcurrentRuntimeBuilds", configuration.Server.MaxConcurrentRuntimeBuilds, int64(16)},
		{"Stream.DrainTimeoutSeconds", configuration.Stream.DrainTimeoutSeconds, int64(2_400)},
		{"Stream.ClassifyMaxAttemptBytes", configuration.Stream.ClassifyMaxAttemptBytes, int64(1_048_576)},
		{"Stream.ClassifyMaxParticipantBytes", configuration.Stream.ClassifyMaxParticipantBytes, int64(10_485_760)},
		{"Stream.ClassifyMaxGlobalBytes", configuration.Stream.ClassifyMaxGlobalBytes, int64(104_857_600)},
		{"Perf.EWMAHalfLifeSeconds", configuration.Perf.EWMAHalfLifeSeconds, int64(600)},
		{"Perf.ConsecutiveFailThreshold", configuration.Perf.ConsecutiveFailThreshold, int64(5)},
		{"Perf.FailureRateThreshold", configuration.Perf.FailureRateThreshold, 0.15},
		{"Perf.FailureRateMinVolume", configuration.Perf.FailureRateMinVolume, 20.0},
		{"Perf.EjectionBaseSeconds", configuration.Perf.EjectionBaseSeconds, int64(30)},
		{"Perf.EjectionMaxSeconds", configuration.Perf.EjectionMaxSeconds, int64(600)},
		{"Perf.MaxEjectionFraction", configuration.Perf.MaxEjectionFraction, 0.5},
		{"Perf.MinAvailableHosts", configuration.Perf.MinAvailableHosts, int64(1)},
		{"Perf.HostStalenessSeconds", configuration.Perf.HostStalenessSeconds, int64(3_600)},
		{"Scheduler.HoldGraceMS", configuration.Scheduler.HoldGraceMS, int64(200)},
		{"Engine.ReceiptTimeoutMS", configuration.Engine.ReceiptTimeoutMS, int64(5_000)},
		{"Engine.FirstTokenFloorMS", configuration.Engine.FirstTokenFloorMS, int64(1_000)},
		{"Engine.InterChunkStallMS", configuration.Engine.InterChunkStallMS, int64(30_000)},
		{"Engine.LoserGraceMS", configuration.Engine.LoserGraceMS, int64(600_000)},
		{"Engine.MaxSpeculativeAttempts", configuration.Engine.MaxSpeculativeAttempts, int64(0)},
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}

	// TxQueryFallbackURLs is a slice, not comparable via != like the table above.
	fallbackURLs := configuration.Chain.TxQueryFallbackURLs
	if len(fallbackURLs) != 1 || fallbackURLs[0] != "http://node1.gonka.ai:8000/chain-api" {
		t.Errorf("Chain.TxQueryFallbackURLs = %v, want single-element [http://node1.gonka.ai:8000/chain-api]", fallbackURLs)
	}
}

func TestValidateRejectsBrokenConfigAndNamesEveryProblem(t *testing.T) {
	configuration := Defaults()
	configuration.Server.Port = 0
	configuration.Limits.MaxTokensCap = 0
	configuration.Limits.AIMD.InitialWindow = 0
	configuration.Chain.RESTBaseURL = "://not-a-url"

	err := configuration.Validate()
	if err == nil {
		t.Fatal("Validate() on broken config: want error, got nil")
	}
	for _, fragment := range []string{"port", "max_tokens_cap", "aimd_initial_window", "chain_rest"} {
		if !strings.Contains(err.Error(), fragment) {
			t.Errorf("Validate() error %q does not mention %q", err.Error(), fragment)
		}
	}
}

// TestValidateCatchesEveryRuleBreach hits every complain() branch in Validate
// individually, so a flipped comparison operator anywhere fails exactly one case.
func TestValidateCatchesEveryRuleBreach(t *testing.T) {
	testCases := []struct {
		name            string
		mutate          func(configuration *Config)
		messageFragment string
	}{
		{"port too low", func(c *Config) { c.Server.Port = 0 }, "port"},
		{"port too high", func(c *Config) { c.Server.Port = 65536 }, "port"},
		{"max_concurrent_runtime_builds too low", func(c *Config) { c.Server.MaxConcurrentRuntimeBuilds = 0 }, "max_concurrent_runtime_builds"},
		{"chain_rest not a url", func(c *Config) { c.Chain.RESTBaseURL = "://not-a-url" }, "chain_rest"},
		{"public_api not a url", func(c *Config) { c.Chain.PublicAPIBaseURL = "://not-a-url" }, "public_api"},
		{"tx_query_fallback_urls element not a url", func(c *Config) { c.Chain.TxQueryFallbackURLs[0] = "://not-a-url" }, "tx_query_fallback_urls"},
		{"tx_fee_amount negative", func(c *Config) { c.Tx.FeeAmount = -1 }, "tx_fee_amount"},
		{"tx_gas_limit too low", func(c *Config) { c.Tx.GasLimit = 0 }, "tx_gas_limit"},
		{"tx_poll_interval_ms too low", func(c *Config) { c.Tx.PollIntervalMS = 0 }, "tx_poll_interval_ms"},
		{"tx_poll_timeout_ms below interval", func(c *Config) { c.Tx.PollTimeoutMS = 100 }, "tx_poll_timeout_ms"},
		{"default_max_tokens too low", func(c *Config) { c.Limits.DefaultMaxTokens = 0 }, "default_max_tokens"},
		{"max_tokens_cap too low", func(c *Config) { c.Limits.MaxTokensCap = 0 }, "max_tokens_cap"},
		{"max_concurrent_requests negative", func(c *Config) { c.Limits.Concurrency.MaxRequests = -1 }, "max_concurrent_requests"},
		{"max_concurrent_requests_per_10000_weight negative", func(c *Config) { c.Limits.Concurrency.RequestsPer10000Weight = -1 }, "max_concurrent_requests_per_10000_weight"},
		{"poc_max_concurrent_requests_per_10000_weight negative", func(c *Config) { c.Limits.Concurrency.PoCRequestsPer10000Weight = -1 }, "poc_max_concurrent_requests_per_10000_weight"},
		{"max_input_tokens_in_flight negative", func(c *Config) { c.Limits.MaxInputTokensInFlight = -1 }, "max_input_tokens_in_flight"},
		{"acquire_wait_ms negative", func(c *Config) { c.Limits.AcquireWaitMS = -1 }, "acquire_wait_ms"},
		{"aimd_initial_window too low", func(c *Config) { c.Limits.AIMD.InitialWindow = 0 }, "aimd_initial_window"},
		{"aimd_max_window below initial", func(c *Config) { c.Limits.AIMD.MaxWindow = 1 }, "aimd_max_window"},
		{"breaker_trip_threshold too low", func(c *Config) { c.Limits.Breaker.TripThreshold = 0 }, "breaker_trip_threshold"},
		{"breaker_base_open_ms too low", func(c *Config) { c.Limits.Breaker.BaseOpenMS = 0 }, "breaker_base_open_ms"},
		{"breaker_max_open_ms below base", func(c *Config) { c.Limits.Breaker.MaxOpenMS = 1 }, "breaker_max_open_ms"},
		{"breaker_max_open_ms above perf ejection horizon", func(c *Config) { c.Limits.Breaker.MaxOpenMS = c.Perf.EjectionMaxSeconds*1000 + 1 }, "dominant ejection authority"},
		{"model_access bad enum", func(c *Config) { c.Limits.ModelAccess = map[string]string{"model-a": "bogus"} }, "model_access"},
		{"admin_api_key too short to be safe", func(c *Config) { c.Server.AdminAPIKey = "short" }, "admin_api_key"},
		{"api_key model with no api keys configured", func(c *Config) {
			c.Limits.ModelAccess = map[string]string{"model-a": ModelAccessAPIKey}
			c.Server.APIKeys = nil
		}, "api_keys"},
		{"model_limits default too low", func(c *Config) {
			c.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 0, MaxTokensCap: 10}}
		}, "model_limits"},
		{"model_limits cap too low", func(c *Config) {
			c.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 100, MaxTokensCap: 0}}
		}, "model_limits"},
		{"model_limits max_concurrent_requests too low", func(c *Config) {
			tooFewRequests := int64(0)
			c.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 100, MaxTokensCap: 200, MaxConcurrentRequests: &tooFewRequests}}
		}, "model_limits"},
		{"model_limits max_input_tokens_in_flight negative", func(c *Config) {
			negativeInputTokens := int64(-1)
			c.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 100, MaxTokensCap: 200, MaxInputTokensInFlight: &negativeInputTokens}}
		}, "model_limits"},
		{"poc_mode bad enum", func(c *Config) { c.Modes.PoCMode = "bogus" }, "poc_mode"},
		{"rotation_pre_poc_blocks negative", func(c *Config) { c.Rotation.PrePoCBlocks = -1 }, "rotation_pre_poc_blocks"},
		{"chat_cache_max_bytes negative", func(c *Config) { c.Cache.ChatCacheMaxBytes = -1 }, "chat_cache_max_bytes"},
		{"capture_sample_rate negative", func(c *Config) { c.Capture.SampleRate = -0.1 }, "capture_sample_rate"},
		{"capture_sample_rate above one", func(c *Config) { c.Capture.SampleRate = 1.1 }, "capture_sample_rate"},
		{"capture_max_bytes uncapped", func(c *Config) { c.Capture.MaxBytes = 0 }, "capture_max_bytes"},
		{"drain_timeout_seconds too low", func(c *Config) { c.Stream.DrainTimeoutSeconds = 0 }, "drain_timeout_seconds"},
		{"classify_max_attempt_bytes too low", func(c *Config) { c.Stream.ClassifyMaxAttemptBytes = 0 }, "classify_max_attempt_bytes"},
		{"classify_max_participant_bytes below attempt", func(c *Config) { c.Stream.ClassifyMaxParticipantBytes = 100 }, "classify_max_participant_bytes"},
		{"classify_max_global_bytes below participant", func(c *Config) { c.Stream.ClassifyMaxGlobalBytes = 100 }, "classify_max_global_bytes"},
		{"perf_ewma_half_life_seconds too low", func(c *Config) { c.Perf.EWMAHalfLifeSeconds = 0 }, "perf_ewma_half_life_seconds"},
		{"perf_consecutive_fail_threshold too low", func(c *Config) { c.Perf.ConsecutiveFailThreshold = 0 }, "perf_consecutive_fail_threshold"},
		{"perf_failure_rate_threshold zero", func(c *Config) { c.Perf.FailureRateThreshold = 0 }, "perf_failure_rate_threshold"},
		{"perf_failure_rate_threshold above one", func(c *Config) { c.Perf.FailureRateThreshold = 1.5 }, "perf_failure_rate_threshold"},
		{"perf_failure_rate_min_volume too low", func(c *Config) { c.Perf.FailureRateMinVolume = 0 }, "perf_failure_rate_min_volume"},
		{"perf_ejection_base_seconds too low", func(c *Config) { c.Perf.EjectionBaseSeconds = 0 }, "perf_ejection_base_seconds"},
		{"perf_ejection_max_seconds below base", func(c *Config) { c.Perf.EjectionMaxSeconds = 1 }, "perf_ejection_max_seconds"},
		{"perf_max_ejection_fraction zero", func(c *Config) { c.Perf.MaxEjectionFraction = 0 }, "perf_max_ejection_fraction"},
		{"perf_max_ejection_fraction above one", func(c *Config) { c.Perf.MaxEjectionFraction = 1.5 }, "perf_max_ejection_fraction"},
		{"perf_min_available_hosts negative", func(c *Config) { c.Perf.MinAvailableHosts = -1 }, "perf_min_available_hosts"},
		{"perf_host_staleness_seconds too low", func(c *Config) { c.Perf.HostStalenessSeconds = 0 }, "perf_host_staleness_seconds"},
		{"scheduler_hold_grace_ms negative", func(c *Config) { c.Scheduler.HoldGraceMS = -1 }, "scheduler_hold_grace_ms"},
		{"scheduler_hold_grace_ms above ceiling", func(c *Config) { c.Scheduler.HoldGraceMS = 5_001 }, "scheduler_hold_grace_ms"},
		{"engine_receipt_timeout_ms too low", func(c *Config) { c.Engine.ReceiptTimeoutMS = 0 }, "engine_receipt_timeout_ms"},
		{"engine_first_token_floor_ms too low", func(c *Config) { c.Engine.FirstTokenFloorMS = 0 }, "engine_first_token_floor_ms"},
		{"engine_inter_chunk_stall_ms too low", func(c *Config) { c.Engine.InterChunkStallMS = 0 }, "engine_inter_chunk_stall_ms"},
		{"engine_loser_grace_ms below inter-chunk stall", func(c *Config) { c.Engine.LoserGraceMS = c.Engine.InterChunkStallMS - 1 }, "engine_loser_grace_ms"},
		{"engine_non_stream_response_floor_ms too low", func(c *Config) { c.Engine.NonStreamResponseFloorMS = 0 }, "engine_non_stream_response_floor_ms"},
		{"engine_per_input_token_response_lag_ms negative", func(c *Config) { c.Engine.PerInputTokenResponseLagMS = -1 }, "engine_per_input_token_response_lag_ms"},
		{"engine_max_speculative_attempts negative", func(c *Config) { c.Engine.MaxSpeculativeAttempts = -1 }, "engine_max_speculative_attempts"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			configuration := Defaults()
			testCase.mutate(&configuration)
			err := configuration.Validate()
			if err == nil {
				t.Fatalf("Validate() accepted a config broken by %q", testCase.name)
			}
			if !strings.Contains(err.Error(), testCase.messageFragment) {
				t.Fatalf("error %q does not mention %q", err.Error(), testCase.messageFragment)
			}
		})
	}
}

// A zero lag is how an operator asks for the response floor alone, so it must not be a startup error.
func TestValidateAcceptsAZeroPerInputTokenResponseLag(t *testing.T) {
	configuration := Defaults()
	configuration.Engine.PerInputTokenResponseLagMS = 0
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() with engine_per_input_token_response_lag_ms 0 = %v, want nil", err)
	}
}

// Zero is what the limiter reads as "no static cap"; rejecting it here left the two disagreeing and
// the shipped template unbootable.
func TestValidateAcceptsAnUncappedConcurrency(t *testing.T) {
	configuration := Defaults()
	configuration.Limits.Concurrency.MaxRequests = 0
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() with max_concurrent_requests 0 = %v, want nil", err)
	}
}

// The cap is the ceiling and the default is clamped to it at request time, so a default above the
// cap is a generous setting the gateway narrows, not a reason to refuse to start.
func TestValidateAcceptsADefaultAboveTheCap(t *testing.T) {
	configuration := Defaults()
	configuration.Limits.DefaultMaxTokens = 10_000
	configuration.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 10_000, MaxTokensCap: 4_096}}
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() with a default above the cap = %v, want nil", err)
	}
}

func TestValidateAcceptsModelLimitsWithNilOptionalPointers(t *testing.T) {
	configuration := Defaults()
	configuration.Limits.ModelLimits = map[string]ModelLimits{
		"model-a": {DefaultMaxTokens: 1024, MaxTokensCap: 2048},
	}
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() on ModelLimits entry with nil optional pointers: want nil, got %v", err)
	}
}

// TestValidateAcceptsEmptyTxQueryFallbackURLs: an empty fallback list is a
// valid, explicit operator choice to accept weakened recovery, not a validation error.
func TestValidateAcceptsEmptyTxQueryFallbackURLs(t *testing.T) {
	configuration := Defaults()
	configuration.Chain.TxQueryFallbackURLs = []string{}
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() with empty TxQueryFallbackURLs: want nil, got %v", err)
	}
}

// TestEngineCarriesOnlyTheLiveTunables pins the section's field set: the legacy per-input-token
// first-token lag and minimum-samples-for-decision were never read, and must not reappear under
// new names.
func TestEngineCarriesOnlyTheLiveTunables(t *testing.T) {
	engineType := reflect.TypeOf(Engine{})
	got := make([]string, 0, engineType.NumField())
	for index := 0; index < engineType.NumField(); index++ {
		got = append(got, engineType.Field(index).Name)
	}
	want := []string{
		"ReceiptTimeoutMS", "FirstTokenFloorMS", "InterChunkStallMS", "LoserGraceMS",
		"NonStreamResponseFloorMS", "PerInputTokenResponseLagMS", "MaxSpeculativeAttempts",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Engine fields = %v, want %v", got, want)
	}
}

func TestAdminEnabledDistinguishesUnsetFromConfigured(t *testing.T) {
	if (Server{}).AdminEnabled() {
		t.Error("AdminEnabled() = true for an unset key; admin routes would authenticate an empty credential")
	}
	if !(Server{AdminAPIKey: "0123456789abcdef"}).AdminEnabled() {
		t.Error("AdminEnabled() = false for a configured key")
	}
}

func TestAccessForFailsClosedOnceAPolicyExists(t *testing.T) {
	tests := []struct {
		name   string
		access map[string]string
		model  string
		want   string
	}{
		{"no policy configured leaves every model open", nil, "model-a", ModelAccessOpen},
		{"model listed in the policy keeps its tier", map[string]string{"model-a": ModelAccessAPIKey}, "model-a", ModelAccessAPIKey},
		{"model outside an existing policy is admin-only", map[string]string{"model-a": ModelAccessOpen}, "model-b", ModelAccessAdminOnly},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := (Limits{ModelAccess: testCase.access}).AccessFor(testCase.model); got != testCase.want {
				t.Fatalf("AccessFor(%q) = %q, want %q", testCase.model, got, testCase.want)
			}
		})
	}
}

func TestValidateAcceptsUnsetAdminKey(t *testing.T) {
	configuration := Defaults()
	configuration.Server.AdminAPIKey = ""
	if err := configuration.Validate(); err != nil {
		t.Fatalf("Validate() = %v, want nil: an unset admin key disables admin routes rather than being invalid", err)
	}
}
