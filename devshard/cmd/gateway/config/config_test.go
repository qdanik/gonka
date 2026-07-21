package config

import (
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
		{"Capture.ShortContentMinOutputChunks", configuration.Capture.ShortContentMinOutputChunks, int64(1_000)},
		{"Capture.ShortContentMaxContentRatio", configuration.Capture.ShortContentMaxContentRatio, 0.75},
		{"Capture.ShortContentResponseMaxBytes", configuration.Capture.ShortContentResponseMaxBytes, int64(16_777_216)},
		{"Stream.ClassifyMaxAttemptBytes", configuration.Stream.ClassifyMaxAttemptBytes, int64(1_048_576)},
		{"Stream.ClassifyMaxParticipantBytes", configuration.Stream.ClassifyMaxParticipantBytes, int64(10_485_760)},
		{"Stream.ClassifyMaxGlobalBytes", configuration.Stream.ClassifyMaxGlobalBytes, int64(104_857_600)},
		{"Perf.EWMAHalfLifeSeconds", configuration.Perf.EWMAHalfLifeSeconds, int64(600)},
		{"Perf.ColdStartReceiptMs", configuration.Perf.ColdStartReceiptMs, 2_000.0},
		{"Perf.ColdStartCTTFLMsPerToken", configuration.Perf.ColdStartCTTFLMsPerToken, 1.0},
		{"Perf.ConsecutiveFailThreshold", configuration.Perf.ConsecutiveFailThreshold, int64(5)},
		{"Perf.FailureRateThreshold", configuration.Perf.FailureRateThreshold, 0.15},
		{"Perf.FailureRateMinVolume", configuration.Perf.FailureRateMinVolume, 20.0},
		{"Perf.EjectionBaseSeconds", configuration.Perf.EjectionBaseSeconds, int64(30)},
		{"Perf.EjectionMaxSeconds", configuration.Perf.EjectionMaxSeconds, int64(600)},
		{"Perf.MaxEjectionFraction", configuration.Perf.MaxEjectionFraction, 0.5},
		{"Perf.MinAvailableHosts", configuration.Perf.MinAvailableHosts, int64(1)},
		{"Perf.FirstTokenReservoir", configuration.Perf.FirstTokenReservoir, int64(100)},
		{"Perf.FirstTokenActivationSamples", configuration.Perf.FirstTokenActivationSamples, int64(20)},
		{"Perf.FirstTokenPercentile", configuration.Perf.FirstTokenPercentile, 0.95},
		{"Perf.FirstTokenStalenessSeconds", configuration.Perf.FirstTokenStalenessSeconds, int64(86_400)},
		{"Perf.HostStalenessSeconds", configuration.Perf.HostStalenessSeconds, int64(3_600)},
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
	configuration.Limits.MaxTokensCap = 100 // below DefaultMaxTokens 3072
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
		{"max_tokens_cap below default", func(c *Config) { c.Limits.MaxTokensCap = 1 }, "max_tokens_cap"},
		{"max_concurrent_requests too low", func(c *Config) { c.Limits.Concurrency.MaxRequests = 0 }, "max_concurrent_requests"},
		{"max_concurrent_requests_per_10000_weight negative", func(c *Config) { c.Limits.Concurrency.RequestsPer10000Weight = -1 }, "max_concurrent_requests_per_10000_weight"},
		{"poc_max_concurrent_requests_per_10000_weight negative", func(c *Config) { c.Limits.Concurrency.PoCRequestsPer10000Weight = -1 }, "poc_max_concurrent_requests_per_10000_weight"},
		{"max_input_tokens_in_flight negative", func(c *Config) { c.Limits.MaxInputTokensInFlight = -1 }, "max_input_tokens_in_flight"},
		{"acquire_wait_ms negative", func(c *Config) { c.Limits.AcquireWaitMS = -1 }, "acquire_wait_ms"},
		{"aimd_initial_window too low", func(c *Config) { c.Limits.AIMD.InitialWindow = 0 }, "aimd_initial_window"},
		{"aimd_max_window below initial", func(c *Config) { c.Limits.AIMD.MaxWindow = 1 }, "aimd_max_window"},
		{"breaker_trip_threshold too low", func(c *Config) { c.Limits.Breaker.TripThreshold = 0 }, "breaker_trip_threshold"},
		{"breaker_base_open_ms too low", func(c *Config) { c.Limits.Breaker.BaseOpenMS = 0 }, "breaker_base_open_ms"},
		{"breaker_max_open_ms below base", func(c *Config) { c.Limits.Breaker.MaxOpenMS = 1 }, "breaker_max_open_ms"},
		{"model_access bad enum", func(c *Config) { c.Limits.ModelAccess = map[string]string{"model-a": "bogus"} }, "model_access"},
		{"model_limits default too low", func(c *Config) {
			c.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 0, MaxTokensCap: 10}}
		}, "model_limits"},
		{"model_limits cap below default", func(c *Config) {
			c.Limits.ModelLimits = map[string]ModelLimits{"model-a": {DefaultMaxTokens: 100, MaxTokensCap: 50}}
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
		{"capture_short_content_min_output_chunks negative", func(c *Config) { c.Capture.ShortContentMinOutputChunks = -1 }, "capture_short_content_min_output_chunks"},
		{"capture_short_content_max_content_ratio zero", func(c *Config) { c.Capture.ShortContentMaxContentRatio = 0 }, "capture_short_content_max_content_ratio"},
		{"capture_short_content_max_content_ratio above one", func(c *Config) { c.Capture.ShortContentMaxContentRatio = 1.5 }, "capture_short_content_max_content_ratio"},
		{"capture_short_content_response_max_bytes too low", func(c *Config) { c.Capture.ShortContentResponseMaxBytes = 0 }, "capture_short_content_response_max_bytes"},
		{"drain_timeout_seconds too low", func(c *Config) { c.Stream.DrainTimeoutSeconds = 0 }, "drain_timeout_seconds"},
		{"classify_max_attempt_bytes too low", func(c *Config) { c.Stream.ClassifyMaxAttemptBytes = 0 }, "classify_max_attempt_bytes"},
		{"classify_max_participant_bytes below attempt", func(c *Config) { c.Stream.ClassifyMaxParticipantBytes = 100 }, "classify_max_participant_bytes"},
		{"classify_max_global_bytes below participant", func(c *Config) { c.Stream.ClassifyMaxGlobalBytes = 100 }, "classify_max_global_bytes"},
		{"perf_ewma_half_life_seconds too low", func(c *Config) { c.Perf.EWMAHalfLifeSeconds = 0 }, "perf_ewma_half_life_seconds"},
		{"perf_cold_start_receipt_ms negative", func(c *Config) { c.Perf.ColdStartReceiptMs = -1 }, "perf_cold_start_receipt_ms"},
		{"perf_cold_start_cttfl_ms_per_token negative", func(c *Config) { c.Perf.ColdStartCTTFLMsPerToken = -1 }, "perf_cold_start_cttfl_ms_per_token"},
		{"perf_consecutive_fail_threshold too low", func(c *Config) { c.Perf.ConsecutiveFailThreshold = 0 }, "perf_consecutive_fail_threshold"},
		{"perf_failure_rate_threshold zero", func(c *Config) { c.Perf.FailureRateThreshold = 0 }, "perf_failure_rate_threshold"},
		{"perf_failure_rate_threshold above one", func(c *Config) { c.Perf.FailureRateThreshold = 1.5 }, "perf_failure_rate_threshold"},
		{"perf_failure_rate_min_volume too low", func(c *Config) { c.Perf.FailureRateMinVolume = 0 }, "perf_failure_rate_min_volume"},
		{"perf_ejection_base_seconds too low", func(c *Config) { c.Perf.EjectionBaseSeconds = 0 }, "perf_ejection_base_seconds"},
		{"perf_ejection_max_seconds below base", func(c *Config) { c.Perf.EjectionMaxSeconds = 1 }, "perf_ejection_max_seconds"},
		{"perf_max_ejection_fraction zero", func(c *Config) { c.Perf.MaxEjectionFraction = 0 }, "perf_max_ejection_fraction"},
		{"perf_max_ejection_fraction above one", func(c *Config) { c.Perf.MaxEjectionFraction = 1.5 }, "perf_max_ejection_fraction"},
		{"perf_min_available_hosts negative", func(c *Config) { c.Perf.MinAvailableHosts = -1 }, "perf_min_available_hosts"},
		{"perf_first_token_reservoir too low", func(c *Config) { c.Perf.FirstTokenReservoir = 0 }, "perf_first_token_reservoir"},
		{"perf_first_token_activation_samples too low", func(c *Config) { c.Perf.FirstTokenActivationSamples = 0 }, "perf_first_token_activation_samples"},
		{"perf_first_token_activation_samples above reservoir", func(c *Config) { c.Perf.FirstTokenActivationSamples = c.Perf.FirstTokenReservoir + 1 }, "perf_first_token_activation_samples"},
		{"perf_first_token_percentile zero", func(c *Config) { c.Perf.FirstTokenPercentile = 0 }, "perf_first_token_percentile"},
		{"perf_first_token_percentile above one", func(c *Config) { c.Perf.FirstTokenPercentile = 1.5 }, "perf_first_token_percentile"},
		{"perf_first_token_staleness_seconds too low", func(c *Config) { c.Perf.FirstTokenStalenessSeconds = 0 }, "perf_first_token_staleness_seconds"},
		{"perf_host_staleness_seconds too low", func(c *Config) { c.Perf.HostStalenessSeconds = 0 }, "perf_host_staleness_seconds"},
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

// TestValidateAcceptsModelLimitsWithNilOptionalPointers: a ModelLimits entry
// with only the required token pair set and both optional pointers nil must validate cleanly.
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
