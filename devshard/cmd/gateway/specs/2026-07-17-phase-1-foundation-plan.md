# Gateway Phase 1: Foundation — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A booting `cmd/gateway` binary with the config-snapshot system (defaults ← env ← store overrides), SQLite store, Prometheus metrics endpoint, and ordered graceful shutdown.

**Architecture:** Phase 1 of 9 (see spec §4, `2026-07-17-gateway-redesign-design.md`). Four leaf packages (`env`, `config`, `store`, `metrics`) plus a thin `main.go` with a testable `run()`. Later phases (filters, chain, limits, scheduler, engine, api, escrow, e2e) plug into these seams; none of them are touched here.

**Tech Stack:** Go 1.24.2, module `devshard`; `modernc.org/sqlite` (driver name `"sqlite"`); `github.com/prometheus/client_golang` v1.23.2; std `testing`; existing `devshard/logging` used directly (no wrapper package — the thinnest possible wrapper is none; conscious simplification of spec §4's `logging/` line).

## Global Constraints

- All new code lives under `devshard/cmd/gateway/`. `cmd/devshardctl` is NOT modified in this phase.
- `os.Getenv` may appear ONLY in `cmd/gateway/env/`. No mutable package-level state anywhere; dependencies passed via constructors.
- Names are semantically clear full words (`storageDir`, not `sd`); booleans read as predicates; collections plural.
- Preserved metric names (dashboards depend on them): `devshard_http_requests_total{path,method,status}`, `devshard_http_request_duration_seconds{path,method}`.
- Every test command runs from `/Users/daniilyankouski/develop/GONKA-AI/gonka/devshard/` with `-race -count=1`.
- Before every commit: `gofmt -l ./cmd/gateway/` prints nothing and `go vet ./cmd/gateway/...` passes.
- **Commits are made by Daniil** (working agreement): at each "Commit" step the executor STOPS, reports the ready diff and the suggested message, and waits. If Daniil has explicitly authorized agent commits for this plan, commit directly instead.
- Error style: `fmt.Errorf("verb-ing thing: %w", err)`; sentinel errors as `var ErrX = errors.New("...")`, lowercase messages.

## File Structure

```
cmd/gateway/
  main.go              package main: main() + run(ctx) — wiring only
  main_test.go         boot smoke test against run()
  env/
    env.go             Values struct (pointer fields), Load(), typed parsers
    env_test.go
  config/
    config.go          Config + grouped sub-structs + Defaults() + Validate()
    build.go           Build(values, overrides) merge
    overrides.go       Overrides struct (admin-tunable subset), JSON round-trip
    holder.go          Holder: atomic snapshot + Subscribe
    config_test.go, build_test.go, overrides_test.go, holder_test.go
  store/
    store.go           Open/Close, pragmas, migration runner
    overrides.go       LoadOverrides / SaveOverrides
    devshards.go       DevshardRecord CRUD
    store_test.go, overrides_test.go, devshards_test.go
  metrics/
    metrics.go         Metrics: registry, HTTP families, Handler(), InstrumentRoute()
    metrics_test.go
```

Storage layout at runtime: `<storageDir>/gateway.db` (this phase), `<storageDir>/perf.db` (perf phase), default `storageDir` = `~/.cache/gonka-gateway` (fresh dir — deliberately NOT the old `~/.cache/gonka`, spec §16: state starts fresh).

---

### Task 1: `env/` — the only place that reads the environment

**Files:**
- Create: `cmd/gateway/env/env.go`
- Test: `cmd/gateway/env/env_test.go`

**Interfaces:**
- Consumes: nothing (leaf package).
- Produces: `env.Values` (struct of typed pointer fields; nil = variable unset), `env.Load() (Values, error)`. Field names used verbatim by `config.Build` in Task 3: `Port, StorageDir, APIKeys, AdminAPIKey, DevshardsJSON, DefaultModel, RoutePrefix, ChainREST, PublicAPI, ChainID, TxQueryREST, TxFeeDenom, TxFeeAmount, TxGasLimit, TxPollIntervalMS, TxPollTimeoutMS, DefaultMaxTokens, MaxTokensCap, MaxConcurrentRequests, MaxConcurrentRequestsPer10000Weight, PoCMaxConcurrentRequestsPer10000Weight, MaxInputTokensInFlight, AcquireWaitMS, AIMDInitialWindow, AIMDMaxWindow, BreakerTripThreshold, BreakerBaseOpenMS, BreakerMaxOpenMS, PoCMode, CapacityAwareLimits, Disabled, DisabledMessage, DisabledRedirectURL, RotationEnabled, RotationSettlementEnabled, RotationPrePoCBlocks, RotationModelsJSON, ChatCacheMaxBytes, MaxConcurrentRuntimeBuilds, DrainTimeoutSeconds, CaptureEnabled, CaptureDir, CaptureShortContentAttempts, CaptureShortContentResponses, CaptureShortContentMinOutputChunks, CaptureShortContentMaxContentRatio, CaptureShortContentResponseMaxBytes, ClassifyMaxAttemptBytes, ClassifyMaxParticipantBytes, ClassifyMaxGlobalBytes`.

The full variable table (name → type). Defaults are NOT applied here — `config.Defaults()` owns them (Task 2):

| Variable | Type |
|---|---|
| `GATEWAY_PORT` | int64 |
| `GATEWAY_STORAGE_DIR` | string |
| `GATEWAY_API_KEYS` | string (comma-separated) |
| `GATEWAY_ADMIN_API_KEY` | string |
| `GATEWAY_DEVSHARDS_JSON` | string |
| `GATEWAY_DEFAULT_MODEL` | string |
| `GATEWAY_ROUTE_PREFIX` | string |
| `GATEWAY_CHAIN_REST` | string |
| `GATEWAY_PUBLIC_API` | string |
| `GATEWAY_CHAIN_ID` | string |
| `GATEWAY_TX_QUERY_REST` | string |
| `GATEWAY_TX_FEE_DENOM` | string |
| `GATEWAY_TX_FEE_AMOUNT` | int64 |
| `GATEWAY_TX_GAS_LIMIT` | int64 |
| `GATEWAY_TX_POLL_INTERVAL_MS` | int64 |
| `GATEWAY_TX_POLL_TIMEOUT_MS` | int64 |
| `GATEWAY_DEFAULT_MAX_TOKENS` | int64 |
| `GATEWAY_MAX_TOKENS_CAP` | int64 |
| `GATEWAY_MAX_CONCURRENT_REQUESTS` | int64 |
| `GATEWAY_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT` | float64 |
| `GATEWAY_POC_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT` | float64 |
| `GATEWAY_MAX_INPUT_TOKENS_IN_FLIGHT` | int64 |
| `GATEWAY_ACQUIRE_WAIT_MS` | int64 |
| `GATEWAY_AIMD_INITIAL_WINDOW` | int64 |
| `GATEWAY_AIMD_MAX_WINDOW` | int64 |
| `GATEWAY_BREAKER_TRIP_THRESHOLD` | int64 |
| `GATEWAY_BREAKER_BASE_OPEN_MS` | int64 |
| `GATEWAY_BREAKER_MAX_OPEN_MS` | int64 |
| `GATEWAY_POC_MODE` | string (`off` \| `relaxed`) |
| `GATEWAY_CAPACITY_AWARE_LIMITS` | bool |
| `GATEWAY_DISABLED` | bool |
| `GATEWAY_DISABLED_MESSAGE` | string |
| `GATEWAY_DISABLED_REDIRECT_URL` | string |
| `GATEWAY_ROTATION_ENABLED` | bool |
| `GATEWAY_ROTATION_SETTLEMENT_ENABLED` | bool |
| `GATEWAY_ROTATION_PRE_POC_BLOCKS` | int64 |
| `GATEWAY_ROTATION_MODELS_JSON` | string |
| `GATEWAY_CHAT_CACHE_MAX_BYTES` | int64 |
| `GATEWAY_MAX_CONCURRENT_RUNTIME_BUILDS` | int64 |
| `GATEWAY_DRAIN_TIMEOUT_SECONDS` | int64 |
| `GATEWAY_CAPTURE_ENABLED` | bool |
| `GATEWAY_CAPTURE_DIR` | string |
| `GATEWAY_CAPTURE_SHORT_CONTENT_ATTEMPTS` | bool |
| `GATEWAY_CAPTURE_SHORT_CONTENT_RESPONSES` | bool |
| `GATEWAY_CAPTURE_SHORT_CONTENT_MIN_OUTPUT_CHUNKS` | int64 |
| `GATEWAY_CAPTURE_SHORT_CONTENT_MAX_CONTENT_RATIO` | float64 |
| `GATEWAY_CAPTURE_SHORT_CONTENT_RESPONSE_MAX_BYTES` | int64 |
| `GATEWAY_CLASSIFY_MAX_ATTEMPT_BYTES` | int64 |
| `GATEWAY_CLASSIFY_MAX_PARTICIPANT_BYTES` | int64 |
| `GATEWAY_CLASSIFY_MAX_GLOBAL_BYTES` | int64 |

- [ ] **Step 1: Write the failing test**

`cmd/gateway/env/env_test.go`:

```go
package env

import (
	"strings"
	"testing"
)

func TestLoadReturnsNilForUnsetVariables(t *testing.T) {
	// t.Setenv is not used here: the test asserts pristine-environment behavior,
	// so clear the two probes explicitly via Setenv with empty = unset semantics.
	t.Setenv("GATEWAY_PORT", "")
	t.Setenv("GATEWAY_CHAIN_REST", "")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load() with clean environment: unexpected error: %v", err)
	}
	if values.Port != nil {
		t.Fatalf("Port = %v, want nil for unset variable", *values.Port)
	}
	if values.ChainREST != nil {
		t.Fatalf("ChainREST = %q, want nil for unset variable", *values.ChainREST)
	}
}

func TestLoadParsesTypedValues(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "9191")
	t.Setenv("GATEWAY_CHAIN_REST", "http://example.test:1317")
	t.Setenv("GATEWAY_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT", "7.5")
	t.Setenv("GATEWAY_DISABLED", "true")
	t.Setenv("GATEWAY_CAPTURE_SHORT_CONTENT_MAX_CONTENT_RATIO", "0.5")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): unexpected error: %v", err)
	}
	if values.Port == nil || *values.Port != 9191 {
		t.Fatalf("Port = %v, want 9191", values.Port)
	}
	if values.ChainREST == nil || *values.ChainREST != "http://example.test:1317" {
		t.Fatalf("ChainREST = %v, want set", values.ChainREST)
	}
	if values.MaxConcurrentRequestsPer10000Weight == nil || *values.MaxConcurrentRequestsPer10000Weight != 7.5 {
		t.Fatalf("MaxConcurrentRequestsPer10000Weight = %v, want 7.5", values.MaxConcurrentRequestsPer10000Weight)
	}
	if values.Disabled == nil || *values.Disabled != true {
		t.Fatalf("Disabled = %v, want true", values.Disabled)
	}
	if values.CaptureShortContentMaxContentRatio == nil || *values.CaptureShortContentMaxContentRatio != 0.5 {
		t.Fatalf("CaptureShortContentMaxContentRatio = %v, want 0.5", values.CaptureShortContentMaxContentRatio)
	}
}

func TestLoadWhitespaceIsTrimmedAndEmptyMeansUnset(t *testing.T) {
	t.Setenv("GATEWAY_DEFAULT_MODEL", "  model-x  ")
	t.Setenv("GATEWAY_TX_GAS_LIMIT", "   ")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): unexpected error: %v", err)
	}
	if values.DefaultModel == nil || *values.DefaultModel != "model-x" {
		t.Fatalf("DefaultModel = %v, want trimmed \"model-x\"", values.DefaultModel)
	}
	if values.TxGasLimit != nil {
		t.Fatalf("TxGasLimit = %v, want nil for blank value", *values.TxGasLimit)
	}
}

func TestLoadRejectsMalformedValuesWithVariableName(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "not-a-number")
	t.Setenv("GATEWAY_DISABLED", "maybe")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with malformed values: want error, got nil")
	}
	message := err.Error()
	if !strings.Contains(message, "GATEWAY_PORT") {
		t.Fatalf("error %q does not name GATEWAY_PORT", message)
	}
	if !strings.Contains(message, "GATEWAY_DISABLED") {
		t.Fatalf("error %q does not name GATEWAY_DISABLED (errors must accumulate, not stop at first)", message)
	}
}

func TestLoadRejectsInvalidPoCMode(t *testing.T) {
	t.Setenv("GATEWAY_POC_MODE", "aggressive")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_POC_MODE") {
		t.Fatalf("want error naming GATEWAY_POC_MODE, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/gateway/env/ -race -count=1 -v`
Expected: FAIL — `undefined: Load` (package does not compile yet).

- [ ] **Step 3: Write the implementation**

`cmd/gateway/env/env.go`:

```go
// Package env is the single place the gateway reads environment variables.
// Load returns what is SET (nil pointer = unset); defaults are owned by the
// config package, never here.
package env

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Values mirrors every GATEWAY_* environment variable. A nil field means the
// variable was unset or blank; config.Build applies defaults in that case.
type Values struct {
	Port          *int64
	StorageDir    *string
	APIKeys       *string
	AdminAPIKey   *string
	DevshardsJSON *string
	DefaultModel  *string
	RoutePrefix   *string

	ChainREST        *string
	PublicAPI        *string
	ChainID          *string
	TxQueryREST      *string
	TxFeeDenom       *string
	TxFeeAmount      *int64
	TxGasLimit       *int64
	TxPollIntervalMS *int64
	TxPollTimeoutMS  *int64

	DefaultMaxTokens                       *int64
	MaxTokensCap                           *int64
	MaxConcurrentRequests                  *int64
	MaxConcurrentRequestsPer10000Weight    *float64
	PoCMaxConcurrentRequestsPer10000Weight *float64
	MaxInputTokensInFlight                 *int64
	AcquireWaitMS                          *int64
	AIMDInitialWindow                      *int64
	AIMDMaxWindow                          *int64
	BreakerTripThreshold                   *int64
	BreakerBaseOpenMS                      *int64
	BreakerMaxOpenMS                       *int64

	PoCMode             *string
	CapacityAwareLimits *bool
	Disabled            *bool
	DisabledMessage     *string
	DisabledRedirectURL *string

	RotationEnabled           *bool
	RotationSettlementEnabled *bool
	RotationPrePoCBlocks      *int64
	RotationModelsJSON        *string

	ChatCacheMaxBytes          *int64
	MaxConcurrentRuntimeBuilds *int64
	DrainTimeoutSeconds        *int64

	CaptureEnabled                      *bool
	CaptureDir                          *string
	CaptureShortContentAttempts         *bool
	CaptureShortContentResponses        *bool
	CaptureShortContentMinOutputChunks  *int64
	CaptureShortContentMaxContentRatio  *float64
	CaptureShortContentResponseMaxBytes *int64

	ClassifyMaxAttemptBytes     *int64
	ClassifyMaxParticipantBytes *int64
	ClassifyMaxGlobalBytes      *int64
}

// PoCModeOff and PoCModeRelaxed are the accepted GATEWAY_POC_MODE values.
const (
	PoCModeOff     = "off"
	PoCModeRelaxed = "relaxed"
)

// Load reads every gateway environment variable. Parse failures are
// accumulated so the operator sees all misconfigured variables at once.
func Load() (Values, error) {
	var values Values
	var problems []error

	readString := func(name string, target **string) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return
		}
		*target = &raw
	}
	readInt := func(name string, target **int64) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return
		}
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %q is not an integer", name, raw))
			return
		}
		*target = &parsed
	}
	readFloat := func(name string, target **float64) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return
		}
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %q is not a number", name, raw))
			return
		}
		*target = &parsed
	}
	readBool := func(name string, target **bool) {
		raw := strings.TrimSpace(os.Getenv(name))
		if raw == "" {
			return
		}
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			problems = append(problems, fmt.Errorf("%s: %q is not a boolean", name, raw))
			return
		}
		*target = &parsed
	}

	readInt("GATEWAY_PORT", &values.Port)
	readString("GATEWAY_STORAGE_DIR", &values.StorageDir)
	readString("GATEWAY_API_KEYS", &values.APIKeys)
	readString("GATEWAY_ADMIN_API_KEY", &values.AdminAPIKey)
	readString("GATEWAY_DEVSHARDS_JSON", &values.DevshardsJSON)
	readString("GATEWAY_DEFAULT_MODEL", &values.DefaultModel)
	readString("GATEWAY_ROUTE_PREFIX", &values.RoutePrefix)

	readString("GATEWAY_CHAIN_REST", &values.ChainREST)
	readString("GATEWAY_PUBLIC_API", &values.PublicAPI)
	readString("GATEWAY_CHAIN_ID", &values.ChainID)
	readString("GATEWAY_TX_QUERY_REST", &values.TxQueryREST)
	readString("GATEWAY_TX_FEE_DENOM", &values.TxFeeDenom)
	readInt("GATEWAY_TX_FEE_AMOUNT", &values.TxFeeAmount)
	readInt("GATEWAY_TX_GAS_LIMIT", &values.TxGasLimit)
	readInt("GATEWAY_TX_POLL_INTERVAL_MS", &values.TxPollIntervalMS)
	readInt("GATEWAY_TX_POLL_TIMEOUT_MS", &values.TxPollTimeoutMS)

	readInt("GATEWAY_DEFAULT_MAX_TOKENS", &values.DefaultMaxTokens)
	readInt("GATEWAY_MAX_TOKENS_CAP", &values.MaxTokensCap)
	readInt("GATEWAY_MAX_CONCURRENT_REQUESTS", &values.MaxConcurrentRequests)
	readFloat("GATEWAY_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT", &values.MaxConcurrentRequestsPer10000Weight)
	readFloat("GATEWAY_POC_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT", &values.PoCMaxConcurrentRequestsPer10000Weight)
	readInt("GATEWAY_MAX_INPUT_TOKENS_IN_FLIGHT", &values.MaxInputTokensInFlight)
	readInt("GATEWAY_ACQUIRE_WAIT_MS", &values.AcquireWaitMS)
	readInt("GATEWAY_AIMD_INITIAL_WINDOW", &values.AIMDInitialWindow)
	readInt("GATEWAY_AIMD_MAX_WINDOW", &values.AIMDMaxWindow)
	readInt("GATEWAY_BREAKER_TRIP_THRESHOLD", &values.BreakerTripThreshold)
	readInt("GATEWAY_BREAKER_BASE_OPEN_MS", &values.BreakerBaseOpenMS)
	readInt("GATEWAY_BREAKER_MAX_OPEN_MS", &values.BreakerMaxOpenMS)

	readString("GATEWAY_POC_MODE", &values.PoCMode)
	readBool("GATEWAY_CAPACITY_AWARE_LIMITS", &values.CapacityAwareLimits)
	readBool("GATEWAY_DISABLED", &values.Disabled)
	readString("GATEWAY_DISABLED_MESSAGE", &values.DisabledMessage)
	readString("GATEWAY_DISABLED_REDIRECT_URL", &values.DisabledRedirectURL)

	readBool("GATEWAY_ROTATION_ENABLED", &values.RotationEnabled)
	readBool("GATEWAY_ROTATION_SETTLEMENT_ENABLED", &values.RotationSettlementEnabled)
	readInt("GATEWAY_ROTATION_PRE_POC_BLOCKS", &values.RotationPrePoCBlocks)
	readString("GATEWAY_ROTATION_MODELS_JSON", &values.RotationModelsJSON)

	readInt("GATEWAY_CHAT_CACHE_MAX_BYTES", &values.ChatCacheMaxBytes)
	readInt("GATEWAY_MAX_CONCURRENT_RUNTIME_BUILDS", &values.MaxConcurrentRuntimeBuilds)
	readInt("GATEWAY_DRAIN_TIMEOUT_SECONDS", &values.DrainTimeoutSeconds)

	readBool("GATEWAY_CAPTURE_ENABLED", &values.CaptureEnabled)
	readString("GATEWAY_CAPTURE_DIR", &values.CaptureDir)
	readBool("GATEWAY_CAPTURE_SHORT_CONTENT_ATTEMPTS", &values.CaptureShortContentAttempts)
	readBool("GATEWAY_CAPTURE_SHORT_CONTENT_RESPONSES", &values.CaptureShortContentResponses)
	readInt("GATEWAY_CAPTURE_SHORT_CONTENT_MIN_OUTPUT_CHUNKS", &values.CaptureShortContentMinOutputChunks)
	readFloat("GATEWAY_CAPTURE_SHORT_CONTENT_MAX_CONTENT_RATIO", &values.CaptureShortContentMaxContentRatio)
	readInt("GATEWAY_CAPTURE_SHORT_CONTENT_RESPONSE_MAX_BYTES", &values.CaptureShortContentResponseMaxBytes)

	readInt("GATEWAY_CLASSIFY_MAX_ATTEMPT_BYTES", &values.ClassifyMaxAttemptBytes)
	readInt("GATEWAY_CLASSIFY_MAX_PARTICIPANT_BYTES", &values.ClassifyMaxParticipantBytes)
	readInt("GATEWAY_CLASSIFY_MAX_GLOBAL_BYTES", &values.ClassifyMaxGlobalBytes)

	if values.PoCMode != nil && *values.PoCMode != PoCModeOff && *values.PoCMode != PoCModeRelaxed {
		problems = append(problems, fmt.Errorf("GATEWAY_POC_MODE: %q is not %q or %q", *values.PoCMode, PoCModeOff, PoCModeRelaxed))
	}

	if len(problems) > 0 {
		return Values{}, fmt.Errorf("reading environment: %w", errors.Join(problems...))
	}
	return values, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/gateway/env/ -race -count=1 -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Format, vet, commit checkpoint**

Run: `gofmt -l ./cmd/gateway/ && go vet ./cmd/gateway/...`
Expected: no output from gofmt, vet passes.

Suggested commit message: `feat(gateway): env package — typed single-pass environment reading`
(Executor: stop and hand off to Daniil per Global Constraints.)

---

### Task 2: `config/` — Config, Defaults, Overrides, Validate

**Files:**
- Create: `cmd/gateway/config/config.go` (Config, Defaults, Validate)
- Create: `cmd/gateway/config/overrides.go` (Overrides)
- Test: `cmd/gateway/config/config_test.go`, `cmd/gateway/config/overrides_test.go`

**Interfaces:**
- Consumes: nothing yet (Build comes in Task 3).
- Produces (used by Tasks 3–6 and every later phase):
  - `type Config struct { Server Server; Chain Chain; Tx Tx; Limits Limits; Modes Modes; Rotation Rotation; Cache Cache; Capture Capture; Stream Stream }`
  - `func Defaults() Config`
  - `func (c *Config) Validate() error`
  - `type Overrides struct{...}` with `ParseOverrides([]byte) (Overrides, error)` and `(Overrides).MarshalJSONBytes() ([]byte, error)`
  - `type ModelTokenLimit struct { DefaultMaxTokens int64; MaxTokensCap int64 }`

- [ ] **Step 1: Write the failing tests**

`cmd/gateway/config/config_test.go`:

```go
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
		{"Server.DefaultModel", configuration.Server.DefaultModel, "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8"},
		{"Chain.RESTBaseURL", configuration.Chain.RESTBaseURL, "http://localhost:1317"},
		{"Chain.PublicAPIBaseURL", configuration.Chain.PublicAPIBaseURL, "http://localhost:9000"},
		{"Chain.TxQueryBaseURL", configuration.Chain.TxQueryBaseURL, "http://node1.gonka.ai:8000/chain-api"},
		{"Tx.FeeDenom", configuration.Tx.FeeDenom, "ngonka"},
		{"Tx.FeeAmount", configuration.Tx.FeeAmount, int64(1_000_000)},
		{"Tx.GasLimit", configuration.Tx.GasLimit, int64(500_000)},
		{"Tx.PollIntervalMS", configuration.Tx.PollIntervalMS, int64(2_000)},
		{"Tx.PollTimeoutMS", configuration.Tx.PollTimeoutMS, int64(45_000)},
		{"Limits.DefaultMaxTokens", configuration.Limits.DefaultMaxTokens, int64(3072)},
		{"Limits.MaxTokensCap", configuration.Limits.MaxTokensCap, int64(4096)},
		{"Limits.MaxConcurrentRequests", configuration.Limits.MaxConcurrentRequests, int64(512)},
		{"Limits.MaxConcurrentRequestsPer10000Weight", configuration.Limits.MaxConcurrentRequestsPer10000Weight, 5.0},
		{"Limits.PoCMaxConcurrentRequestsPer10000Weight", configuration.Limits.PoCMaxConcurrentRequestsPer10000Weight, 10.0},
		{"Limits.MaxInputTokensInFlight", configuration.Limits.MaxInputTokensInFlight, int64(0)},
		{"Limits.AcquireWaitMS", configuration.Limits.AcquireWaitMS, int64(500)},
		{"Limits.AIMDInitialWindow", configuration.Limits.AIMDInitialWindow, int64(4)},
		{"Limits.AIMDMaxWindow", configuration.Limits.AIMDMaxWindow, int64(64)},
		{"Limits.BreakerTripThreshold", configuration.Limits.BreakerTripThreshold, int64(3)},
		{"Limits.BreakerBaseOpenMS", configuration.Limits.BreakerBaseOpenMS, int64(5_000)},
		{"Limits.BreakerMaxOpenMS", configuration.Limits.BreakerMaxOpenMS, int64(300_000)},
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
	}
	for _, check := range checks {
		if check.got != check.want {
			t.Errorf("%s = %v, want %v", check.name, check.got, check.want)
		}
	}
}

func TestValidateRejectsBrokenConfigAndNamesEveryProblem(t *testing.T) {
	configuration := Defaults()
	configuration.Server.Port = 0
	configuration.Limits.MaxTokensCap = 100 // below DefaultMaxTokens 3072
	configuration.Limits.AIMDInitialWindow = 0
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
```

`cmd/gateway/config/overrides_test.go`:

```go
package config

import (
	"testing"
)

func TestOverridesJSONRoundTrip(t *testing.T) {
	maxTokens := int64(2048)
	disabled := true
	original := Overrides{
		DefaultMaxTokens: &maxTokens,
		Disabled:         &disabled,
		ModelAccess:      map[string]string{"model-a": "api_key"},
		ModelTokenLimits: map[string]ModelTokenLimit{
			"model-a": {DefaultMaxTokens: 1024, MaxTokensCap: 2048},
		},
	}

	encoded, err := original.MarshalJSONBytes()
	if err != nil {
		t.Fatalf("MarshalJSONBytes(): %v", err)
	}
	decoded, err := ParseOverrides(encoded)
	if err != nil {
		t.Fatalf("ParseOverrides(): %v", err)
	}
	if decoded.DefaultMaxTokens == nil || *decoded.DefaultMaxTokens != 2048 {
		t.Fatalf("DefaultMaxTokens = %v, want 2048", decoded.DefaultMaxTokens)
	}
	if decoded.MaxTokensCap != nil {
		t.Fatalf("MaxTokensCap = %v, want nil (never set)", *decoded.MaxTokensCap)
	}
	if decoded.ModelAccess["model-a"] != "api_key" {
		t.Fatalf("ModelAccess = %v, want model-a→api_key", decoded.ModelAccess)
	}
	if decoded.ModelTokenLimits["model-a"].MaxTokensCap != 2048 {
		t.Fatalf("ModelTokenLimits = %v, want model-a cap 2048", decoded.ModelTokenLimits)
	}
}

func TestParseOverridesRejectsUnknownFields(t *testing.T) {
	_, err := ParseOverrides([]byte(`{"no_such_setting": 1}`))
	if err == nil {
		t.Fatal("ParseOverrides with unknown field: want error, got nil (admin typos must not be silently ignored)")
	}
}

func TestParseOverridesOfEmptyObjectIsEmpty(t *testing.T) {
	decoded, err := ParseOverrides([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseOverrides(empty): %v", err)
	}
	if decoded.DefaultMaxTokens != nil || decoded.Disabled != nil || len(decoded.ModelAccess) != 0 {
		t.Fatalf("ParseOverrides(empty) = %+v, want zero value", decoded)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/gateway/config/ -race -count=1 -v`
Expected: FAIL — `undefined: Defaults`, `undefined: Overrides`.

- [ ] **Step 3: Write `config.go`**

```go
// Package config owns the gateway's immutable configuration snapshot:
// defaults ← environment ← admin overrides. A *Config is never mutated after
// Build; hot reconfiguration swaps the whole snapshot (see Holder).
package config

import (
	"errors"
	"fmt"
	"net/url"
)

// Server groups process-level settings.
type Server struct {
	Port                       int64
	StorageDir                 string // resolved by main; empty means "use the platform default"
	APIKeys                    []string
	AdminAPIKey                string
	DevshardsJSON              string
	DefaultModel               string
	RoutePrefix                string // empty means "use the build version"
	MaxConcurrentRuntimeBuilds int64
}

// Chain groups chain-facing endpoints.
type Chain struct {
	RESTBaseURL      string
	PublicAPIBaseURL string
	TxQueryBaseURL   string
	ChainID          string // empty = auto-discover from node_info
}

// Tx groups transaction parameters.
type Tx struct {
	FeeDenom       string
	FeeAmount      int64
	GasLimit       int64
	PollIntervalMS int64
	PollTimeoutMS  int64
}

// ModelTokenLimit is a per-model override of the output-token limits.
type ModelTokenLimit struct {
	DefaultMaxTokens int64 `json:"default_max_tokens"`
	MaxTokensCap     int64 `json:"max_tokens_cap"`
}

// Limits groups admission and participant-protection tuning.
type Limits struct {
	DefaultMaxTokens                       int64
	MaxTokensCap                           int64
	MaxConcurrentRequests                  int64
	MaxConcurrentRequestsPer10000Weight    float64
	PoCMaxConcurrentRequestsPer10000Weight float64
	MaxInputTokensInFlight                 int64 // 0 = unlimited
	AcquireWaitMS                          int64
	AIMDInitialWindow                      int64
	AIMDMaxWindow                          int64
	BreakerTripThreshold                   int64
	BreakerBaseOpenMS                      int64
	BreakerMaxOpenMS                       int64
	ModelTokenLimits                       map[string]ModelTokenLimit
	ModelAccess                            map[string]string // model → open|api_key|admin_only
}

// Modes groups operational switches.
type Modes struct {
	PoCMode             string // off|relaxed
	CapacityAwareLimits bool
	Disabled            bool
	DisabledMessage     string
	DisabledRedirectURL string
}

// Rotation groups escrow-rotation settings.
type Rotation struct {
	Enabled           bool
	SettlementEnabled bool
	PrePoCBlocks      int64
	ModelsJSON        string
}

// Cache groups response-cache settings.
type Cache struct {
	ChatCacheMaxBytes int64
}

// Capture groups the debug request-capture settings.
type Capture struct {
	Enabled                      bool
	Dir                          string // empty = <storageDir>/captured-requests
	ShortContentAttempts         bool
	ShortContentResponses        bool
	ShortContentMinOutputChunks  int64
	ShortContentMaxContentRatio  float64
	ShortContentResponseMaxBytes int64
}

// Stream groups streaming/classification bounds.
type Stream struct {
	DrainTimeoutSeconds         int64
	ClassifyMaxAttemptBytes     int64
	ClassifyMaxParticipantBytes int64
	ClassifyMaxGlobalBytes      int64
}

// Config is the complete immutable gateway configuration snapshot.
type Config struct {
	Server   Server
	Chain    Chain
	Tx       Tx
	Limits   Limits
	Modes    Modes
	Rotation Rotation
	Cache    Cache
	Capture  Capture
	Stream   Stream
}

// PoC mode values accepted in Modes.PoCMode.
const (
	PoCModeOff     = "off"
	PoCModeRelaxed = "relaxed"
)

// Model access values accepted in Limits.ModelAccess.
const (
	ModelAccessOpen      = "open"
	ModelAccessAPIKey    = "api_key"
	ModelAccessAdminOnly = "admin_only"
)

// Defaults returns the spec §12 default configuration.
func Defaults() Config {
	return Config{
		Server: Server{
			Port:                       8080,
			DefaultModel:               "Qwen/Qwen3-235B-A22B-Instruct-2507-FP8",
			MaxConcurrentRuntimeBuilds: 16,
		},
		Chain: Chain{
			RESTBaseURL:      "http://localhost:1317",
			PublicAPIBaseURL: "http://localhost:9000",
			TxQueryBaseURL:   "http://node1.gonka.ai:8000/chain-api",
		},
		Tx: Tx{
			FeeDenom:       "ngonka",
			FeeAmount:      1_000_000,
			GasLimit:       500_000,
			PollIntervalMS: 2_000,
			PollTimeoutMS:  45_000,
		},
		Limits: Limits{
			DefaultMaxTokens:                       3072,
			MaxTokensCap:                           4096,
			MaxConcurrentRequests:                  512,
			MaxConcurrentRequestsPer10000Weight:    5.0,
			PoCMaxConcurrentRequestsPer10000Weight: 10.0,
			MaxInputTokensInFlight:                 0,
			AcquireWaitMS:                          500,
			AIMDInitialWindow:                      4,
			AIMDMaxWindow:                          64,
			BreakerTripThreshold:                   3,
			BreakerBaseOpenMS:                      5_000,
			BreakerMaxOpenMS:                       300_000,
		},
		Modes: Modes{
			PoCMode: PoCModeOff,
		},
		Rotation: Rotation{
			PrePoCBlocks: 300,
		},
		Cache: Cache{
			ChatCacheMaxBytes: 268_435_456,
		},
		Capture: Capture{
			ShortContentMinOutputChunks:  1_000,
			ShortContentMaxContentRatio:  0.75,
			ShortContentResponseMaxBytes: 16_777_216,
		},
		Stream: Stream{
			DrainTimeoutSeconds:         2_400,
			ClassifyMaxAttemptBytes:     1_048_576,
			ClassifyMaxParticipantBytes: 10_485_760,
			ClassifyMaxGlobalBytes:      104_857_600,
		},
	}
}

// Validate reports every problem in the snapshot at once. Field names in
// messages use snake_case to match the admin-API/JSON spelling.
func (c *Config) Validate() error {
	var problems []error
	complain := func(format string, args ...any) {
		problems = append(problems, fmt.Errorf(format, args...))
	}

	if c.Server.Port < 1 || c.Server.Port > 65535 {
		complain("port: %d out of range 1..65535", c.Server.Port)
	}
	if c.Server.MaxConcurrentRuntimeBuilds < 1 {
		complain("max_concurrent_runtime_builds: %d must be >= 1", c.Server.MaxConcurrentRuntimeBuilds)
	}
	checkBaseURL := func(name, value string) {
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			complain("%s: %q is not an absolute URL", name, value)
		}
	}
	checkBaseURL("chain_rest", c.Chain.RESTBaseURL)
	checkBaseURL("public_api", c.Chain.PublicAPIBaseURL)
	checkBaseURL("tx_query_rest", c.Chain.TxQueryBaseURL)

	if c.Tx.FeeAmount < 0 {
		complain("tx_fee_amount: %d must be >= 0", c.Tx.FeeAmount)
	}
	if c.Tx.GasLimit < 1 {
		complain("tx_gas_limit: %d must be >= 1", c.Tx.GasLimit)
	}
	if c.Tx.PollIntervalMS < 1 {
		complain("tx_poll_interval_ms: %d must be >= 1", c.Tx.PollIntervalMS)
	}
	if c.Tx.PollTimeoutMS < c.Tx.PollIntervalMS {
		complain("tx_poll_timeout_ms: %d must be >= tx_poll_interval_ms %d", c.Tx.PollTimeoutMS, c.Tx.PollIntervalMS)
	}

	if c.Limits.DefaultMaxTokens < 1 {
		complain("default_max_tokens: %d must be >= 1", c.Limits.DefaultMaxTokens)
	}
	if c.Limits.MaxTokensCap < c.Limits.DefaultMaxTokens {
		complain("max_tokens_cap: %d must be >= default_max_tokens %d", c.Limits.MaxTokensCap, c.Limits.DefaultMaxTokens)
	}
	if c.Limits.MaxConcurrentRequests < 1 {
		complain("max_concurrent_requests: %d must be >= 1", c.Limits.MaxConcurrentRequests)
	}
	if c.Limits.MaxConcurrentRequestsPer10000Weight < 0 {
		complain("max_concurrent_requests_per_10000_weight: %v must be >= 0", c.Limits.MaxConcurrentRequestsPer10000Weight)
	}
	if c.Limits.PoCMaxConcurrentRequestsPer10000Weight < 0 {
		complain("poc_max_concurrent_requests_per_10000_weight: %v must be >= 0", c.Limits.PoCMaxConcurrentRequestsPer10000Weight)
	}
	if c.Limits.MaxInputTokensInFlight < 0 {
		complain("max_input_tokens_in_flight: %d must be >= 0", c.Limits.MaxInputTokensInFlight)
	}
	if c.Limits.AcquireWaitMS < 0 {
		complain("acquire_wait_ms: %d must be >= 0", c.Limits.AcquireWaitMS)
	}
	if c.Limits.AIMDInitialWindow < 1 {
		complain("aimd_initial_window: %d must be >= 1", c.Limits.AIMDInitialWindow)
	}
	if c.Limits.AIMDMaxWindow < c.Limits.AIMDInitialWindow {
		complain("aimd_max_window: %d must be >= aimd_initial_window %d", c.Limits.AIMDMaxWindow, c.Limits.AIMDInitialWindow)
	}
	if c.Limits.BreakerTripThreshold < 1 {
		complain("breaker_trip_threshold: %d must be >= 1", c.Limits.BreakerTripThreshold)
	}
	if c.Limits.BreakerBaseOpenMS < 1 {
		complain("breaker_base_open_ms: %d must be >= 1", c.Limits.BreakerBaseOpenMS)
	}
	if c.Limits.BreakerMaxOpenMS < c.Limits.BreakerBaseOpenMS {
		complain("breaker_max_open_ms: %d must be >= breaker_base_open_ms %d", c.Limits.BreakerMaxOpenMS, c.Limits.BreakerBaseOpenMS)
	}
	for model, access := range c.Limits.ModelAccess {
		if access != ModelAccessOpen && access != ModelAccessAPIKey && access != ModelAccessAdminOnly {
			complain("model_access[%s]: %q is not open, api_key or admin_only", model, access)
		}
	}
	for model, limit := range c.Limits.ModelTokenLimits {
		if limit.DefaultMaxTokens < 1 || limit.MaxTokensCap < limit.DefaultMaxTokens {
			complain("model_token_limits[%s]: default %d / cap %d invalid", model, limit.DefaultMaxTokens, limit.MaxTokensCap)
		}
	}

	if c.Modes.PoCMode != PoCModeOff && c.Modes.PoCMode != PoCModeRelaxed {
		complain("poc_mode: %q is not %q or %q", c.Modes.PoCMode, PoCModeOff, PoCModeRelaxed)
	}
	if c.Rotation.PrePoCBlocks < 0 {
		complain("rotation_pre_poc_blocks: %d must be >= 0", c.Rotation.PrePoCBlocks)
	}
	if c.Cache.ChatCacheMaxBytes < 0 {
		complain("chat_cache_max_bytes: %d must be >= 0", c.Cache.ChatCacheMaxBytes)
	}
	if c.Capture.ShortContentMinOutputChunks < 0 {
		complain("capture_short_content_min_output_chunks: %d must be >= 0", c.Capture.ShortContentMinOutputChunks)
	}
	if c.Capture.ShortContentMaxContentRatio <= 0 || c.Capture.ShortContentMaxContentRatio > 1 {
		complain("capture_short_content_max_content_ratio: %v must be in (0, 1]", c.Capture.ShortContentMaxContentRatio)
	}
	if c.Capture.ShortContentResponseMaxBytes < 1 {
		complain("capture_short_content_response_max_bytes: %d must be >= 1", c.Capture.ShortContentResponseMaxBytes)
	}
	if c.Stream.DrainTimeoutSeconds < 1 {
		complain("drain_timeout_seconds: %d must be >= 1", c.Stream.DrainTimeoutSeconds)
	}
	if c.Stream.ClassifyMaxAttemptBytes < 1 {
		complain("classify_max_attempt_bytes: %d must be >= 1", c.Stream.ClassifyMaxAttemptBytes)
	}
	if c.Stream.ClassifyMaxParticipantBytes < c.Stream.ClassifyMaxAttemptBytes {
		complain("classify_max_participant_bytes: %d must be >= classify_max_attempt_bytes %d", c.Stream.ClassifyMaxParticipantBytes, c.Stream.ClassifyMaxAttemptBytes)
	}
	if c.Stream.ClassifyMaxGlobalBytes < c.Stream.ClassifyMaxParticipantBytes {
		complain("classify_max_global_bytes: %d must be >= classify_max_participant_bytes %d", c.Stream.ClassifyMaxGlobalBytes, c.Stream.ClassifyMaxParticipantBytes)
	}

	if len(problems) > 0 {
		return fmt.Errorf("invalid configuration: %w", errors.Join(problems...))
	}
	return nil
}
```

- [ ] **Step 4: Write `overrides.go`**

```go
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Overrides is the admin-tunable subset of Config, persisted by the store and
// merged over env by Build. Nil field = not overridden. Later phases extend
// this struct as their settings become admin-tunable.
type Overrides struct {
	DefaultMaxTokens                       *int64                     `json:"default_max_tokens,omitempty"`
	MaxTokensCap                           *int64                     `json:"max_tokens_cap,omitempty"`
	MaxConcurrentRequests                  *int64                     `json:"max_concurrent_requests,omitempty"`
	MaxConcurrentRequestsPer10000Weight    *float64                   `json:"max_concurrent_requests_per_10000_weight,omitempty"`
	PoCMaxConcurrentRequestsPer10000Weight *float64                   `json:"poc_max_concurrent_requests_per_10000_weight,omitempty"`
	MaxInputTokensInFlight                 *int64                     `json:"max_input_tokens_in_flight,omitempty"`
	AcquireWaitMS                          *int64                     `json:"acquire_wait_ms,omitempty"`
	AIMDInitialWindow                      *int64                     `json:"aimd_initial_window,omitempty"`
	AIMDMaxWindow                          *int64                     `json:"aimd_max_window,omitempty"`
	BreakerTripThreshold                   *int64                     `json:"breaker_trip_threshold,omitempty"`
	BreakerBaseOpenMS                      *int64                     `json:"breaker_base_open_ms,omitempty"`
	BreakerMaxOpenMS                       *int64                     `json:"breaker_max_open_ms,omitempty"`
	ModelTokenLimits                       map[string]ModelTokenLimit `json:"model_token_limits,omitempty"`
	ModelAccess                            map[string]string          `json:"model_access,omitempty"`
	Disabled                               *bool                      `json:"disabled,omitempty"`
	DisabledMessage                        *string                    `json:"disabled_message,omitempty"`
	DisabledRedirectURL                    *string                    `json:"disabled_redirect_url,omitempty"`
	RotationEnabled                        *bool                      `json:"rotation_enabled,omitempty"`
	RotationSettlementEnabled              *bool                      `json:"rotation_settlement_enabled,omitempty"`
	RotationPrePoCBlocks                   *int64                     `json:"rotation_pre_poc_blocks,omitempty"`
	RotationModelsJSON                     *string                    `json:"rotation_models_json,omitempty"`
}

// ParseOverrides decodes admin-supplied override JSON. Unknown fields are an
// error: a typo in an admin PUT must be reported, never silently ignored.
func ParseOverrides(raw []byte) (Overrides, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var overrides Overrides
	if err := decoder.Decode(&overrides); err != nil {
		return Overrides{}, fmt.Errorf("parsing overrides: %w", err)
	}
	return overrides, nil
}

// MarshalJSONBytes encodes the overrides for persistence.
func (o Overrides) MarshalJSONBytes() ([]byte, error) {
	encoded, err := json.Marshal(o)
	if err != nil {
		return nil, fmt.Errorf("encoding overrides: %w", err)
	}
	return encoded, nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./cmd/gateway/config/ -race -count=1 -v`
Expected: PASS (6 tests). If the `strings` guard line was needed to compile Step 3 before Step 4 existed, delete it now and re-run.

- [ ] **Step 6: Format, vet, commit checkpoint**

Run: `gofmt -l ./cmd/gateway/ && go vet ./cmd/gateway/...`
Suggested commit message: `feat(gateway): config package — snapshot struct, defaults, validation, overrides`
(Executor: stop and hand off to Daniil.)

---

### Task 3: `config.Build` merge + `config.Holder` atomic snapshot

**Files:**
- Create: `cmd/gateway/config/build.go`
- Create: `cmd/gateway/config/holder.go`
- Test: `cmd/gateway/config/build_test.go`, `cmd/gateway/config/holder_test.go`

**Interfaces:**
- Consumes: `env.Values` (Task 1), `Config`/`Defaults`/`Validate`/`Overrides` (Task 2).
- Produces (used by `main.go` Task 6 and the api phase later):
  - `func Build(values env.Values, overrides Overrides) (*Config, error)` — merge defaults ← env ← overrides, then Validate.
  - `type Holder struct{...}`; `func NewHolder(initial *Config) *Holder`; `(h *Holder) Load() *Config`; `(h *Holder) Swap(next *Config)`; `(h *Holder) Subscribe(callback func(*Config)) (cancel func())`.

- [ ] **Step 1: Write the failing tests**

`cmd/gateway/config/build_test.go`:

```go
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
		Port:             int64Pointer(9000),          // env overrides default 8080
		DefaultMaxTokens: int64Pointer(2000),          // env sets 2000...
		ChainREST:        stringPointer("http://chain.test:1317"),
	}
	overrides := Overrides{
		DefaultMaxTokens: int64Pointer(1500), // ...but admin override wins over env
		Disabled:         boolPointer(true),
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

func TestBuildPerWeightFloatFromEnv(t *testing.T) {
	values := env.Values{MaxConcurrentRequestsPer10000Weight: float64Pointer(2.5)}
	configuration, err := Build(values, Overrides{})
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	if configuration.Limits.MaxConcurrentRequestsPer10000Weight != 2.5 {
		t.Errorf("per-weight = %v, want 2.5", configuration.Limits.MaxConcurrentRequestsPer10000Weight)
	}
}
```

`cmd/gateway/config/holder_test.go`:

```go
package config

import (
	"sync"
	"testing"
)

func TestHolderLoadReturnsInitialSnapshot(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)
	if holder.Load() != &initial {
		t.Fatal("Load() must return the exact pointer given to NewHolder")
	}
}

func TestHolderSwapNotifiesSubscribersWithNewSnapshot(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)

	var mutex sync.Mutex
	var seen []*Config
	cancel := holder.Subscribe(func(next *Config) {
		mutex.Lock()
		defer mutex.Unlock()
		seen = append(seen, next)
	})
	defer cancel()

	next := Defaults()
	next.Server.Port = 9999
	holder.Swap(&next)

	if holder.Load().Server.Port != 9999 {
		t.Fatalf("Load().Server.Port = %d, want 9999 after Swap", holder.Load().Server.Port)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(seen) != 1 || seen[0] != &next {
		t.Fatalf("subscriber saw %v, want exactly the swapped snapshot once", seen)
	}
}

func TestHolderCancelledSubscriberIsNotNotified(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)

	notified := false
	cancel := holder.Subscribe(func(*Config) { notified = true })
	cancel()

	next := Defaults()
	holder.Swap(&next)
	if notified {
		t.Fatal("cancelled subscriber must not be notified")
	}
}

func TestHolderConcurrentLoadAndSwapIsRaceFree(t *testing.T) {
	initial := Defaults()
	holder := NewHolder(&initial)

	var waitGroup sync.WaitGroup
	for range 4 {
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()
			for range 500 {
				snapshot := Defaults()
				holder.Swap(&snapshot)
			}
		}()
		go func() {
			defer waitGroup.Done()
			for range 500 {
				_ = holder.Load().Server.Port
			}
		}()
	}
	waitGroup.Wait()
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/gateway/config/ -race -count=1 -v`
Expected: FAIL — `undefined: Build`, `undefined: NewHolder`.

- [ ] **Step 3: Write `build.go`**

```go
package config

import (
	"strings"

	"devshard/cmd/gateway/env"
)

// Build produces the immutable snapshot: Defaults() ← env values ← admin
// overrides, validated as a whole. The returned *Config must never be mutated.
func Build(values env.Values, overrides Overrides) (*Config, error) {
	configuration := Defaults()

	applyInt := func(target *int64, source *int64) {
		if source != nil {
			*target = *source
		}
	}
	applyFloat := func(target *float64, source *float64) {
		if source != nil {
			*target = *source
		}
	}
	applyString := func(target *string, source *string) {
		if source != nil {
			*target = *source
		}
	}
	applyBool := func(target *bool, source *bool) {
		if source != nil {
			*target = *source
		}
	}

	// Environment layer.
	applyInt(&configuration.Server.Port, values.Port)
	applyString(&configuration.Server.StorageDir, values.StorageDir)
	if values.APIKeys != nil {
		configuration.Server.APIKeys = splitCommaSeparated(*values.APIKeys)
	}
	applyString(&configuration.Server.AdminAPIKey, values.AdminAPIKey)
	applyString(&configuration.Server.DevshardsJSON, values.DevshardsJSON)
	applyString(&configuration.Server.DefaultModel, values.DefaultModel)
	applyString(&configuration.Server.RoutePrefix, values.RoutePrefix)
	applyInt(&configuration.Server.MaxConcurrentRuntimeBuilds, values.MaxConcurrentRuntimeBuilds)

	applyString(&configuration.Chain.RESTBaseURL, values.ChainREST)
	applyString(&configuration.Chain.PublicAPIBaseURL, values.PublicAPI)
	applyString(&configuration.Chain.ChainID, values.ChainID)
	applyString(&configuration.Chain.TxQueryBaseURL, values.TxQueryREST)
	applyString(&configuration.Tx.FeeDenom, values.TxFeeDenom)
	applyInt(&configuration.Tx.FeeAmount, values.TxFeeAmount)
	applyInt(&configuration.Tx.GasLimit, values.TxGasLimit)
	applyInt(&configuration.Tx.PollIntervalMS, values.TxPollIntervalMS)
	applyInt(&configuration.Tx.PollTimeoutMS, values.TxPollTimeoutMS)

	applyInt(&configuration.Limits.DefaultMaxTokens, values.DefaultMaxTokens)
	applyInt(&configuration.Limits.MaxTokensCap, values.MaxTokensCap)
	applyInt(&configuration.Limits.MaxConcurrentRequests, values.MaxConcurrentRequests)
	applyFloat(&configuration.Limits.MaxConcurrentRequestsPer10000Weight, values.MaxConcurrentRequestsPer10000Weight)
	applyFloat(&configuration.Limits.PoCMaxConcurrentRequestsPer10000Weight, values.PoCMaxConcurrentRequestsPer10000Weight)
	applyInt(&configuration.Limits.MaxInputTokensInFlight, values.MaxInputTokensInFlight)
	applyInt(&configuration.Limits.AcquireWaitMS, values.AcquireWaitMS)
	applyInt(&configuration.Limits.AIMDInitialWindow, values.AIMDInitialWindow)
	applyInt(&configuration.Limits.AIMDMaxWindow, values.AIMDMaxWindow)
	applyInt(&configuration.Limits.BreakerTripThreshold, values.BreakerTripThreshold)
	applyInt(&configuration.Limits.BreakerBaseOpenMS, values.BreakerBaseOpenMS)
	applyInt(&configuration.Limits.BreakerMaxOpenMS, values.BreakerMaxOpenMS)

	applyString(&configuration.Modes.PoCMode, values.PoCMode)
	applyBool(&configuration.Modes.CapacityAwareLimits, values.CapacityAwareLimits)
	applyBool(&configuration.Modes.Disabled, values.Disabled)
	applyString(&configuration.Modes.DisabledMessage, values.DisabledMessage)
	applyString(&configuration.Modes.DisabledRedirectURL, values.DisabledRedirectURL)

	applyBool(&configuration.Rotation.Enabled, values.RotationEnabled)
	applyBool(&configuration.Rotation.SettlementEnabled, values.RotationSettlementEnabled)
	applyInt(&configuration.Rotation.PrePoCBlocks, values.RotationPrePoCBlocks)
	applyString(&configuration.Rotation.ModelsJSON, values.RotationModelsJSON)

	applyInt(&configuration.Cache.ChatCacheMaxBytes, values.ChatCacheMaxBytes)
	applyInt(&configuration.Stream.DrainTimeoutSeconds, values.DrainTimeoutSeconds)
	applyInt(&configuration.Stream.ClassifyMaxAttemptBytes, values.ClassifyMaxAttemptBytes)
	applyInt(&configuration.Stream.ClassifyMaxParticipantBytes, values.ClassifyMaxParticipantBytes)
	applyInt(&configuration.Stream.ClassifyMaxGlobalBytes, values.ClassifyMaxGlobalBytes)

	applyBool(&configuration.Capture.Enabled, values.CaptureEnabled)
	applyString(&configuration.Capture.Dir, values.CaptureDir)
	applyBool(&configuration.Capture.ShortContentAttempts, values.CaptureShortContentAttempts)
	applyBool(&configuration.Capture.ShortContentResponses, values.CaptureShortContentResponses)
	applyInt(&configuration.Capture.ShortContentMinOutputChunks, values.CaptureShortContentMinOutputChunks)
	applyFloat(&configuration.Capture.ShortContentMaxContentRatio, values.CaptureShortContentMaxContentRatio)
	applyInt(&configuration.Capture.ShortContentResponseMaxBytes, values.CaptureShortContentResponseMaxBytes)

	// Admin-override layer (wins over env).
	applyInt(&configuration.Limits.DefaultMaxTokens, overrides.DefaultMaxTokens)
	applyInt(&configuration.Limits.MaxTokensCap, overrides.MaxTokensCap)
	applyInt(&configuration.Limits.MaxConcurrentRequests, overrides.MaxConcurrentRequests)
	applyFloat(&configuration.Limits.MaxConcurrentRequestsPer10000Weight, overrides.MaxConcurrentRequestsPer10000Weight)
	applyFloat(&configuration.Limits.PoCMaxConcurrentRequestsPer10000Weight, overrides.PoCMaxConcurrentRequestsPer10000Weight)
	applyInt(&configuration.Limits.MaxInputTokensInFlight, overrides.MaxInputTokensInFlight)
	applyInt(&configuration.Limits.AcquireWaitMS, overrides.AcquireWaitMS)
	applyInt(&configuration.Limits.AIMDInitialWindow, overrides.AIMDInitialWindow)
	applyInt(&configuration.Limits.AIMDMaxWindow, overrides.AIMDMaxWindow)
	applyInt(&configuration.Limits.BreakerTripThreshold, overrides.BreakerTripThreshold)
	applyInt(&configuration.Limits.BreakerBaseOpenMS, overrides.BreakerBaseOpenMS)
	applyInt(&configuration.Limits.BreakerMaxOpenMS, overrides.BreakerMaxOpenMS)
	if overrides.ModelTokenLimits != nil {
		configuration.Limits.ModelTokenLimits = overrides.ModelTokenLimits
	}
	if overrides.ModelAccess != nil {
		configuration.Limits.ModelAccess = overrides.ModelAccess
	}
	applyBool(&configuration.Modes.Disabled, overrides.Disabled)
	applyString(&configuration.Modes.DisabledMessage, overrides.DisabledMessage)
	applyString(&configuration.Modes.DisabledRedirectURL, overrides.DisabledRedirectURL)
	applyBool(&configuration.Rotation.Enabled, overrides.RotationEnabled)
	applyBool(&configuration.Rotation.SettlementEnabled, overrides.RotationSettlementEnabled)
	applyInt(&configuration.Rotation.PrePoCBlocks, overrides.RotationPrePoCBlocks)
	applyString(&configuration.Rotation.ModelsJSON, overrides.RotationModelsJSON)

	if err := configuration.Validate(); err != nil {
		return nil, err
	}
	return &configuration, nil
}

func splitCommaSeparated(raw string) []string {
	parts := strings.Split(raw, ",")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}
```

- [ ] **Step 4: Write `holder.go`**

```go
package config

import (
	"sync"
	"sync/atomic"
)

// Holder publishes the live configuration snapshot. Readers call Load on
// every use (never cache across requests); reconfiguration calls Swap with a
// freshly Built snapshot. Subscribers run synchronously inside Swap, so they
// must be fast and must not call back into the Holder.
type Holder struct {
	current     atomic.Pointer[Config]
	mutex       sync.Mutex
	subscribers map[int]func(*Config)
	nextID      int
}

// NewHolder creates a Holder with the initial snapshot.
func NewHolder(initial *Config) *Holder {
	holder := &Holder{subscribers: make(map[int]func(*Config))}
	holder.current.Store(initial)
	return holder
}

// Load returns the current snapshot. The result is shared and immutable.
func (h *Holder) Load() *Config { return h.current.Load() }

// Swap publishes a new snapshot and notifies subscribers in registration
// order (map iteration order is fine: subscribers must be order-independent).
func (h *Holder) Swap(next *Config) {
	h.current.Store(next)
	h.mutex.Lock()
	callbacks := make([]func(*Config), 0, len(h.subscribers))
	for _, callback := range h.subscribers {
		callbacks = append(callbacks, callback)
	}
	h.mutex.Unlock()
	for _, callback := range callbacks {
		callback(next)
	}
}

// Subscribe registers a callback invoked on every Swap. The returned cancel
// removes the subscription.
func (h *Holder) Subscribe(callback func(*Config)) (cancel func()) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	id := h.nextID
	h.nextID++
	h.subscribers[id] = callback
	return func() {
		h.mutex.Lock()
		defer h.mutex.Unlock()
		delete(h.subscribers, id)
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./cmd/gateway/config/ -race -count=1 -v`
Expected: PASS (all config tests, including the concurrent Holder test under -race).

- [ ] **Step 6: Format, vet, commit checkpoint**

Run: `gofmt -l ./cmd/gateway/ && go vet ./cmd/gateway/...`
Suggested commit message: `feat(gateway): config Build merge and atomic snapshot Holder`
(Executor: stop and hand off to Daniil.)

---

### Task 4: `store/` — SQLite gateway.db, overrides + devshard registry

**Files:**
- Create: `cmd/gateway/store/store.go`
- Create: `cmd/gateway/store/overrides.go`
- Create: `cmd/gateway/store/devshards.go`
- Test: `cmd/gateway/store/store_test.go`

**Interfaces:**
- Consumes: `config.Overrides`, `config.ParseOverrides`, `(config.Overrides).MarshalJSONBytes` (Task 2).
- Produces (used by Task 6 and later phases):
  - `func Open(storageDir string) (*Store, error)` — creates the directory, opens `<dir>/gateway.db`, applies pragmas, runs migrations.
  - `(s *Store) Close() error`
  - `(s *Store) LoadOverrides(ctx context.Context) (config.Overrides, error)` — zero value when none saved.
  - `(s *Store) SaveOverrides(ctx context.Context, overrides config.Overrides) error`
  - `type DevshardRecord struct { EscrowID, PrivateKeyEnv, Model string; Active bool; RotationRole string; RotationEpoch int64; SettlementPending bool }` — note: **no raw private keys in the database**, only the env-var name (spec §12 indirection).
  - `(s *Store) UpsertDevshard(ctx, DevshardRecord) error`, `(s *Store) ListDevshards(ctx) ([]DevshardRecord, error)` (ordered by `escrow_id`), `(s *Store) SetDevshardActive(ctx, escrowID string, active bool) error`, `(s *Store) SetDevshardSettlementPending(ctx, escrowID string, pending bool) error`, `(s *Store) DeleteDevshard(ctx, escrowID string) error`.
  - Sentinel: `var ErrDevshardNotFound = errors.New("devshard not found")` (returned by the two Set* methods and Delete when no row matches).

- [ ] **Step 1: Write the failing tests**

`cmd/gateway/store/store_test.go`:

```go
package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"devshard/cmd/gateway/config"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	testStore, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open(): %v", err)
	}
	t.Cleanup(func() {
		if err := testStore.Close(); err != nil {
			t.Errorf("Close(): %v", err)
		}
	})
	return testStore
}

func TestOpenCreatesDatabaseFileAndDirectory(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "nested", "storage")
	testStore, err := Open(baseDir)
	if err != nil {
		t.Fatalf("Open() with missing directory: %v", err)
	}
	defer testStore.Close()
	if _, err := os.Stat(filepath.Join(baseDir, "gateway.db")); err != nil {
		t.Fatalf("gateway.db not created: %v", err)
	}
}

func TestOverridesRoundTripAndEmptyLoad(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	empty, err := testStore.LoadOverrides(ctx)
	if err != nil {
		t.Fatalf("LoadOverrides() on fresh store: %v", err)
	}
	if empty.DefaultMaxTokens != nil {
		t.Fatal("fresh store must return zero-value overrides")
	}

	maxTokens := int64(1234)
	saved := config.Overrides{DefaultMaxTokens: &maxTokens}
	if err := testStore.SaveOverrides(ctx, saved); err != nil {
		t.Fatalf("SaveOverrides(): %v", err)
	}
	loaded, err := testStore.LoadOverrides(ctx)
	if err != nil {
		t.Fatalf("LoadOverrides() after save: %v", err)
	}
	if loaded.DefaultMaxTokens == nil || *loaded.DefaultMaxTokens != 1234 {
		t.Fatalf("loaded overrides = %+v, want DefaultMaxTokens 1234", loaded)
	}

	// Second save replaces, not appends.
	newTokens := int64(999)
	if err := testStore.SaveOverrides(ctx, config.Overrides{DefaultMaxTokens: &newTokens}); err != nil {
		t.Fatalf("second SaveOverrides(): %v", err)
	}
	replaced, err := testStore.LoadOverrides(ctx)
	if err != nil {
		t.Fatalf("LoadOverrides() after replace: %v", err)
	}
	if replaced.DefaultMaxTokens == nil || *replaced.DefaultMaxTokens != 999 {
		t.Fatalf("replaced overrides = %+v, want 999", replaced)
	}
}

func TestDevshardCRUDLifecycle(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	record := DevshardRecord{
		EscrowID:      "escrow-7",
		PrivateKeyEnv: "GATEWAY_KEY_ESCROW_7",
		Model:         "model-a",
		Active:        true,
		RotationRole:  "regular",
		RotationEpoch: 12,
	}
	if err := testStore.UpsertDevshard(ctx, record); err != nil {
		t.Fatalf("UpsertDevshard(): %v", err)
	}

	listed, err := testStore.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards(): %v", err)
	}
	if len(listed) != 1 || listed[0] != record {
		t.Fatalf("ListDevshards() = %+v, want exactly the upserted record", listed)
	}

	if err := testStore.SetDevshardActive(ctx, "escrow-7", false); err != nil {
		t.Fatalf("SetDevshardActive(): %v", err)
	}
	if err := testStore.SetDevshardSettlementPending(ctx, "escrow-7", true); err != nil {
		t.Fatalf("SetDevshardSettlementPending(): %v", err)
	}
	listed, err = testStore.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards() after updates: %v", err)
	}
	if listed[0].Active || !listed[0].SettlementPending {
		t.Fatalf("after updates got %+v, want inactive + settlement pending", listed[0])
	}

	// Upsert replaces fields for the same escrow.
	record.Model = "model-b"
	record.Active = false
	if err := testStore.UpsertDevshard(ctx, record); err != nil {
		t.Fatalf("UpsertDevshard() replace: %v", err)
	}
	listed, _ = testStore.ListDevshards(ctx)
	if listed[0].Model != "model-b" {
		t.Fatalf("after replace got model %q, want model-b", listed[0].Model)
	}

	if err := testStore.DeleteDevshard(ctx, "escrow-7"); err != nil {
		t.Fatalf("DeleteDevshard(): %v", err)
	}
	listed, _ = testStore.ListDevshards(ctx)
	if len(listed) != 0 {
		t.Fatalf("after delete list = %+v, want empty", listed)
	}
}

func TestMissingDevshardReturnsSentinel(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()
	if err := testStore.SetDevshardActive(ctx, "ghost", true); !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("SetDevshardActive(ghost) = %v, want ErrDevshardNotFound", err)
	}
	if err := testStore.SetDevshardSettlementPending(ctx, "ghost", true); !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("SetDevshardSettlementPending(ghost) = %v, want ErrDevshardNotFound", err)
	}
	if err := testStore.DeleteDevshard(ctx, "ghost"); !errors.Is(err, ErrDevshardNotFound) {
		t.Fatalf("DeleteDevshard(ghost) = %v, want ErrDevshardNotFound", err)
	}
}

func TestOpenIsIdempotentAcrossRestarts(t *testing.T) {
	baseDir := t.TempDir()
	first, err := Open(baseDir)
	if err != nil {
		t.Fatalf("first Open(): %v", err)
	}
	ctx := context.Background()
	if err := first.UpsertDevshard(ctx, DevshardRecord{EscrowID: "escrow-1", Model: "m", RotationRole: "regular"}); err != nil {
		t.Fatalf("UpsertDevshard(): %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	second, err := Open(baseDir) // migrations must be re-runnable
	if err != nil {
		t.Fatalf("second Open(): %v", err)
	}
	defer second.Close()
	listed, err := second.ListDevshards(ctx)
	if err != nil {
		t.Fatalf("ListDevshards() after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].EscrowID != "escrow-1" {
		t.Fatalf("data lost across reopen: %+v", listed)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./cmd/gateway/store/ -race -count=1 -v`
Expected: FAIL — `undefined: Open`.

- [ ] **Step 3: Write `store.go`**

```go
// Package store persists gateway control-plane state in SQLite
// (<storageDir>/gateway.db): admin config overrides and the devshard
// registry. Later phases add escrow intent commitments, rotation status and
// suspicious hosts here, and a separate perf.db.
package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// Store wraps the gateway.db handle. Access is serialized via a single
// connection; contention waits on busy_timeout instead of failing.
type Store struct {
	db *sql.DB
}

// gatewayDatabaseFileName is the control-plane database inside storageDir.
const gatewayDatabaseFileName = "gateway.db"

// migrations run in order inside one transaction per version; the schema
// version table records progress so Open is idempotent.
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS config_overrides (
		id INTEGER PRIMARY KEY CHECK (id = 1),
		overrides_json TEXT NOT NULL,
		updated_at TEXT NOT NULL DEFAULT (datetime('now'))
	);
	CREATE TABLE IF NOT EXISTS devshards (
		escrow_id TEXT PRIMARY KEY,
		private_key_env TEXT NOT NULL DEFAULT '',
		model TEXT NOT NULL,
		active INTEGER NOT NULL DEFAULT 1,
		rotation_role TEXT NOT NULL DEFAULT 'regular',
		rotation_epoch INTEGER NOT NULL DEFAULT 0,
		settlement_pending INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL DEFAULT (datetime('now')),
		updated_at TEXT NOT NULL DEFAULT (datetime('now'))
	);`,
}

// Open creates storageDir if needed, opens gateway.db and migrates it.
func Open(storageDir string) (*Store, error) {
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating storage dir: %w", err)
	}
	databasePath := filepath.Join(storageDir, gatewayDatabaseFileName)
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		return nil, fmt.Errorf("opening gateway store: %w", err)
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("applying pragma %q: %w", pragma, err)
		}
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func migrate(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)`); err != nil {
		return fmt.Errorf("creating schema_version: %w", err)
	}
	var currentVersion int
	row := db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_version`)
	if err := row.Scan(&currentVersion); err != nil {
		return fmt.Errorf("reading schema version: %w", err)
	}
	for index := currentVersion; index < len(migrations); index++ {
		transaction, err := db.Begin()
		if err != nil {
			return fmt.Errorf("beginning migration %d: %w", index+1, err)
		}
		if _, err := transaction.Exec(migrations[index]); err != nil {
			transaction.Rollback()
			return fmt.Errorf("applying migration %d: %w", index+1, err)
		}
		if _, err := transaction.Exec(`INSERT INTO schema_version (version) VALUES (?)`, index+1); err != nil {
			transaction.Rollback()
			return fmt.Errorf("recording migration %d: %w", index+1, err)
		}
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("committing migration %d: %w", index+1, err)
		}
	}
	return nil
}

// Close releases the database handle.
func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing gateway store: %w", err)
	}
	return nil
}
```

- [ ] **Step 4: Write `overrides.go` and `devshards.go`**

`cmd/gateway/store/overrides.go`:

```go
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"devshard/cmd/gateway/config"
)

// LoadOverrides returns the persisted admin overrides, or the zero value when
// none were ever saved.
func (s *Store) LoadOverrides(ctx context.Context) (config.Overrides, error) {
	var raw string
	row := s.db.QueryRowContext(ctx, `SELECT overrides_json FROM config_overrides WHERE id = 1`)
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return config.Overrides{}, nil
		}
		return config.Overrides{}, fmt.Errorf("loading overrides: %w", err)
	}
	overrides, err := config.ParseOverrides([]byte(raw))
	if err != nil {
		return config.Overrides{}, fmt.Errorf("loading overrides: %w", err)
	}
	return overrides, nil
}

// SaveOverrides replaces the persisted admin overrides.
func (s *Store) SaveOverrides(ctx context.Context, overrides config.Overrides) error {
	encoded, err := overrides.MarshalJSONBytes()
	if err != nil {
		return fmt.Errorf("saving overrides: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO config_overrides (id, overrides_json, updated_at) VALUES (1, ?, datetime('now'))
		ON CONFLICT(id) DO UPDATE SET overrides_json = excluded.overrides_json, updated_at = excluded.updated_at`,
		string(encoded))
	if err != nil {
		return fmt.Errorf("saving overrides: %w", err)
	}
	return nil
}
```

`cmd/gateway/store/devshards.go`:

```go
package store

import (
	"context"
	"errors"
	"fmt"
)

// ErrDevshardNotFound is returned by updates/deletes that match no row.
var ErrDevshardNotFound = errors.New("devshard not found")

// DevshardRecord is one row of the devshard registry. Private keys are never
// stored — only the name of the environment variable that holds the key.
type DevshardRecord struct {
	EscrowID          string
	PrivateKeyEnv     string
	Model             string
	Active            bool
	RotationRole      string
	RotationEpoch     int64
	SettlementPending bool
}

// UpsertDevshard inserts or fully replaces the record for its escrow.
func (s *Store) UpsertDevshard(ctx context.Context, record DevshardRecord) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO devshards (escrow_id, private_key_env, model, active, rotation_role, rotation_epoch, settlement_pending)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(escrow_id) DO UPDATE SET
			private_key_env = excluded.private_key_env,
			model = excluded.model,
			active = excluded.active,
			rotation_role = excluded.rotation_role,
			rotation_epoch = excluded.rotation_epoch,
			settlement_pending = excluded.settlement_pending,
			updated_at = datetime('now')`,
		record.EscrowID, record.PrivateKeyEnv, record.Model, record.Active,
		record.RotationRole, record.RotationEpoch, record.SettlementPending)
	if err != nil {
		return fmt.Errorf("upserting devshard %s: %w", record.EscrowID, err)
	}
	return nil
}

// ListDevshards returns every record ordered by escrow id.
func (s *Store) ListDevshards(ctx context.Context) ([]DevshardRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT escrow_id, private_key_env, model, active, rotation_role, rotation_epoch, settlement_pending
		FROM devshards ORDER BY escrow_id`)
	if err != nil {
		return nil, fmt.Errorf("listing devshards: %w", err)
	}
	defer rows.Close()
	var records []DevshardRecord
	for rows.Next() {
		var record DevshardRecord
		if err := rows.Scan(&record.EscrowID, &record.PrivateKeyEnv, &record.Model,
			&record.Active, &record.RotationRole, &record.RotationEpoch, &record.SettlementPending); err != nil {
			return nil, fmt.Errorf("scanning devshard row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating devshards: %w", err)
	}
	return records, nil
}

// SetDevshardActive flips the active flag.
func (s *Store) SetDevshardActive(ctx context.Context, escrowID string, active bool) error {
	return s.updateDevshardFlag(ctx, `UPDATE devshards SET active = ?, updated_at = datetime('now') WHERE escrow_id = ?`, active, escrowID)
}

// SetDevshardSettlementPending flips the settlement_pending flag.
func (s *Store) SetDevshardSettlementPending(ctx context.Context, escrowID string, pending bool) error {
	return s.updateDevshardFlag(ctx, `UPDATE devshards SET settlement_pending = ?, updated_at = datetime('now') WHERE escrow_id = ?`, pending, escrowID)
}

// DeleteDevshard removes the record.
func (s *Store) DeleteDevshard(ctx context.Context, escrowID string) error {
	result, err := s.db.ExecContext(ctx, `DELETE FROM devshards WHERE escrow_id = ?`, escrowID)
	if err != nil {
		return fmt.Errorf("deleting devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func (s *Store) updateDevshardFlag(ctx context.Context, query string, flagValue bool, escrowID string) error {
	result, err := s.db.ExecContext(ctx, query, flagValue, escrowID)
	if err != nil {
		return fmt.Errorf("updating devshard %s: %w", escrowID, err)
	}
	return requireOneRow(result, escrowID)
}

func requireOneRow(result interface{ RowsAffected() (int64, error) }, escrowID string) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking affected rows for %s: %w", escrowID, err)
	}
	if affected == 0 {
		return fmt.Errorf("%s: %w", escrowID, ErrDevshardNotFound)
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./cmd/gateway/store/ -race -count=1 -v`
Expected: PASS (6 tests, including reopen idempotency).

- [ ] **Step 6: Format, vet, commit checkpoint**

Run: `gofmt -l ./cmd/gateway/ && go vet ./cmd/gateway/...`
Suggested commit message: `feat(gateway): sqlite store — config overrides and devshard registry`
(Executor: stop and hand off to Daniil.)

---

### Task 5: `metrics/` — registry, HTTP families, instrumentation

**Files:**
- Create: `cmd/gateway/metrics/metrics.go`
- Test: `cmd/gateway/metrics/metrics_test.go`

**Interfaces:**
- Consumes: nothing internal (prometheus only).
- Produces (used by Task 6 and every later phase):
  - `func New() *Metrics`
  - `(m *Metrics) Handler() http.Handler` — serves the registry (Go + process collectors included).
  - `(m *Metrics) InstrumentRoute(routeLabel string, next http.Handler) http.Handler` — records `devshard_http_requests_total{path,method,status}` and `devshard_http_request_duration_seconds{path,method}` with `path` = the fixed routeLabel (bounded cardinality; `{id}` stays literal in labels).
  - `(m *Metrics) Registry() *prometheus.Registry` — later phases register their families here.

- [ ] **Step 1: Write the failing test**

`cmd/gateway/metrics/metrics_test.go`:

```go
package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInstrumentRouteCountsRequestsWithPreservedFamilyNames(t *testing.T) {
	gatewayMetrics := New()
	instrumented := gatewayMetrics.InstrumentRoute("/v1/test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	request := httptest.NewRequest(http.MethodGet, "/v1/test", nil)
	recorder := httptest.NewRecorder()
	instrumented.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusTeapot {
		t.Fatalf("instrumented handler must pass through status, got %d", recorder.Code)
	}

	exposition := scrape(t, gatewayMetrics)
	wantCounter := `devshard_http_requests_total{method="GET",path="/v1/test",status="418"} 1`
	if !strings.Contains(exposition, wantCounter) {
		t.Fatalf("exposition missing %q\n---\n%s", wantCounter, exposition)
	}
	if !strings.Contains(exposition, `devshard_http_request_duration_seconds_count{method="GET",path="/v1/test"} 1`) {
		t.Fatalf("exposition missing duration histogram for the route\n---\n%s", exposition)
	}
}

func TestHandlerServesGoRuntimeCollectors(t *testing.T) {
	gatewayMetrics := New()
	exposition := scrape(t, gatewayMetrics)
	if !strings.Contains(exposition, "go_goroutines") {
		t.Fatal("exposition missing go_goroutines — Go collector not registered")
	}
}

func TestDefaultStatusIsRecordedAs200WhenHandlerWritesBody(t *testing.T) {
	gatewayMetrics := New()
	instrumented := gatewayMetrics.InstrumentRoute("/plain", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok")) // implicit 200, WriteHeader never called
	}))
	recorder := httptest.NewRecorder()
	instrumented.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/plain", nil))

	exposition := scrape(t, gatewayMetrics)
	if !strings.Contains(exposition, `devshard_http_requests_total{method="POST",path="/plain",status="200"} 1`) {
		t.Fatalf("implicit 200 not recorded\n---\n%s", exposition)
	}
}

func scrape(t *testing.T, gatewayMetrics *Metrics) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	gatewayMetrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatalf("reading exposition: %v", err)
	}
	return string(body)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/gateway/metrics/ -race -count=1 -v`
Expected: FAIL — `undefined: New`.

- [ ] **Step 3: Write the implementation**

`cmd/gateway/metrics/metrics.go`:

```go
// Package metrics owns the gateway's Prometheus registry. Family names are
// frozen: dashboards and alerts depend on the devshard_* names carried over
// from devshardctl (spec §3).
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles the registry and the HTTP instrumentation families. Later
// phases register their own families on Registry().
type Metrics struct {
	registry            *prometheus.Registry
	httpRequestsTotal   *prometheus.CounterVec
	httpRequestDuration *prometheus.HistogramVec
}

// New builds the registry with Go/process collectors and the HTTP families.
func New() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	httpRequestsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "devshard_http_requests_total",
			Help: "Total HTTP requests handled by the devshard gateway.",
		},
		[]string{"path", "method", "status"},
	)
	httpRequestDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "devshard_http_request_duration_seconds",
			Help:    "HTTP request duration by route.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"path", "method"},
	)
	registry.MustRegister(httpRequestsTotal, httpRequestDuration)

	return &Metrics{
		registry:            registry,
		httpRequestsTotal:   httpRequestsTotal,
		httpRequestDuration: httpRequestDuration,
	}
}

// Registry exposes the underlying registry for other packages' families.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the Prometheus exposition.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// InstrumentRoute wraps next, recording count and duration under the fixed
// routeLabel. Pass the route PATTERN (e.g. "/devshard/{id}/v1/status"), never
// the raw request path — label cardinality must stay bounded.
func (m *Metrics) InstrumentRoute(routeLabel string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r)
		m.httpRequestsTotal.WithLabelValues(routeLabel, r.Method, strconv.Itoa(recorder.status)).Inc()
		m.httpRequestDuration.WithLabelValues(routeLabel, r.Method).Observe(time.Since(startedAt).Seconds())
	})
}

// statusRecorder captures the response status for labeling. Flush is
// forwarded so streaming handlers keep working when wrapped.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/gateway/metrics/ -race -count=1 -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Format, vet, commit checkpoint**

Run: `gofmt -l ./cmd/gateway/ && go vet ./cmd/gateway/...`
Suggested commit message: `feat(gateway): metrics package — registry and HTTP instrumentation with preserved family names`
(Executor: stop and hand off to Daniil.)

---

### Task 6: `main.go` — wiring, graceful shutdown, Makefile target

**Files:**
- Create: `cmd/gateway/main.go`
- Test: `cmd/gateway/main_test.go`
- Modify: `devshard/Makefile` (append two targets at the end)

**Interfaces:**
- Consumes: `env.Load` (Task 1), `config.Build`/`config.NewHolder` (Tasks 2–3), `store.Open`/`LoadOverrides` (Task 4), `metrics.New`/`Handler`/`InstrumentRoute` (Task 5), `devshard/logging` (existing: `logging.Info/Error`).
- Produces: `run(ctx context.Context) error` — the whole process; `main()` is 5 lines around it. `var Version = "dev"` set via `-ldflags "-X main.Version=..."`. Later phases extend `run` with their constructors; the shutdown ORDER established here is contract: HTTP server first, store last.

- [ ] **Step 1: Write the failing test**

`cmd/gateway/main_test.go`:

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserving port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

func TestRunServesMetricsAndShutsDownGracefully(t *testing.T) {
	port := freePort(t)
	t.Setenv("GATEWAY_PORT", fmt.Sprintf("%d", port))
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- run(ctx) }()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	var body string
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := http.Get(baseURL + "/metrics")
		if err == nil {
			raw, readErr := io.ReadAll(response.Body)
			response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK {
				body = string(raw)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway did not serve /metrics within 5s (last error: %v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(body, "devshard_http_requests_total") && !strings.Contains(body, "go_goroutines") {
		t.Fatalf("/metrics exposition unexpected:\n%s", body)
	}

	cancel()
	select {
	case err := <-runResult:
		if err != nil {
			t.Fatalf("run() after cancel: %v, want nil (graceful shutdown)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run() did not return within 5s of cancellation")
	}
}

func TestRunFailsFastOnInvalidEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "not-a-port")
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "GATEWAY_PORT") {
		t.Fatalf("run() with bad env = %v, want error naming GATEWAY_PORT", err)
	}
}

func TestRunFailsFastOnInvalidMergedConfig(t *testing.T) {
	t.Setenv("GATEWAY_MAX_TOKENS_CAP", "1") // below default_max_tokens 3072
	t.Setenv("GATEWAY_STORAGE_DIR", t.TempDir())
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "max_tokens_cap") {
		t.Fatalf("run() with invalid merged config = %v, want max_tokens_cap error", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./cmd/gateway/ -race -count=1 -v`
Expected: FAIL — `undefined: run`.

- [ ] **Step 3: Write `main.go`**

```go
// Command gateway is the devshard gateway between the broker and race
// participants. Design spec: cmd/gateway/specs/2026-07-17-gateway-redesign-design.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/store"
	"devshard/logging"
)

// Version is stamped by the build via -ldflags "-X main.Version=...".
var Version = "dev"

// shutdownGracePeriod bounds how long in-flight HTTP work may drain.
const shutdownGracePeriod = 10 * time.Second

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		logging.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

// run wires the gateway and serves until ctx is cancelled. Shutdown order is
// contract for later phases: stop accepting HTTP first, close the store last.
func run(ctx context.Context) error {
	values, err := env.Load()
	if err != nil {
		return err
	}

	storageDir, err := resolveStorageDir(values.StorageDir)
	if err != nil {
		return err
	}
	gatewayStore, err := store.Open(storageDir)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := gatewayStore.Close(); closeErr != nil {
			logging.Error("closing store", "error", closeErr)
		}
	}()

	overrides, err := gatewayStore.LoadOverrides(ctx)
	if err != nil {
		return err
	}
	configuration, err := config.Build(values, overrides)
	if err != nil {
		return err
	}
	configHolder := config.NewHolder(configuration)

	gatewayMetrics := metrics.New()
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", gatewayMetrics.InstrumentRoute("/metrics", gatewayMetrics.Handler()))

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", configHolder.Load().Server.Port),
		Handler: mux,
	}
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.ListenAndServe() }()
	logging.Info("gateway started", "version", Version, "port", configHolder.Load().Server.Port, "storage_dir", storageDir)

	select {
	case err := <-serveResult:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancelDrain()
	if err := server.Shutdown(drainCtx); err != nil {
		return fmt.Errorf("shutting down http server: %w", err)
	}
	if err := <-serveResult; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	logging.Info("gateway stopped")
	return nil
}

// resolveStorageDir picks the storage directory: explicit value or the
// platform default ~/.cache/gonka-gateway (fresh dir, spec §16).
func resolveStorageDir(explicit *string) (string, error) {
	if explicit != nil {
		return *explicit, nil
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolving home dir for storage: %w", err)
	}
	return filepath.Join(homeDir, ".cache", "gonka-gateway"), nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./cmd/gateway/ -race -count=1 -v`
Expected: PASS (3 tests). Then the full phase suite: `go test ./cmd/gateway/... -race -count=1` — all packages PASS.

- [ ] **Step 5: Add Makefile targets and verify the binary builds**

Append to `devshard/Makefile` (extend the `.PHONY` line at the top with ` gateway gateway-test`):

```make
gateway:
	go build -ldflags "-X main.Version=$$(git describe --tags --always 2>/dev/null || echo dev)" -o bin/gateway ./cmd/gateway

gateway-test:
	go test ./cmd/gateway/... -race -count=1
```

Run: `make gateway && ./bin/gateway & sleep 1 && curl -s http://127.0.0.1:8080/metrics | head -3 && kill %1`
Expected: binary builds; curl prints Prometheus exposition lines; process exits 0 on SIGTERM.
(If port 8080 is busy locally: `GATEWAY_PORT=18080 ./bin/gateway` and curl that port.)

- [ ] **Step 6: Format, vet, commit checkpoint**

Run: `gofmt -l ./cmd/gateway/ && go vet ./cmd/gateway/...`
Suggested commit message: `feat(gateway): bootable binary — env→config→store→metrics wiring with graceful shutdown`
(Executor: stop and hand off to Daniil.)

---

## Phase 1 Definition of Done

- `go test ./cmd/gateway/... -race -count=1` fully green.
- `make gateway` produces `bin/gateway`; the binary boots with zero env vars set (all defaults), serves `/metrics`, and exits cleanly on SIGTERM.
- `gofmt` and `go vet` clean; no `os.Getenv` outside `cmd/gateway/env/` (verify: `grep -rn "os.Getenv" cmd/gateway/ | grep -v "cmd/gateway/env/"` prints nothing).
- `cmd/devshardctl` untouched (`git status` shows no changes there).

## What Phase 2 (filters) will consume from here

`config.Config` (token limits + capture settings), `config.Holder`, `metrics.Registry()`, the golden-corpus workflow described in spec §15.1. Phase 2 gets its own plan document after Phase 1 lands.

---

## Amendments (2026-07-18, approved by Daniil during execution)

Tasks 1–2 were implemented before these decisions; a dedicated amendment task reshapes them. Tasks 3–6 MUST follow this section wherever it conflicts with their original text. Overrides JSON keys stay flat snake_case (wire-format stability); only the Go struct shape changes.

### A. Grouped limit sub-types (kill repeated prefixes)

```go
// Concurrency bounds in-flight requests: absolute cap + weight-scaled allowance.
type Concurrency struct {
	MaxRequests               int64
	RequestsPer10000Weight    float64
	PoCRequestsPer10000Weight float64
}

// AIMD is the additive-increase/multiplicative-decrease per-host window tuning.
type AIMD struct {
	InitialWindow int64
	MaxWindow     int64
}

// Breaker is the participant circuit-breaker ladder tuning.
type Breaker struct {
	TripThreshold int64
	BaseOpenMS    int64
	MaxOpenMS     int64
}
```

`Limits` drops the eight flat fields in favor of `Concurrency Concurrency`, `AIMD AIMD`, `Breaker Breaker`. Mechanical renames for all later task code:

| Old path | New path |
|---|---|
| `Limits.MaxConcurrentRequests` | `Limits.Concurrency.MaxRequests` |
| `Limits.MaxConcurrentRequestsPer10000Weight` | `Limits.Concurrency.RequestsPer10000Weight` |
| `Limits.PoCMaxConcurrentRequestsPer10000Weight` | `Limits.Concurrency.PoCRequestsPer10000Weight` |
| `Limits.AIMDInitialWindow` | `Limits.AIMD.InitialWindow` |
| `Limits.AIMDMaxWindow` | `Limits.AIMD.MaxWindow` |
| `Limits.BreakerTripThreshold` | `Limits.Breaker.TripThreshold` |
| `Limits.BreakerBaseOpenMS` | `Limits.Breaker.BaseOpenMS` |
| `Limits.BreakerMaxOpenMS` | `Limits.Breaker.MaxOpenMS` |

Validation message field names (snake_case) are unchanged.

### B. Per-model limits: tokens + optional concurrency

`ModelTokenLimit` is renamed and extended (validation name `model_token_limits` → `model_limits`; JSON key `model_token_limits` → `model_limits` in Overrides):

```go
// ModelLimits is the per-model override set. Token fields are required as a
// pair; pointer fields are optional — nil inherits the global limit.
type ModelLimits struct {
	DefaultMaxTokens       int64  `json:"default_max_tokens"`
	MaxTokensCap           int64  `json:"max_tokens_cap"`
	MaxConcurrentRequests  *int64 `json:"max_concurrent_requests,omitempty"`
	MaxInputTokensInFlight *int64 `json:"max_input_tokens_in_flight,omitempty"`
}
```

`Limits.ModelTokenLimits map[string]ModelTokenLimit` → `Limits.ModelLimits map[string]ModelLimits`; `Overrides.ModelTokenLimits` → `Overrides.ModelLimits` (tag `model_limits`). Validate additionally rejects `*MaxConcurrentRequests < 1` and `*MaxInputTokensInFlight < 0`.

### C. DefaultModel is dropped

`Server.DefaultModel`, `env.Values.DefaultModel` and `GATEWAY_DEFAULT_MODEL` are removed everywhere (struct, Load, tables, tests, Defaults, TestDefaultsMatchSpec row). The pooled chat route requires `model` in the body (400 without it — enforced in the api phase); per-escrow routes fall back to the escrow's model.

### D. Tx-query fallbacks become a list

`Chain.TxQueryBaseURL string` → `Chain.TxQueryFallbackURLs []string`, default `[]string{"http://node1.gonka.ai:8000/chain-api"}`. Env `GATEWAY_TX_QUERY_REST` → `GATEWAY_TX_QUERY_FALLBACK_URLS` (comma-separated, split like APIKeys); `env.Values.TxQueryREST` → `env.Values.TxQueryFallbackURLs *string`. Validate checks every element as an absolute URL (`tx_query_fallback_urls[i]`); empty list is allowed (weakened recovery is the operator's explicit choice). Semantics note for the chain phase: fallbacks are polled only during active tx confirmation, with backoff.

### Consequences for later tasks

- Task 3 `build.go`: map the renamed paths (table above); `values.TxQueryFallbackURLs` splits via `splitCommaSeparated`; no DefaultModel mapping. `build_test.go` asserts `Limits.Concurrency.RequestsPer10000Weight` etc.
- Task 6 `main.go`/tests: unaffected (never used DefaultModel).
- Env var count drops to 49 (−GATEWAY_DEFAULT_MODEL; GATEWAY_TX_QUERY_REST renamed).

### E. Snapshot no-aliasing contract (from config re-review, 2026-07-18)

`Config` holds reference types (`Server.APIKeys`, `Chain.TxQueryFallbackURLs`, `Limits.ModelLimits`, `Limits.ModelAccess`). Task 3's `Build` MUST clone every map/slice taken from `env.Values`/`Overrides` into the snapshot (`maps.Clone` / `slices.Clone`) instead of assigning them — the original Task 3 code block assigns `overrides.ModelTokenLimits` directly; that line is superseded. Task 3 also adds three config test cases carried from review: (1) `Validate()` accepts a `ModelLimits` entry with valid tokens and both pointers nil, (2) `Validate()` accepts `TxQueryFallbackURLs = []string{}`, (3) Overrides JSON round-trip preserves `ModelLimits.MaxInputTokensInFlight`.

### F. Env diet (2026-07-18, requested by Daniil: drop env vars the new architecture doesn't need)

Principle: env = deployment identity (ports, paths, secrets, topology, endpoints, kill-switch, coarse limits, debug toggles); admin overrides = run-time tuning; config defaults = plumbing constants nobody tunes per-deploy.

**Removed from env (25)** — values stay as Config defaults; tuning ones remain admin-overridable: `GATEWAY_ACQUIRE_WAIT_MS`, `GATEWAY_AIMD_INITIAL_WINDOW`, `GATEWAY_AIMD_MAX_WINDOW`, `GATEWAY_BREAKER_TRIP_THRESHOLD`, `GATEWAY_BREAKER_BASE_OPEN_MS`, `GATEWAY_BREAKER_MAX_OPEN_MS`, `GATEWAY_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT`, `GATEWAY_POC_MAX_CONCURRENT_REQUESTS_PER_10000_WEIGHT`, `GATEWAY_MAX_INPUT_TOKENS_IN_FLIGHT`, `GATEWAY_ROTATION_PRE_POC_BLOCKS`, `GATEWAY_TX_POLL_INTERVAL_MS`, `GATEWAY_TX_POLL_TIMEOUT_MS`, `GATEWAY_TX_FEE_DENOM`, `GATEWAY_MAX_CONCURRENT_RUNTIME_BUILDS`, `GATEWAY_DRAIN_TIMEOUT_SECONDS`, `GATEWAY_CLASSIFY_MAX_ATTEMPT_BYTES`, `GATEWAY_CLASSIFY_MAX_PARTICIPANT_BYTES`, `GATEWAY_CLASSIFY_MAX_GLOBAL_BYTES`, `GATEWAY_CAPTURE_SHORT_CONTENT_ATTEMPTS`, `GATEWAY_CAPTURE_SHORT_CONTENT_RESPONSES`, `GATEWAY_CAPTURE_SHORT_CONTENT_MIN_OUTPUT_CHUNKS`, `GATEWAY_CAPTURE_SHORT_CONTENT_MAX_CONTENT_RATIO`, `GATEWAY_CAPTURE_SHORT_CONTENT_RESPONSE_MAX_BYTES`, `GATEWAY_ROUTE_PREFIX`, `GATEWAY_CHAIN_ID`.

**Removed from Config entirely** (not configuration): `Server.RoutePrefix` (derive from the devshard protocol version in code when the api phase mounts routes) and `Chain.ChainID` (auto-discovered from node_info at runtime by the chain phase — discovered state, not config).

**Surviving env (24):** PORT, STORAGE_DIR, API_KEYS, ADMIN_API_KEY, DEVSHARDS_JSON, CHAIN_REST, PUBLIC_API, TX_QUERY_FALLBACK_URLS, TX_FEE_AMOUNT, TX_GAS_LIMIT, DEFAULT_MAX_TOKENS, MAX_TOKENS_CAP, MAX_CONCURRENT_REQUESTS, POC_MODE, CAPACITY_AWARE_LIMITS, DISABLED, DISABLED_MESSAGE, DISABLED_REDIRECT_URL, ROTATION_ENABLED, ROTATION_SETTLEMENT_ENABLED, ROTATION_MODELS_JSON, CHAT_CACHE_MAX_BYTES, CAPTURE_ENABLED, CAPTURE_DIR.

**Code changes:** env.Values loses the 25 fields + read calls (env_test's trim probe moves from GATEWAY_ROUTE_PREFIX to GATEWAY_DISABLED_MESSAGE); config.Config loses Server.RoutePrefix and Chain.ChainID (no Validate branches existed for either); build.go loses the 25 mappings; build_test.go's TestBuildPerWeightFloatFromEnv is deleted (no longer env-sourced — per-weight stays testable via the overrides layer in the precedence test family); TestDefaultsMatchSpec keeps all remaining default rows. Overrides unchanged.
