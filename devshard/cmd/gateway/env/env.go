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

// Values mirrors every GATEWAY_* environment variable; nil = unset.
type Values struct {
	Port          *int64
	StorageDir    *string
	APIKeys       *string
	AdminAPIKey   *string
	DevshardsJSON *string

	ChainREST           *string
	ChainGRPC           *string
	ChainRPC            *string
	PublicAPI           *string
	TxQueryFallbackURLs *string
	TxFeeAmount         *int64
	TxGasLimit          *int64

	DefaultMaxTokens      *int64
	MaxTokensCap          *int64
	MaxConcurrentRequests *int64

	PoCMode             *string
	Disabled            *bool
	DisabledMessage     *string
	DisabledRedirectURL *string

	RotationEnabled           *bool
	RotationSettlementEnabled *bool
	RotationModelsJSON        *string

	ChatCacheMaxBytes *int64

	AccountingRetentionHours   *int64
	AccountingRetentionMaxRows *int64

	CaptureEnabled    *bool
	CaptureDir        *string
	CaptureSampleRate *float64
	CaptureMaxBytes   *int64

	PerfEWMAHalfLifeSeconds *int64
}

// PoCModeOff and PoCModeRelaxed are the accepted GATEWAY_POC_MODE values.
const (
	PoCModeOff     = "off"
	PoCModeRelaxed = "relaxed"
)

// ErrPrivateKeyMissing marks a devshard whose signing key the environment does not hold.
var ErrPrivateKeyMissing = errors.New("private key missing")

// PrivateKey reads the hex signing key held by the named variable. Errors name the variable and
// never the value, so a failure can be logged without leaking key material.
func PrivateKey(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: no environment variable named", ErrPrivateKeyMissing)
	}
	key := strings.TrimSpace(os.Getenv(name))
	if key == "" {
		return "", fmt.Errorf("%w: %s is unset", ErrPrivateKeyMissing, name)
	}
	return key, nil
}

// legacyNames is the devshardctl spelling each variable falls back to, so a deployment still running
// the shipped template starts without an edit. The suffixes do not match mechanically -- rotation, the
// disabled switch, the PoC mode and the tx-query URLs were all renamed -- so the pairs are listed
// rather than derived. Variables absent here have no devshardctl equivalent.
var legacyNames = map[string]string{
	"GATEWAY_PORT":                        "DEVSHARD_PORT",
	"GATEWAY_STORAGE_DIR":                 "DEVSHARD_STORAGE_DIR",
	"GATEWAY_API_KEYS":                    "DEVSHARD_API_KEYS",
	"GATEWAY_ADMIN_API_KEY":               "DEVSHARD_ADMIN_API_KEY",
	"GATEWAY_CHAIN_REST":                  "DEVSHARD_CHAIN_REST",
	"GATEWAY_CHAIN_GRPC":                  "DEVSHARD_CHAIN_GRPC",
	"GATEWAY_CHAIN_RPC":                   "DEVSHARD_CHAIN_RPC",
	"GATEWAY_PUBLIC_API":                  "DEVSHARD_PUBLIC_API",
	"GATEWAY_TX_QUERY_FALLBACK_URLS":      "DEVSHARD_TX_QUERY_REST",
	"GATEWAY_TX_FEE_AMOUNT":               "DEVSHARD_TX_FEE_AMOUNT",
	"GATEWAY_TX_GAS_LIMIT":                "DEVSHARD_TX_GAS_LIMIT",
	"GATEWAY_POC_MODE":                    "DEVSHARD_POC_REQUEST_MODE",
	"GATEWAY_DISABLED":                    "DEVSHARD_GATEWAY_DISABLED",
	"GATEWAY_DISABLED_MESSAGE":            "DEVSHARD_GATEWAY_DISABLED_MESSAGE",
	"GATEWAY_DISABLED_REDIRECT_URL":       "DEVSHARD_GATEWAY_DISABLED_NEW_URL",
	"GATEWAY_ROTATION_ENABLED":            "DEVSHARD_ESCROW_ROTATION_ENABLED",
	"GATEWAY_ROTATION_SETTLEMENT_ENABLED": "DEVSHARD_ESCROW_ROTATION_SETTLEMENT_ENABLED",
	"GATEWAY_ROTATION_MODELS_JSON":        "DEVSHARD_ESCROW_ROTATION_MODELS_JSON",
	"GATEWAY_CHAT_CACHE_MAX_BYTES":        "DEVSHARD_CHAT_CACHE_MAX_BYTES",
}

// lookup prefers the gateway's own spelling and falls back to devshardctl's. An empty value counts as
// unset on both, so an operator can blank a legacy variable without the fallback resurrecting it.
func lookup(name string) string {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		return raw
	}
	if legacy, aliased := legacyNames[name]; aliased {
		return strings.TrimSpace(os.Getenv(legacy))
	}
	return ""
}

// Load reads every gateway environment variable. Parse failures are
// accumulated so the operator sees all misconfigured variables at once.
func Load() (Values, error) {
	var values Values
	var problems []error

	readString := func(name string, target **string) {
		raw := lookup(name)
		if raw == "" {
			return
		}
		*target = &raw
	}
	readInt := func(name string, target **int64) {
		raw := lookup(name)
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
		raw := lookup(name)
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
		raw := lookup(name)
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

	readString("GATEWAY_CHAIN_REST", &values.ChainREST)
	readString("GATEWAY_CHAIN_GRPC", &values.ChainGRPC)
	readString("GATEWAY_CHAIN_RPC", &values.ChainRPC)
	readString("GATEWAY_PUBLIC_API", &values.PublicAPI)
	readString("GATEWAY_TX_QUERY_FALLBACK_URLS", &values.TxQueryFallbackURLs)
	readInt("GATEWAY_TX_FEE_AMOUNT", &values.TxFeeAmount)
	readInt("GATEWAY_TX_GAS_LIMIT", &values.TxGasLimit)

	readInt("GATEWAY_DEFAULT_MAX_TOKENS", &values.DefaultMaxTokens)
	readInt("GATEWAY_MAX_TOKENS_CAP", &values.MaxTokensCap)
	readInt("GATEWAY_MAX_CONCURRENT_REQUESTS", &values.MaxConcurrentRequests)

	readString("GATEWAY_POC_MODE", &values.PoCMode)
	readBool("GATEWAY_DISABLED", &values.Disabled)
	readString("GATEWAY_DISABLED_MESSAGE", &values.DisabledMessage)
	readString("GATEWAY_DISABLED_REDIRECT_URL", &values.DisabledRedirectURL)

	readBool("GATEWAY_ROTATION_ENABLED", &values.RotationEnabled)
	readBool("GATEWAY_ROTATION_SETTLEMENT_ENABLED", &values.RotationSettlementEnabled)
	readString("GATEWAY_ROTATION_MODELS_JSON", &values.RotationModelsJSON)

	readInt("GATEWAY_CHAT_CACHE_MAX_BYTES", &values.ChatCacheMaxBytes)

	readInt("GATEWAY_ACCOUNTING_RETENTION_HOURS", &values.AccountingRetentionHours)
	readInt("GATEWAY_ACCOUNTING_RETENTION_MAX_ROWS", &values.AccountingRetentionMaxRows)

	readBool("GATEWAY_CAPTURE_ENABLED", &values.CaptureEnabled)
	readString("GATEWAY_CAPTURE_DIR", &values.CaptureDir)
	readFloat("GATEWAY_CAPTURE_SAMPLE_RATE", &values.CaptureSampleRate)
	readInt("GATEWAY_CAPTURE_MAX_BYTES", &values.CaptureMaxBytes)

	readInt("GATEWAY_PERF_EWMA_HALFLIFE_SECONDS", &values.PerfEWMAHalfLifeSeconds)

	if values.PoCMode != nil && *values.PoCMode != PoCModeOff && *values.PoCMode != PoCModeRelaxed {
		problems = append(problems, fmt.Errorf("GATEWAY_POC_MODE: %q is not %q or %q", *values.PoCMode, PoCModeOff, PoCModeRelaxed))
	}

	if len(problems) > 0 {
		return Values{}, fmt.Errorf("reading environment: %w", errors.Join(problems...))
	}
	return values, nil
}
