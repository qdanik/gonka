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

	ChainREST           *string
	PublicAPI           *string
	TxQueryFallbackURLs *string
	TxFeeAmount         *int64
	TxGasLimit          *int64

	DefaultMaxTokens      *int64
	MaxTokensCap          *int64
	MaxConcurrentRequests *int64

	PoCMode             *string
	CapacityAwareLimits *bool
	Disabled            *bool
	DisabledMessage     *string
	DisabledRedirectURL *string

	RotationEnabled           *bool
	RotationSettlementEnabled *bool
	RotationModelsJSON        *string

	ChatCacheMaxBytes *int64

	CaptureEnabled *bool
	CaptureDir     *string
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

	readString("GATEWAY_CHAIN_REST", &values.ChainREST)
	readString("GATEWAY_PUBLIC_API", &values.PublicAPI)
	readString("GATEWAY_TX_QUERY_FALLBACK_URLS", &values.TxQueryFallbackURLs)
	readInt("GATEWAY_TX_FEE_AMOUNT", &values.TxFeeAmount)
	readInt("GATEWAY_TX_GAS_LIMIT", &values.TxGasLimit)

	readInt("GATEWAY_DEFAULT_MAX_TOKENS", &values.DefaultMaxTokens)
	readInt("GATEWAY_MAX_TOKENS_CAP", &values.MaxTokensCap)
	readInt("GATEWAY_MAX_CONCURRENT_REQUESTS", &values.MaxConcurrentRequests)

	readString("GATEWAY_POC_MODE", &values.PoCMode)
	readBool("GATEWAY_CAPACITY_AWARE_LIMITS", &values.CapacityAwareLimits)
	readBool("GATEWAY_DISABLED", &values.Disabled)
	readString("GATEWAY_DISABLED_MESSAGE", &values.DisabledMessage)
	readString("GATEWAY_DISABLED_REDIRECT_URL", &values.DisabledRedirectURL)

	readBool("GATEWAY_ROTATION_ENABLED", &values.RotationEnabled)
	readBool("GATEWAY_ROTATION_SETTLEMENT_ENABLED", &values.RotationSettlementEnabled)
	readString("GATEWAY_ROTATION_MODELS_JSON", &values.RotationModelsJSON)

	readInt("GATEWAY_CHAT_CACHE_MAX_BYTES", &values.ChatCacheMaxBytes)

	readBool("GATEWAY_CAPTURE_ENABLED", &values.CaptureEnabled)
	readString("GATEWAY_CAPTURE_DIR", &values.CaptureDir)

	if values.PoCMode != nil && *values.PoCMode != PoCModeOff && *values.PoCMode != PoCModeRelaxed {
		problems = append(problems, fmt.Errorf("GATEWAY_POC_MODE: %q is not %q or %q", *values.PoCMode, PoCModeOff, PoCModeRelaxed))
	}

	if len(problems) > 0 {
		return Values{}, fmt.Errorf("reading environment: %w", errors.Join(problems...))
	}
	return values, nil
}
