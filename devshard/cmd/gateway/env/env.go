// Package env is the single place the gateway reads environment variables. Load returns what is SET
// (nil pointer = unset); defaults belong to config, never here. See README.md.
package env

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

// Values mirrors every GATEWAY_* environment variable the gateway reads; nil = unset.
type Values struct {
	Port          *int64
	StorageDir    *string
	APIKeys       *string
	AdminAPIKey   *string
	DevshardsJSON *string

	ChainGRPC   *string
	PublicAPI   *string
	ChainID     *string
	ChainRPC    *string
	TxFeeAmount *int64
	TxGasLimit  *int64

	DefaultMaxTokens      *int64
	MaxTokensCap          *int64
	MaxConcurrentRequests *int64
	AdmissionQueueWaitMS  *int64
	AdmissionQueuePerSlot *int64

	PoCMode             *string
	Disabled            *bool
	DisabledMessage     *string
	DisabledRedirectURL *string

	RotationEnabled           *bool
	RotationPrePoCBlocks      *int64
	MatchWaitMS               *int64
	ForceUpstreamStreaming    *bool
	MaxBufferedResponseBytes  *int64
	WarmNewEscrows            *bool
	RotationSettlementEnabled *bool
	RotationModelsJSON        *string

	ChatCacheMaxBytes *int64

	AccountingRetentionHours   *int64
	AccountingRetentionMaxRows *int64

	NonceAccountingEnabled         *bool
	NonceAccountingListenAddr      *string
	NonceAccountingRetentionEpochs *int64
	NonceAccountingSnapshotSeconds *int64

	CaptureEnabled    *bool
	CaptureDir        *string
	CaptureSampleRate *float64
	CaptureMaxBytes   *int64

	PerfEWMAHalfLifeSeconds      *int64
	PerfConsecutiveFailThreshold *int64
	PerfFailureRateThreshold     *float64
	PerfFailureRateMinVolume     *float64
	PerfEjectionBaseSeconds      *int64
	PerfEjectionMaxSeconds       *int64
	PerfMaxEjectionFraction      *float64
	PerfMinAvailableHosts        *int64
	PerfHostStalenessSeconds     *int64

	ChainSnapshotMaxAgeSeconds *int64

	EngineReceiptTimeoutMS    *int64
	EngineFirstTokenFloorMS   *int64
	EngineFirstTokenCeilingMS *int64
	EngineInterChunkStallMS   *int64
	EngineLoserGraceMS        *int64
}

// PoCModeOff and PoCModeRelaxed are the accepted GATEWAY_POC_MODE values.
const (
	PoCModeOff     = "off"
	PoCModeRelaxed = "relaxed"
)

var (
	// ErrPrivateKeyMissing marks a devshard whose signing key the environment does not hold.
	ErrPrivateKeyMissing = errors.New("private key missing")

	// legacyNames is the devshardctl spelling each variable falls back to. See operations.md, "Variable names".
	legacyNames = map[string]string{
		"GATEWAY_ESCROWS_JSON":                "DEVSHARDS_JSON",
		"GATEWAY_PORT":                        "DEVSHARD_PORT",
		"GATEWAY_STORAGE_DIR":                 "DEVSHARD_STORAGE_DIR",
		"GATEWAY_API_KEYS":                    "DEVSHARD_API_KEYS",
		"GATEWAY_ADMIN_API_KEY":               "DEVSHARD_ADMIN_API_KEY",
		"GATEWAY_CHAIN_GRPC":                  "DEVSHARD_CHAIN_GRPC",
		"GATEWAY_PUBLIC_API":                  "DEVSHARD_PUBLIC_API",
		"GATEWAY_CHAIN_ID":                    "DEVSHARD_CHAIN_ID",
		"GATEWAY_CHAIN_RPC":                   "DEVSHARD_CHAIN_RPC",
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
)

// PrivateKey reads the key held by the named variable; errors and logs name the variable, never the value.
func PrivateKey(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("%w: no environment variable named", ErrPrivateKeyMissing)
	}
	if key := strings.TrimSpace(os.Getenv(name)); key != "" {
		return key, nil
	}
	if renamed, aliased := strings.CutPrefix(name, "DEVSHARD_"); aliased {
		renamed = "GATEWAY_" + renamed
		if key := strings.TrimSpace(os.Getenv(renamed)); key != "" {
			logging.Warn("signing key read from the renamed variable",
				logkey.Subsystem, "env", logkey.Recorded, name, logkey.Used, renamed)
			return key, nil
		}
	}
	return "", fmt.Errorf("%w: %s is unset", ErrPrivateKeyMissing, name)
}

// lookup prefers the gateway's spelling; empty counts as unset on both, so blanking a legacy variable sticks.
func lookup(name string) string {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		return raw
	}
	if legacy, aliased := legacyNames[name]; aliased {
		return strings.TrimSpace(os.Getenv(legacy))
	}
	return ""
}

// Load reads every gateway environment variable, accumulating parse failures so none is reported alone.
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
	readString("GATEWAY_ESCROWS_JSON", &values.DevshardsJSON)

	readString("GATEWAY_CHAIN_GRPC", &values.ChainGRPC)
	readString("GATEWAY_PUBLIC_API", &values.PublicAPI)
	readString("GATEWAY_CHAIN_ID", &values.ChainID)
	readString("GATEWAY_CHAIN_RPC", &values.ChainRPC)
	readInt("GATEWAY_TX_FEE_AMOUNT", &values.TxFeeAmount)
	readInt("GATEWAY_TX_GAS_LIMIT", &values.TxGasLimit)

	readInt("GATEWAY_DEFAULT_MAX_TOKENS", &values.DefaultMaxTokens)
	readInt("GATEWAY_MAX_TOKENS_CAP", &values.MaxTokensCap)
	readInt("GATEWAY_MAX_CONCURRENT_REQUESTS", &values.MaxConcurrentRequests)
	readInt("GATEWAY_ADMISSION_QUEUE_WAIT_MS", &values.AdmissionQueueWaitMS)
	readInt("GATEWAY_ADMISSION_QUEUE_PER_SLOT", &values.AdmissionQueuePerSlot)

	readString("GATEWAY_POC_MODE", &values.PoCMode)
	readBool("GATEWAY_DISABLED", &values.Disabled)
	readString("GATEWAY_DISABLED_MESSAGE", &values.DisabledMessage)
	readString("GATEWAY_DISABLED_REDIRECT_URL", &values.DisabledRedirectURL)

	readBool("GATEWAY_ROTATION_ENABLED", &values.RotationEnabled)
	readInt("GATEWAY_ROTATION_PRE_POC_BLOCKS", &values.RotationPrePoCBlocks)
	readInt("GATEWAY_MATCH_WAIT_MS", &values.MatchWaitMS)
	readBool("GATEWAY_FORCE_UPSTREAM_STREAMING", &values.ForceUpstreamStreaming)
	readInt("GATEWAY_MAX_BUFFERED_RESPONSE_BYTES", &values.MaxBufferedResponseBytes)
	readBool("GATEWAY_WARM_NEW_ESCROWS", &values.WarmNewEscrows)
	readBool("GATEWAY_ROTATION_SETTLEMENT_ENABLED", &values.RotationSettlementEnabled)
	readString("GATEWAY_ROTATION_MODELS_JSON", &values.RotationModelsJSON)

	readInt("GATEWAY_CHAT_CACHE_MAX_BYTES", &values.ChatCacheMaxBytes)

	readInt("GATEWAY_ACCOUNTING_RETENTION_HOURS", &values.AccountingRetentionHours)
	readInt("GATEWAY_ACCOUNTING_RETENTION_MAX_ROWS", &values.AccountingRetentionMaxRows)

	readBool("GATEWAY_NONCE_ACCOUNTING_ENABLED", &values.NonceAccountingEnabled)
	readString("GATEWAY_NONCE_ACCOUNTING_LISTEN_ADDR", &values.NonceAccountingListenAddr)
	readInt("GATEWAY_NONCE_ACCOUNTING_RETENTION_EPOCHS", &values.NonceAccountingRetentionEpochs)
	readInt("GATEWAY_NONCE_ACCOUNTING_SNAPSHOT_SECONDS", &values.NonceAccountingSnapshotSeconds)

	readBool("GATEWAY_CAPTURE_ENABLED", &values.CaptureEnabled)
	readString("GATEWAY_CAPTURE_DIR", &values.CaptureDir)
	readFloat("GATEWAY_CAPTURE_SAMPLE_RATE", &values.CaptureSampleRate)
	readInt("GATEWAY_CAPTURE_MAX_BYTES", &values.CaptureMaxBytes)

	readInt("GATEWAY_PERF_EWMA_HALFLIFE_SECONDS", &values.PerfEWMAHalfLifeSeconds)
	readInt("GATEWAY_PERF_CONSECUTIVE_FAIL_THRESHOLD", &values.PerfConsecutiveFailThreshold)
	readFloat("GATEWAY_PERF_FAILURE_RATE_THRESHOLD", &values.PerfFailureRateThreshold)
	readFloat("GATEWAY_PERF_FAILURE_RATE_MIN_VOLUME", &values.PerfFailureRateMinVolume)
	readInt("GATEWAY_PERF_EJECTION_BASE_SECONDS", &values.PerfEjectionBaseSeconds)
	readInt("GATEWAY_PERF_EJECTION_MAX_SECONDS", &values.PerfEjectionMaxSeconds)
	readFloat("GATEWAY_PERF_MAX_EJECTION_FRACTION", &values.PerfMaxEjectionFraction)
	readInt("GATEWAY_PERF_MIN_AVAILABLE_HOSTS", &values.PerfMinAvailableHosts)
	readInt("GATEWAY_PERF_HOST_STALENESS_SECONDS", &values.PerfHostStalenessSeconds)

	readInt("GATEWAY_CHAIN_SNAPSHOT_MAX_AGE_SECONDS", &values.ChainSnapshotMaxAgeSeconds)

	readInt("GATEWAY_ENGINE_RECEIPT_TIMEOUT_MS", &values.EngineReceiptTimeoutMS)
	readInt("GATEWAY_ENGINE_FIRST_TOKEN_FLOOR_MS", &values.EngineFirstTokenFloorMS)
	readInt("GATEWAY_ENGINE_FIRST_TOKEN_CEILING_MS", &values.EngineFirstTokenCeilingMS)
	readInt("GATEWAY_ENGINE_INTER_CHUNK_STALL_MS", &values.EngineInterChunkStallMS)
	readInt("GATEWAY_ENGINE_LOSER_GRACE_MS", &values.EngineLoserGraceMS)

	if values.PoCMode != nil && *values.PoCMode != PoCModeOff && *values.PoCMode != PoCModeRelaxed {
		problems = append(problems, fmt.Errorf("GATEWAY_POC_MODE: %q is not %q or %q", *values.PoCMode, PoCModeOff, PoCModeRelaxed))
	}

	if len(problems) > 0 {
		return Values{}, fmt.Errorf("reading environment: %w", errors.Join(problems...))
	}
	return values, nil
}
