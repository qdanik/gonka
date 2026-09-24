package env

import (
	"errors"
	"os"
	"strings"
	"testing"
)

// Test flow:
//  1. Clear the port variable to empty (`t.Setenv` treats empty as unset), asserting pristine-environment behavior.
//  2. Call `Load`.
//  3. Assert `Port` comes back nil.
func TestLoadReturnsNilForUnsetVariables(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load() with clean environment: unexpected error: %v", err)
	}
	if values.Port != nil {
		t.Fatalf("Port = %v, want nil for unset variable", *values.Port)
	}
}

// Test flow:
//  1. Set one environment variable of each type `Load` parses: int, bool, float.
//  2. Call `Load`.
//  3. Assert each parsed field matches its set value.
func TestLoadParsesTypedValues(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "9191")
	t.Setenv("GATEWAY_ROTATION_ENABLED", "true")
	t.Setenv("GATEWAY_DISABLED", "true")
	t.Setenv("GATEWAY_TX_FEE_AMOUNT", "500")
	t.Setenv("GATEWAY_PERF_EWMA_HALFLIFE_SECONDS", "900")
	t.Setenv("GATEWAY_CAPTURE_SAMPLE_RATE", "0.25")
	t.Setenv("GATEWAY_CAPTURE_MAX_BYTES", "4096")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): unexpected error: %v", err)
	}
	if values.Port == nil || *values.Port != 9191 {
		t.Fatalf("Port = %v, want 9191", values.Port)
	}
	if values.RotationEnabled == nil || *values.RotationEnabled != true {
		t.Fatalf("RotationEnabled = %v, want true", values.RotationEnabled)
	}
	if values.Disabled == nil || *values.Disabled != true {
		t.Fatalf("Disabled = %v, want true", values.Disabled)
	}
	if values.TxFeeAmount == nil || *values.TxFeeAmount != 500 {
		t.Fatalf("TxFeeAmount = %v, want 500", values.TxFeeAmount)
	}
	if values.PerfEWMAHalfLifeSeconds == nil || *values.PerfEWMAHalfLifeSeconds != 900 {
		t.Fatalf("PerfEWMAHalfLifeSeconds = %v, want 900", values.PerfEWMAHalfLifeSeconds)
	}
	if values.CaptureSampleRate == nil || *values.CaptureSampleRate != 0.25 {
		t.Fatalf("CaptureSampleRate = %v, want 0.25", values.CaptureSampleRate)
	}
	if values.CaptureMaxBytes == nil || *values.CaptureMaxBytes != 4096 {
		t.Fatalf("CaptureMaxBytes = %v, want 4096", values.CaptureMaxBytes)
	}
}

// Test flow:
//  1. Set a string variable padded with whitespace and an int variable containing only whitespace.
//  2. Call `Load`.
//  3. Assert the string value is trimmed and the blank int variable loads as unset (nil).
func TestLoadWhitespaceIsTrimmedAndEmptyMeansUnset(t *testing.T) {
	t.Setenv("GATEWAY_DISABLED_MESSAGE", "  gateway paused  ")
	t.Setenv("GATEWAY_TX_GAS_LIMIT", "   ")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): unexpected error: %v", err)
	}
	if values.DisabledMessage == nil || *values.DisabledMessage != "gateway paused" {
		t.Fatalf("DisabledMessage = %v, want trimmed \"gateway paused\"", values.DisabledMessage)
	}
	if values.TxGasLimit != nil {
		t.Fatalf("TxGasLimit = %v, want nil for blank value", *values.TxGasLimit)
	}
}

// Test flow:
//  1. Set three variables of different types to malformed values.
//  2. Call `Load`.
//  3. Assert it returns an error naming all three variables, since errors must accumulate rather than stop at the first.
func TestLoadRejectsMalformedValuesWithVariableName(t *testing.T) {
	t.Setenv("GATEWAY_PORT", "not-a-number")
	t.Setenv("GATEWAY_DISABLED", "maybe")
	t.Setenv("GATEWAY_CAPTURE_SAMPLE_RATE", "half")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() with malformed values: want error, got nil")
	}
	message := err.Error()
	if !strings.Contains(message, "GATEWAY_PORT") {
		t.Fatalf("error %q does not name GATEWAY_PORT", message)
	}
	if !strings.Contains(message, "GATEWAY_CAPTURE_SAMPLE_RATE") {
		t.Fatalf("error %q does not name GATEWAY_CAPTURE_SAMPLE_RATE", message)
	}
	if !strings.Contains(message, "GATEWAY_DISABLED") {
		t.Fatalf("error %q does not name GATEWAY_DISABLED (errors must accumulate, not stop at first)", message)
	}
}

// Test flow:
//  1. Set the PoC mode variable to a value outside its enum.
//  2. Call `Load`.
//  3. Assert it returns an error naming the variable.
func TestLoadRejectsInvalidPoCMode(t *testing.T) {
	t.Setenv("GATEWAY_POC_MODE", "aggressive")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_POC_MODE") {
		t.Fatalf("want error naming GATEWAY_POC_MODE, got %v", err)
	}
}

// Test flow:
//  1. Set a private key variable with surrounding whitespace and read it via `PrivateKey`.
//  2. Assert the key comes back trimmed.
//  3. Look up a blank and an unset variable name.
//  4. Assert both return `ErrPrivateKeyMissing` and neither error embeds the key material.
func TestPrivateKeyReadsTheNamedVariableAndNamesOnlyTheVariableOnFailure(t *testing.T) {
	t.Setenv("DEVSHARD_KEY_A", "  deadbeef  ")
	key, err := PrivateKey("DEVSHARD_KEY_A")
	if err != nil || key != "deadbeef" {
		t.Fatalf("PrivateKey() = %q, %v, want the trimmed key", key, err)
	}

	for _, name := range []string{"", "DEVSHARD_KEY_UNSET"} {
		_, err := PrivateKey(name)
		if !errors.Is(err, ErrPrivateKeyMissing) {
			t.Fatalf("PrivateKey(%q) = %v, want ErrPrivateKeyMissing", name, err)
		}
		if strings.Contains(err.Error(), "deadbeef") {
			t.Fatalf("PrivateKey(%q) error embeds key material: %v", name, err)
		}
	}
}

// Test flow:
//  1. Subtest "the legacy name is read when the gateway name is unset": set devshardctl-spelled variables and assert `Load` reads them (the shipped deployment template still spells these the devshardctl way).
//  2. Subtest "the gateway name wins when both are set": set both spellings and assert the gateway's own name wins.
//  3. Subtest "the host-ping and height-sync variables devshardctl had are read too": set every devshardctl host-ping and height-sync variable and assert each loads, including a duration converted to milliseconds.
//  4. Subtest "legacy spellings devshardctl accepted are read the way it read them": set devshardctl's own boolean spellings (on/off/yes) and assert they parse the same way.
//  5. Subtest "a legacy value devshardctl ignored is ignored, not refused": set legacy values devshardctl could not parse and assert `Load` succeeds with those fields left unset.
//  6. Subtest "the gateway's own spelling stays strict": set the gateway's own variable to a loose spelling devshardctl would have accepted and assert `Load` still refuses it.
//  7. Subtest "every alias names a variable Load actually reads": read `env.go`'s own source and assert every legacy name and legacy duration name in the alias tables appears in a matching reader call.
func TestLoadFallsBackToTheDevshardctlSpelling(t *testing.T) {
	t.Run("the legacy name is read when the gateway name is unset", func(t *testing.T) {
		t.Setenv("DEVSHARD_PORT", "9999")
		t.Setenv("DEVSHARD_ESCROW_ROTATION_ENABLED", "true")
		t.Setenv("DEVSHARD_GATEWAY_DISABLED_NEW_URL", "https://example.invalid/v1")

		values, err := Load()
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if values.Port == nil || *values.Port != 9999 {
			t.Errorf("Port = %v, want 9999 from DEVSHARD_PORT", values.Port)
		}
		if values.RotationEnabled == nil || !*values.RotationEnabled {
			t.Errorf("RotationEnabled = %v, want true from the renamed legacy variable", values.RotationEnabled)
		}
		if values.DisabledRedirectURL == nil || *values.DisabledRedirectURL != "https://example.invalid/v1" {
			t.Errorf("DisabledRedirectURL = %v, want the value from DEVSHARD_GATEWAY_DISABLED_NEW_URL", values.DisabledRedirectURL)
		}
	})

	t.Run("the gateway name wins when both are set", func(t *testing.T) {
		t.Setenv("GATEWAY_PORT", "8080")
		t.Setenv("DEVSHARD_PORT", "9999")

		values, err := Load()
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if values.Port == nil || *values.Port != 8080 {
			t.Errorf("Port = %v, want 8080 from the gateway's own name", values.Port)
		}
	})

	t.Run("the host-ping and height-sync variables devshardctl had are read too", func(t *testing.T) {
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_DISABLED", "true")
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_INTERVAL", "15s")
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_TIMEOUT", "2s")
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_CONCURRENCY", "8")
		t.Setenv("DEVSHARD_HEIGHTSYNC_K", "12")
		t.Setenv("DEVSHARD_HEIGHTSYNC_SLOTS", "3")
		t.Setenv("DEVSHARD_REQUIRE_HEIGHT_SEED", "false")
		t.Setenv("DEVSHARD_GATEWAY_CHAIN_ORACLE", "true")

		values, err := Load()
		if err != nil {
			t.Fatalf("Load(): %v", err)
		}
		if values.HostPingDisabled == nil || !*values.HostPingDisabled {
			t.Errorf("HostPingDisabled = %v, want true", values.HostPingDisabled)
		}
		if values.HostPingIntervalMS == nil || *values.HostPingIntervalMS != 15_000 {
			t.Errorf("HostPingIntervalMS = %v, want 15000 from the duration 15s", values.HostPingIntervalMS)
		}
		if values.HostPingTimeoutMS == nil || *values.HostPingTimeoutMS != 2_000 {
			t.Errorf("HostPingTimeoutMS = %v, want 2000 from the duration 2s", values.HostPingTimeoutMS)
		}
		if values.HostPingConcurrency == nil || *values.HostPingConcurrency != 8 {
			t.Errorf("HostPingConcurrency = %v, want 8", values.HostPingConcurrency)
		}
		if values.HeightSyncAnchorK == nil || *values.HeightSyncAnchorK != 12 {
			t.Errorf("HeightSyncAnchorK = %v, want 12", values.HeightSyncAnchorK)
		}
		if values.HeightSyncAnchorSlots == nil || *values.HeightSyncAnchorSlots != 3 {
			t.Errorf("HeightSyncAnchorSlots = %v, want 3", values.HeightSyncAnchorSlots)
		}
		if values.HeightSyncRequireSeed == nil || *values.HeightSyncRequireSeed {
			t.Errorf("HeightSyncRequireSeed = %v, want false", values.HeightSyncRequireSeed)
		}
		if values.HeightSyncChainOracle == nil || !*values.HeightSyncChainOracle {
			t.Errorf("HeightSyncChainOracle = %v, want true", values.HeightSyncChainOracle)
		}
	})

	t.Run("legacy spellings devshardctl accepted are read the way it read them", func(t *testing.T) {
		t.Setenv("DEVSHARD_GATEWAY_CHAIN_ORACLE", "on")
		t.Setenv("DEVSHARD_REQUIRE_HEIGHT_SEED", "off")
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_DISABLED", "yes")

		values, err := Load()
		if err != nil {
			t.Fatalf("Load() = %v, want the devshardctl spellings accepted", err)
		}
		if values.HeightSyncChainOracle == nil || !*values.HeightSyncChainOracle {
			t.Errorf("HeightSyncChainOracle = %v, want true from on", values.HeightSyncChainOracle)
		}
		if values.HeightSyncRequireSeed == nil || *values.HeightSyncRequireSeed {
			t.Errorf("HeightSyncRequireSeed = %v, want false from off", values.HeightSyncRequireSeed)
		}
		if values.HostPingDisabled == nil || !*values.HostPingDisabled {
			t.Errorf("HostPingDisabled = %v, want true from yes", values.HostPingDisabled)
		}
	})

	t.Run("a legacy value devshardctl ignored is ignored, not refused", func(t *testing.T) {
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_INTERVAL", "fifteen")
		t.Setenv("DEVSHARD_GATEWAY_HOST_PING_TIMEOUT", "0s")
		t.Setenv("DEVSHARD_GATEWAY_CHAIN_ORACLE", "maybe")

		values, err := Load()
		if err != nil {
			t.Fatalf("Load() = %v, want the unusable legacy values left unset", err)
		}
		if values.HostPingIntervalMS != nil || values.HostPingTimeoutMS != nil || values.HeightSyncChainOracle != nil {
			t.Errorf("interval %v, timeout %v, chain oracle %v: want all unset so the defaults apply", values.HostPingIntervalMS, values.HostPingTimeoutMS, values.HeightSyncChainOracle)
		}
	})

	t.Run("the gateway's own spelling stays strict", func(t *testing.T) {
		t.Setenv("GATEWAY_HEIGHT_SYNC_CHAIN_ORACLE", "on")

		if _, err := Load(); err == nil {
			t.Fatal("Load() = nil, want GATEWAY_HEIGHT_SYNC_CHAIN_ORACLE=on refused as before")
		}
	})

	t.Run("every alias names a variable Load actually reads", func(t *testing.T) {
		source, err := os.ReadFile("env.go")
		if err != nil {
			t.Fatalf("reading env.go: %v", err)
		}
		for name := range legacyNames {
			if !strings.Contains(string(source), `("`+name+`"`) {
				t.Errorf("legacyNames has %s, which no reader in Load asks for", name)
			}
		}
		for name := range legacyDurationNames {
			if !strings.Contains(string(source), `readMilliseconds("`+name+`"`) {
				t.Errorf("legacyDurationNames has %s, which no readMilliseconds in Load asks for", name)
			}
		}
	})
}

// Test flow:
//  1. Set every nonce-accounting ledger variable under its `GATEWAY_ACCOUNTING_` prefix.
//  2. Call `Load`.
//  3. Assert each field loads with its set value.
func TestLoadReadsTheAccountingLedgerUnderTheAccountingPrefix(t *testing.T) {
	t.Setenv("GATEWAY_ACCOUNTING_ENABLED", "true")
	t.Setenv("GATEWAY_ACCOUNTING_PORT", "9191")
	t.Setenv("GATEWAY_ACCOUNTING_RETENTION_EPOCHS", "3")
	t.Setenv("GATEWAY_ACCOUNTING_SNAPSHOT_SECONDS", "60")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if values.NonceAccountingEnabled == nil || !*values.NonceAccountingEnabled {
		t.Errorf("NonceAccountingEnabled = %v, want true", values.NonceAccountingEnabled)
	}
	if values.NonceAccountingPort == nil || *values.NonceAccountingPort != 9191 {
		t.Errorf("NonceAccountingPort = %v, want 9191", values.NonceAccountingPort)
	}
	if values.NonceAccountingRetentionEpochs == nil || *values.NonceAccountingRetentionEpochs != 3 {
		t.Errorf("NonceAccountingRetentionEpochs = %v, want 3", values.NonceAccountingRetentionEpochs)
	}
	if values.NonceAccountingSnapshotSeconds == nil || *values.NonceAccountingSnapshotSeconds != 60 {
		t.Errorf("NonceAccountingSnapshotSeconds = %v, want 60", values.NonceAccountingSnapshotSeconds)
	}
}

// Test flow:
//  1. Set every nonce-accounting ledger variable under devshardctl's `DEVSHARD_STATS_` prefix (devshardctl called the same ledger "stats").
//  2. Call `Load`.
//  3. Assert each field loads with its set value, the same as the gateway's own prefix would.
func TestTheAccountingLedgerAnswersToTheDevshardctlStatsNames(t *testing.T) {
	t.Setenv("DEVSHARD_STATS_ENABLED", "true")
	t.Setenv("DEVSHARD_STATS_PORT", "9292")
	t.Setenv("DEVSHARD_STATS_RETENTION_EPOCHS", "4")
	t.Setenv("DEVSHARD_STATS_SNAPSHOT_SECONDS", "120")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if values.NonceAccountingEnabled == nil || !*values.NonceAccountingEnabled {
		t.Errorf("NonceAccountingEnabled = %v, want true from DEVSHARD_STATS_ENABLED", values.NonceAccountingEnabled)
	}
	if values.NonceAccountingPort == nil || *values.NonceAccountingPort != 9292 {
		t.Errorf("NonceAccountingPort = %v, want 9292 from DEVSHARD_STATS_PORT", values.NonceAccountingPort)
	}
	if values.NonceAccountingRetentionEpochs == nil || *values.NonceAccountingRetentionEpochs != 4 {
		t.Errorf("NonceAccountingRetentionEpochs = %v, want 4 from DEVSHARD_STATS_RETENTION_EPOCHS", values.NonceAccountingRetentionEpochs)
	}
	if values.NonceAccountingSnapshotSeconds == nil || *values.NonceAccountingSnapshotSeconds != 120 {
		t.Errorf("NonceAccountingSnapshotSeconds = %v, want 120 from DEVSHARD_STATS_SNAPSHOT_SECONDS", values.NonceAccountingSnapshotSeconds)
	}
}

// Test flow:
//  1. Set the request-record retention hours and max-rows variables under the `GATEWAY_REQUESTS_` prefix.
//  2. Call `Load`.
//  3. Assert both fields load with their set values.
func TestLoadReadsTheRequestRecordRetentionUnderTheRequestsPrefix(t *testing.T) {
	t.Setenv("GATEWAY_REQUESTS_RETENTION_HOURS", "24")
	t.Setenv("GATEWAY_REQUESTS_RETENTION_MAX_ROWS", "500")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if values.AccountingRetentionHours == nil || *values.AccountingRetentionHours != 24 {
		t.Errorf("AccountingRetentionHours = %v, want 24", values.AccountingRetentionHours)
	}
	if values.AccountingRetentionMaxRows == nil || *values.AccountingRetentionMaxRows != 500 {
		t.Errorf("AccountingRetentionMaxRows = %v, want 500", values.AccountingRetentionMaxRows)
	}
}

// Test flow:
//  1. Set the request-record retention variables under the old `GATEWAY_ACCOUNTING_` prefix.
//  2. Call `Load`.
//  3. Assert both fields stay nil, since that prefix now names the ledger and no longer reads the request records' former names.
func TestTheRequestRecordRetentionNoLongerAnswersToTheAccountingPrefix(t *testing.T) {
	t.Setenv("GATEWAY_ACCOUNTING_RETENTION_HOURS", "24")
	t.Setenv("GATEWAY_ACCOUNTING_RETENTION_MAX_ROWS", "500")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): %v", err)
	}
	if values.AccountingRetentionHours != nil {
		t.Errorf("AccountingRetentionHours = %v, want nil: GATEWAY_ACCOUNTING_RETENTION_HOURS is no longer read", *values.AccountingRetentionHours)
	}
	if values.AccountingRetentionMaxRows != nil {
		t.Errorf("AccountingRetentionMaxRows = %v, want nil: GATEWAY_ACCOUNTING_RETENTION_MAX_ROWS is no longer read", *values.AccountingRetentionMaxRows)
	}
}

// Test flow:
//  1. Set the escrow list under its former `DEVSHARDS_JSON` name.
//  2. Call `Load`.
//  3. Assert the escrow list loads from that former name.
func TestTheEscrowListStillAnswersToItsFormerName(t *testing.T) {
	t.Setenv("DEVSHARDS_JSON", `[{"escrow_id":"1"}]`)

	values, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if values.DevshardsJSON == nil || *values.DevshardsJSON != `[{"escrow_id":"1"}]` {
		t.Fatalf("escrow list = %v, want the value read from the former name", values.DevshardsJSON)
	}
}

// Test flow:
//  1. Set all four rotation environment variables (enabled, settlement enabled, pre-PoC blocks, models JSON).
//  2. Call `Load`.
//  3. Assert every one of the four knobs loaded its own variable.
func TestEveryRotationKnobIsReachableFromTheEnvironment(t *testing.T) {
	t.Setenv("GATEWAY_ROTATION_ENABLED", "true")
	t.Setenv("GATEWAY_ROTATION_SETTLEMENT_ENABLED", "true")
	t.Setenv("GATEWAY_ROTATION_PRE_POC_BLOCKS", "42")
	t.Setenv("GATEWAY_ROTATION_MODELS_JSON", "[]")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if values.RotationPrePoCBlocks == nil || *values.RotationPrePoCBlocks != 42 {
		t.Fatalf("RotationPrePoCBlocks = %v, want the value the environment set", values.RotationPrePoCBlocks)
	}
	if values.RotationEnabled == nil || values.RotationSettlementEnabled == nil || values.RotationModelsJSON == nil {
		t.Fatal("a rotation knob stopped reading its variable")
	}
}

// Test flow:
//  1. Set the rotation hold's enabled, max-per-model, and resume-answers variables.
//  2. Call `Load`.
//  3. Assert each field loads with its set value.
func TestTheHoldKnobsReadTheirVariables(t *testing.T) {
	t.Setenv("GATEWAY_ROTATION_HOLD_ENABLED", "false")
	t.Setenv("GATEWAY_ROTATION_HOLD_MAX_PER_MODEL", "3")
	t.Setenv("GATEWAY_ROTATION_HOLD_RESUME_ANSWERS", "64")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load() = %v, want nil", err)
	}
	if values.RotationHoldEnabled == nil || *values.RotationHoldEnabled {
		t.Fatalf("RotationHoldEnabled = %v, want false from the environment", values.RotationHoldEnabled)
	}
	if values.RotationHoldMaxPerModel == nil || *values.RotationHoldMaxPerModel != 3 {
		t.Fatalf("RotationHoldMaxPerModel = %v, want 3", values.RotationHoldMaxPerModel)
	}
	if values.RotationHoldResumeAnswers == nil || *values.RotationHoldResumeAnswers != 64 {
		t.Fatalf("RotationHoldResumeAnswers = %v, want 64", values.RotationHoldResumeAnswers)
	}
}

// Test flow:
//  1. Set the key under its current gateway name while looking it up by its old, renamed name (an escrow records the name of its key variable when created, so a rename would otherwise leave it inactive).
//  2. Call `PrivateKey` with the old name.
//  3. Assert it falls back to the renamed variable and returns the key.
func TestASigningKeyFallsBackToItsRenamedVariable(t *testing.T) {
	t.Setenv("GATEWAY_PRIVATE_KEY", "deadbeef")

	key, err := PrivateKey("DEVSHARD_PRIVATE_KEY")
	if err != nil {
		t.Fatalf("PrivateKey() = %v, want the key read from the renamed variable", err)
	}
	if key != "deadbeef" {
		t.Fatalf("key = %q, want the value the renamed variable holds", key)
	}
}

// Test flow:
//  1. Call `PrivateKey` with neither the old nor the current variable set.
//  2. Assert it returns `ErrPrivateKeyMissing`, since the fallback must not invent a key.
func TestASigningKeyWithNeitherNameSetStillFails(t *testing.T) {
	if _, err := PrivateKey("DEVSHARD_PRIVATE_KEY"); !errors.Is(err, ErrPrivateKeyMissing) {
		t.Fatalf("PrivateKey() = %v, want ErrPrivateKeyMissing", err)
	}
}

// Test flow:
//  1. Set every engine timing and the chain snapshot max age environment variable.
//  2. Call `Load`.
//  3. Assert each field loads with its set value.
func TestLoadParsesEngineTimings(t *testing.T) {
	for name, value := range map[string]string{
		"GATEWAY_ENGINE_RECEIPT_TIMEOUT_MS":         "7000",
		"GATEWAY_ENGINE_FIRST_TOKEN_FLOOR_MS":       "1500",
		"GATEWAY_ENGINE_FIRST_TOKEN_CEILING_MS":     "25000",
		"GATEWAY_ENGINE_INTER_CHUNK_STALL_MS":       "45000",
		"GATEWAY_ENGINE_LOSER_GRACE_MS":             "120000",
		"GATEWAY_ENGINE_HEDGE_FIRST_TOKEN_FLOOR_MS": "900",
		"GATEWAY_CHAIN_SNAPSHOT_MAX_AGE_SECONDS":    "45",
	} {
		t.Setenv(name, value)
	}

	values, err := Load()
	if err != nil {
		t.Fatalf("Load(): unexpected error: %v", err)
	}

	for _, field := range []struct {
		name string
		got  *int64
		want int64
	}{
		{"GATEWAY_ENGINE_RECEIPT_TIMEOUT_MS", values.EngineReceiptTimeoutMS, 7000},
		{"GATEWAY_ENGINE_FIRST_TOKEN_FLOOR_MS", values.EngineFirstTokenFloorMS, 1500},
		{"GATEWAY_ENGINE_FIRST_TOKEN_CEILING_MS", values.EngineFirstTokenCeilingMS, 25000},
		{"GATEWAY_ENGINE_INTER_CHUNK_STALL_MS", values.EngineInterChunkStallMS, 45000},
		{"GATEWAY_ENGINE_LOSER_GRACE_MS", values.EngineLoserGraceMS, 120000},
		{"GATEWAY_ENGINE_HEDGE_FIRST_TOKEN_FLOOR_MS", values.EngineHedgeFirstTokenFloorMS, 900},
		{"GATEWAY_CHAIN_SNAPSHOT_MAX_AGE_SECONDS", values.ChainSnapshotMaxAgeSeconds, 45},
	} {
		if field.got == nil || *field.got != field.want {
			t.Errorf("%s = %v, want %d", field.name, field.got, field.want)
		}
	}
}
