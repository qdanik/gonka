package env

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func TestLoadReturnsNilForUnsetVariables(t *testing.T) {
	// t.Setenv only clears the two probes (empty = unset); the test asserts pristine-environment behavior.
	t.Setenv("GATEWAY_PORT", "")

	values, err := Load()
	if err != nil {
		t.Fatalf("Load() with clean environment: unexpected error: %v", err)
	}
	if values.Port != nil {
		t.Fatalf("Port = %v, want nil for unset variable", *values.Port)
	}
}

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

func TestLoadRejectsInvalidPoCMode(t *testing.T) {
	t.Setenv("GATEWAY_POC_MODE", "aggressive")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_POC_MODE") {
		t.Fatalf("want error naming GATEWAY_POC_MODE, got %v", err)
	}
}

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

// The shipped deployment template still spells these the devshardctl way, so a gateway that read only
// its own names would start on defaults: wrong port, no API keys, no chain endpoint.
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
	})
}

// The fallback is the devshardctl spelling, as every other entry in the table is. A node still on the
// gateway's own former name must be migrated: nothing reads it, and the gateway starts serving nothing.
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

// Three of the four rotation knobs were reachable from the environment and this one was not, so an
// operator reading the deployment file concluded it did not exist while it quietly ran on its default.
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

// An escrow records the name of its key variable when it is created, so renaming the variable in the
// deployment leaves the stored record pointing at a name nothing sets and the escrow goes inactive.
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

// The fallback must not invent a key: an unset pair still fails, and the error names the recorded
// variable so an operator fixes the record rather than guessing.
func TestASigningKeyWithNeitherNameSetStillFails(t *testing.T) {
	if _, err := PrivateKey("DEVSHARD_PRIVATE_KEY"); !errors.Is(err, ErrPrivateKeyMissing) {
		t.Fatalf("PrivateKey() = %v, want ErrPrivateKeyMissing", err)
	}
}
