package env

import (
	"errors"
	"strings"
	"testing"
)

func TestLoadReturnsNilForUnsetVariables(t *testing.T) {
	// t.Setenv only clears the two probes (empty = unset); the test asserts pristine-environment behavior.
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
	if values.ChainREST == nil || *values.ChainREST != "http://example.test:1317" {
		t.Fatalf("ChainREST = %v, want set", values.ChainREST)
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
