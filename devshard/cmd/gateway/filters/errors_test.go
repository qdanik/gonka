package filters

import (
	"errors"
	"net/http"
	"testing"
)

func TestErrorsStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		fallback int
		want     int
	}{
		{"Reject defaults to 400", Reject("boom"), 999, http.StatusBadRequest},
		{"RejectWithStatus keeps the given status", RejectWithStatus(http.StatusRequestEntityTooLarge, "too big"), 400, http.StatusRequestEntityTooLarge},
		{"WrapReject defaults to 400", WrapReject(errors.New("inner")), 999, http.StatusBadRequest},
		{"non-RejectError falls back", errors.New("plain"), 418, 418},
		{"nil error falls back", nil, 500, 500},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := ErrorStatus(testCase.err, testCase.fallback); got != testCase.want {
				t.Errorf("ErrorStatus() = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestErrorsRejectFormatsMessage(t *testing.T) {
	err := Reject("field %q must be %d", "n", 5)
	want := `field "n" must be 5`
	if err.Error() != want {
		t.Errorf("Error() = %q, want %q", err.Error(), want)
	}
}

func TestErrorsUnwrapReturnsNilWithoutWrapped(t *testing.T) {
	err := Reject("boom")
	var rejectErr *RejectError
	if !errors.As(err, &rejectErr) {
		t.Fatalf("Reject() did not produce a *RejectError")
	}
	if unwrapped := rejectErr.Unwrap(); unwrapped != nil {
		t.Errorf("Unwrap() = %v, want nil", unwrapped)
	}
}

func TestErrorsWrapRejectPreservesChain(t *testing.T) {
	sentinel := errors.New("sentinel failure")
	wrapped := WrapReject(sentinel)

	if !errors.Is(wrapped, sentinel) {
		t.Fatalf("errors.Is(wrapped, sentinel) = false, want true")
	}
	if wrapped.Error() != sentinel.Error() {
		t.Errorf("Error() = %q, want %q", wrapped.Error(), sentinel.Error())
	}
	if got := ErrorStatus(wrapped, 0); got != http.StatusBadRequest {
		t.Errorf("ErrorStatus(wrapped) = %d, want %d", got, http.StatusBadRequest)
	}
}

func TestErrorsWrapRejectNilReturnsNil(t *testing.T) {
	if err := WrapReject(nil); err != nil {
		t.Errorf("WrapReject(nil) = %v, want nil", err)
	}
}

// Pins the whitelist text verbatim; every whitelist_ golden asserts this exact string.
func TestErrorsUnsupportedParameterMessageExactText(t *testing.T) {
	got := unsupportedParameterMessage("weird_field")
	want := `Chat completions parameter "weird_field" is currently rejected by the Gonka network. Some non-standard parameters can crash the vLLM engine on Gonka Host MLNodes, so the network rejects parameters that are not explicitly supported (see: https://github.com/gonka-ai/gonka/blob/main/docs/chat-api/README.md). If you do not need this parameter, remove it from the request; if you need it, file a request at https://github.com/gonka-ai/gonka/issues`
	if got != want {
		t.Errorf("unsupportedParameterMessage() mismatch\n got:  %q\n want: %q", got, want)
	}
}
