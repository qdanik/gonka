package filters

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type parityCase struct {
	RoutedModel      string          `json:"routed_model"`
	Admin            bool            `json:"admin"`
	DefaultMaxTokens uint64          `json:"default_max_tokens"`
	MaxTokensCap     uint64          `json:"max_tokens_cap"`
	Body             json.RawMessage `json:"body"`
}

type parityGolden struct {
	Status     string `json:"status"`
	BodyBase64 string `json:"body_base64"`
	HTTPStatus int    `json:"http_status"`
	Message    string `json:"message"`
}

// TestGoldenParity requires every corpus case to reproduce the recorded
// golden's exact bytes (ok) or exact status+message (rejected).
func TestGoldenParity(t *testing.T) {
	corpusEntries, err := os.ReadDir(filepath.Join("testdata", "corpus"))
	if err != nil {
		t.Fatalf("reading corpus: %v", err)
	}
	if len(corpusEntries) == 0 {
		t.Fatal("corpus is empty")
	}
	for _, entry := range corpusEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		t.Run(strings.TrimSuffix(entry.Name(), ".json"), func(t *testing.T) {
			rawCase, err := os.ReadFile(filepath.Join("testdata", "corpus", entry.Name()))
			if err != nil {
				t.Fatalf("reading case: %v", err)
			}
			var testCase parityCase
			if err := json.Unmarshal(rawCase, &testCase); err != nil {
				t.Fatalf("parsing case: %v", err)
			}
			rawGolden, err := os.ReadFile(filepath.Join("testdata", "goldens", entry.Name()))
			if err != nil {
				t.Fatalf("reading golden (regenerate with: go test ./cmd/devshardctl/ -tags goldengen -run TestGenerateFilterGoldens): %v", err)
			}
			var golden parityGolden
			if err := json.Unmarshal(rawGolden, &golden); err != nil {
				t.Fatalf("parsing golden: %v", err)
			}

			result, err := NormalizeRequest(testCase.Body, Options{
				Admin:            testCase.Admin,
				DefaultMaxTokens: testCase.DefaultMaxTokens,
				MaxTokensCap:     testCase.MaxTokensCap,
				RoutedModel:      testCase.RoutedModel,
			})

			switch golden.Status {
			case "ok":
				if err != nil {
					t.Fatalf("golden accepted, result rejected: %v", err)
				}
				wantBody, decodeErr := base64.StdEncoding.DecodeString(golden.BodyBase64)
				if decodeErr != nil {
					t.Fatalf("decoding golden body: %v", decodeErr)
				}
				// Forcing the stream is the one deliberate divergence from the old pipeline, so both
				// sides drop it and parity covers the rest.
				if got, want := withoutStreamFields(t, result.Body), withoutStreamFields(t, wantBody); !bytes.Equal(got, want) {
					t.Fatalf("body mismatch\n golden: %s\n got: %s", want, got)
				}
			case "rejected":
				if err == nil {
					t.Fatalf("golden rejected (%d %q), result accepted: %s", golden.HTTPStatus, golden.Message, result.Body)
				}
				if got := ErrorStatus(err, 400); got != golden.HTTPStatus {
					t.Fatalf("status mismatch: golden %d, got %d (%v)", golden.HTTPStatus, got, err)
				}
				if err.Error() != golden.Message {
					t.Fatalf("message mismatch\n golden: %q\n got: %q", golden.Message, err.Error())
				}
			default:
				t.Fatalf("golden has unknown status %q", golden.Status)
			}
		})
	}
}

func withoutStreamFields(t *testing.T, body []byte) []byte {
	t.Helper()
	document, err := ParseDocument(body)
	if err != nil {
		t.Fatalf("parsing body %s: %v", body, err)
	}
	document.Delete("stream")
	document.Delete("stream_options")
	stripped, err := document.Marshal()
	if err != nil {
		t.Fatalf("marshalling body %s: %v", body, err)
	}
	return stripped
}
