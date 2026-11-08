package streamevidence

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// Test flow:
//  1. Read the terminal-error SSE fixture `testdata/newlineless_final_error.sse`, whose only line is an error event with no final newline.
//  2. Take its `DataLines` and serialize them with `Payload`.
//  3. Assert the bytes equal `testdata/newlineless_final_error.json`.
func TestPayloadOfATerminalErrorStreamMatchesTheGolden(t *testing.T) {
	events, err := os.ReadFile(filepath.Join("testdata", "newlineless_final_error.sse"))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "newlineless_final_error.json"))
	if err != nil {
		t.Fatalf("reading golden: %v", err)
	}

	payload, err := Payload(DataLines(events))
	if err != nil {
		t.Fatalf("Payload: %v", err)
	}

	if !bytes.Equal(payload, want) {
		t.Fatalf("Payload = %s, want %s", payload, want)
	}
}

// Test flow:
//  1. Serialize an error line whose message carries `<`, `>` and `&`, followed by `[DONE]`, with `Payload`.
//  2. Assert the bytes are the JSON object whose `events` array holds both lines in order, with quotes escaped and `<`, `>` and `&` written as `\u003c`, `\u003e` and `\u0026`.
func TestPayloadKeepsTheLinesVerbatimInOrder(t *testing.T) {
	payload, err := Payload([]string{`data: {"error":{"message":"<think> & boom"}}`, "data: [DONE]"})
	if err != nil {
		t.Fatalf("Payload: %v", err)
	}

	want := `{"events":["data: {\"error\":{\"message\":\"\u003cthink\u003e \u0026 boom\"}}","data: [DONE]"]}`
	if string(payload) != want {
		t.Fatalf("Payload = %s, want %s", payload, want)
	}
}
