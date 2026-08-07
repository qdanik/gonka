package api

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"devshard/cmd/gateway/filters"
)

// A non-streaming reply is held whole until Close, so a host that wins the race by producing one token
// first could otherwise send unlimited valid SSE and take the process out. The streaming path forwards
// as it goes and needs no such bound.
func TestNonStreamingRepliesAreBoundedInMemory(t *testing.T) {
	stream := newClientStream(httptest.NewRecorder(), "req-1", false, true, filters.LogprobIntent{})
	chunk := bytes.Repeat([]byte("x"), 1<<20)

	written := 0
	var failure error
	for written <= maxBufferedResponseBytes+len(chunk) {
		n, err := stream.Write(chunk)
		if err != nil {
			failure = err
			break
		}
		written += n
	}

	if failure == nil {
		t.Fatalf("wrote %d bytes with no limit, want a refusal past %d", written, maxBufferedResponseBytes)
	}
	if !strings.Contains(failure.Error(), "buffered response exceeds") {
		t.Fatalf("error = %v, want the buffered-response bound", failure)
	}
	if written > maxBufferedResponseBytes {
		t.Fatalf("buffered %d bytes, want no more than %d", written, maxBufferedResponseBytes)
	}
}

func TestNonStreamingRepliesUnderTheBoundAreKept(t *testing.T) {
	recorder := httptest.NewRecorder()
	stream := newClientStream(recorder, "req-1", false, true, filters.LogprobIntent{})

	if _, err := stream.Write([]byte(`data: {"choices":[{"message":{"content":"ok"}}]}` + "\n\n")); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	if !strings.Contains(recorder.Body.String(), `"content":"ok"`) {
		t.Fatalf("body = %q, want the assembled reply", recorder.Body.String())
	}
}

// A finished request cannot be asked afterwards how much of it the client actually received, so the
// stream has to count as it goes. Byte counts come from the recorder, not from what the caller handed
// in: the strip rewrites events on the way out, so the two differ.
func TestAStreamCountsWhatReachedTheClient(t *testing.T) {
	recorder := httptest.NewRecorder()
	stream := newClientStream(recorder, "req-1", true, true, filters.LogprobIntent{})

	if _, err := stream.Write([]byte(`data: {"choices":[{"delta":{"content":"ok"}}]}` + "\n\n")); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	written, terminated := stream.delivered()
	if want := int64(recorder.Body.Len()); written != want {
		t.Fatalf("delivered %d bytes, but %d reached the recorder", written, want)
	}
	if !terminated {
		t.Fatal("the terminator never went out, so a client would wait out its own timeout")
	}
}

func TestANonStreamingReplyCountsItsBody(t *testing.T) {
	recorder := httptest.NewRecorder()
	stream := newClientStream(recorder, "req-1", false, true, filters.LogprobIntent{})

	if _, err := stream.Write([]byte(`data: {"choices":[{"message":{"content":"ok"}}]}` + "\n\n")); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}

	written, _ := stream.delivered()
	if want := int64(recorder.Body.Len()); written != want {
		t.Fatalf("delivered %d bytes, but %d reached the recorder", written, want)
	}
	if written == 0 {
		t.Fatal("a served reply reported nothing delivered")
	}
}
