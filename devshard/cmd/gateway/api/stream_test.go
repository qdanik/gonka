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
	stream := newClientStream(httptest.NewRecorder(), "req-1", false, true, filters.LogprobIntent{}, nil)
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
	stream := newClientStream(recorder, "req-1", false, true, filters.LogprobIntent{}, nil)

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
	stream := newClientStream(recorder, "req-1", true, true, filters.LogprobIntent{}, nil)

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
	stream := newClientStream(recorder, "req-1", false, true, filters.LogprobIntent{}, nil)

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

// A caller that reads the status must not be told the request succeeded and then handed a body saying
// it did not. The old gateway chose the status from the assembled body for exactly this reason.
func TestABodyTheAssemblerCouldNotFoldIsNotServedAsSuccess(t *testing.T) {
	tests := []struct {
		name       string
		events     string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "a stream that carried no payload",
			events:     "\n\n",
			wantStatus: 502,
			wantBody:   string(filters.NoResponseDataBody),
		},
		{
			name:       "an answer the host actually produced",
			events:     "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: [DONE]\n\n",
			wantStatus: 200,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			stream := newClientStream(recorder, "req-1", false, true, filters.LogprobIntent{}, nil)
			if _, err := stream.Write([]byte(testCase.events)); err != nil {
				t.Fatalf("Write(): %v", err)
			}

			if err := stream.Close(); err != nil {
				t.Fatalf("Close(): %v", err)
			}

			if recorder.Code != testCase.wantStatus {
				t.Errorf("status = %d, want %d for body %s", recorder.Code, testCase.wantStatus, recorder.Body)
			}
			if testCase.wantBody != "" && strings.TrimSpace(recorder.Body.String()) != testCase.wantBody {
				t.Errorf("body = %s, want %s", recorder.Body, testCase.wantBody)
			}
		})
	}
}
