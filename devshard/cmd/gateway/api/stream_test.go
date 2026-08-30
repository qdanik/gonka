package api

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"devshard/cmd/gateway/filters"
)

func TestAnUnterminatedEventIsBoundedInMemory(t *testing.T) {
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
	if !errors.Is(failure, filters.ErrStreamCarryOverflow) {
		t.Fatalf("error = %v, want the carry bound", failure)
	}
	if written > maxBufferedResponseBytes {
		t.Fatalf("held %d bytes, want no more than %d", written, maxBufferedResponseBytes)
	}
}

// The reply is folded as it arrives, so what a non-streaming client costs is the answer it will be
// given, not the stream it was assembled from.
func TestAFoldedReplyHoldsTheAnswerRatherThanTheStream(t *testing.T) {
	stream := newClientStream(httptest.NewRecorder(), "req-1", false, true, filters.LogprobIntent{}, nil)

	raw := 0
	for token := range 200 {
		event := fmt.Appendf(nil, "data: {\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"%d \"},\"logprobs\":{\"content\":[{\"token\":\"%d\",\"logprob\":-0.5,\"top_logprobs\":[{\"token\":\"a\",\"logprob\":-1.5},{\"token\":\"b\",\"logprob\":-2.5},{\"token\":\"c\",\"logprob\":-3.5}]}]}}]}\n\n", token, token)
		if _, err := stream.Write(event); err != nil {
			t.Fatalf("Write(): %v", err)
		}
		raw += len(event)
	}

	held := stream.folder.Held()
	if held >= int64(raw)/4 {
		t.Errorf("holding %d bytes of a %d-byte stream: the logprobs this client never asked for are being kept", held, raw)
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

// A reply the shared budget refuses must be shed, not served. The reply has to clear the fold's own
// measuring step, which is what the budget is charged from: below it the fold reports nothing held.
func TestAReplyPastTheBufferBudgetIsRefused(t *testing.T) {
	recorder := httptest.NewRecorder()
	stream := newClientStream(recorder, "req-1", false, true, filters.LogprobIntent{}, NewBufferBudget(1))

	oversized := `data: {"choices":[{"delta":{"content":"` + strings.Repeat("x", 512<<10) + `"}}]}` + "\n\n"
	if _, err := stream.Write([]byte(oversized)); !errors.Is(err, ErrResponseBufferFull) {
		t.Fatalf("Write() = %v, want %v", err, ErrResponseBufferFull)
	}
	if err := stream.Close(); !errors.Is(err, ErrResponseBufferFull) {
		t.Fatalf("Close() = %v, want %v", err, ErrResponseBufferFull)
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", recorder.Code)
	}
	if strings.Contains(recorder.Body.String(), "xxxx") {
		t.Error("the refused reply was served anyway")
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
