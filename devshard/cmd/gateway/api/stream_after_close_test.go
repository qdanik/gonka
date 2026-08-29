package api

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"devshard/cmd/gateway/filters"
)

// A recorder stands in for the real http.ResponseWriter, which Go invalidates the moment the handler
// returns. In production that invalidation is a nil buffer and the flush below is a panic.
func closedStream(t *testing.T) *clientStream {
	t.Helper()
	stream := newClientStream(httptest.NewRecorder(), "req-1", true, false, filters.LogprobIntent{}, nil)
	if _, err := stream.Write([]byte("data: {\"choices\":[]}\n\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return stream
}

// The real writer panics when flushed after the handler returned, so the count matters, not the bytes.
type flushSpy struct {
	http.ResponseWriter
	flushes int
}

func (f *flushSpy) Flush() { f.flushes++ }

func TestClientStreamStopsFlushingAfterClose(t *testing.T) {
	spy := &flushSpy{ResponseWriter: httptest.NewRecorder()}
	stream := newClientStream(spy, "req-1", true, false, filters.LogprobIntent{}, nil)
	if _, err := stream.Write([]byte("data: {\"choices\":[]}\n\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	afterClose := spy.flushes

	stream.Flush()

	if spy.flushes != afterClose {
		t.Errorf("flushed %d more times after close: in production that is the nil-buffer panic",
			spy.flushes-afterClose)
	}
}

func TestClientStreamIsInertAfterClose(t *testing.T) {
	stream := closedStream(t)
	written, _ := stream.delivered()

	stream.Flush()
	count, err := stream.Write([]byte("data: {\"late\":true}\n\n"))
	if err != nil {
		t.Fatalf("a late write returned %v, want the chunk swallowed", err)
	}
	if count == 0 {
		t.Error("a late write must report the chunk consumed so the producer stops, not retries")
	}
	if after, _ := stream.delivered(); after != written {
		t.Errorf("delivered grew from %d to %d after close: a goroutine outliving the handler reached the client", written, after)
	}
}

func TestClientStreamCloseIsIdempotent(t *testing.T) {
	stream := closedStream(t)
	written, _ := stream.delivered()

	if err := stream.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	if after, _ := stream.delivered(); after != written {
		t.Errorf("a second Close wrote %d more bytes, want none", after-written)
	}
}

// Two attempt goroutines racing on one stream corrupted the rewriter's carry, which surfaced as a
// slice bounds panic rather than a wrong answer.
func TestClientStreamSurvivesConcurrentWriters(t *testing.T) {
	stream := newClientStream(httptest.NewRecorder(), "req-1", true, false, filters.LogprobIntent{}, nil)

	var waiting sync.WaitGroup
	for range 8 {
		waiting.Go(func() {
			for range 50 {
				_, _ = stream.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"))
				stream.Flush()
			}
		})
	}
	waiting.Wait()

	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
