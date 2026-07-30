package engine

import (
	"context"
	"io"
	"net/http"
	"time"

	"devshard/cmd/gateway/config"
)

// defaultDrainTimeout must exceed every deadline a race arms for itself, so the only host it ever ends
// is one that streams forever without tripping any of them.
const defaultDrainTimeout = 40 * time.Minute

func DrainTimeoutFromConfig(stream config.Stream) time.Duration {
	return time.Duration(stream.DrainTimeoutSeconds) * time.Second
}

// drain holds a race's two contexts. The client's cancellation is deliberately dropped rather than
// inherited: the receipt, the response the session applies to its own state and the vote that settles
// a committed nonce all have to complete after the client is gone. The drain deadline is what bounds
// the race from that point on.
type drain struct {
	client  context.Context
	race    context.Context
	timeout time.Duration
}

func newDrain(clientCtx context.Context, timeout time.Duration) drain {
	if timeout <= 0 {
		timeout = defaultDrainTimeout
	}
	return drain{
		client:  clientCtx,
		race:    context.WithoutCancel(clientCtx),
		timeout: timeout,
	}
}

func (d drain) clientGone() <-chan struct{} { return d.client.Done() }

func (d drain) clientErr() error { return d.client.Err() }

func (d drain) deadline(clientGoneAt time.Time) time.Time {
	if clientGoneAt.IsZero() {
		return time.Time{}
	}
	return clientGoneAt.Add(d.timeout)
}

func (d drain) gate(client io.Writer) io.Writer {
	if client == nil {
		return nil
	}
	return clientStream{client: client, gone: d.clientGone()}
}

// clientStream reports a departed client's bytes as written instead of failing: a write error would
// end the attempt carrying them, and the host that earned the crown still owes its receipt.
type clientStream struct {
	client io.Writer
	gone   <-chan struct{}
}

func (s clientStream) Write(chunk []byte) (int, error) {
	select {
	case <-s.gone:
		return len(chunk), nil
	default:
	}
	return s.client.Write(chunk)
}

func (s clientStream) Flush() {
	if flusher, ok := s.client.(http.Flusher); ok {
		flusher.Flush()
	}
}
