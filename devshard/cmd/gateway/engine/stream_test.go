package engine

import (
	"bytes"
	"errors"
	"testing"

	"go.uber.org/goleak"

	"devshard/cmd/gateway/filters"
)

type recordingClient struct {
	writes  [][]byte
	failure error
}

func (c *recordingClient) Write(chunk []byte) (int, error) {
	if c.failure != nil {
		return 0, c.failure
	}
	c.writes = append(c.writes, append([]byte(nil), chunk...))
	return len(chunk), nil
}

func (c *recordingClient) forwarded() []byte { return bytes.Join(c.writes, nil) }

type flushingClient struct {
	recordingClient
	flushes int
}

func (c *flushingClient) Flush() { c.flushes++ }

type crownDesk struct {
	requests chan crownRequest
	claimed  chan uint64
	stop     chan struct{}
}

func newCrownDesk(t *testing.T, verdicts ...streamVerdict) *crownDesk {
	t.Helper()
	desk := &crownDesk{
		requests: make(chan crownRequest),
		claimed:  make(chan uint64, len(verdicts)),
		stop:     make(chan struct{}),
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for _, verdict := range verdicts {
			select {
			case request := <-desk.requests:
				desk.claimed <- request.Nonce
				request.Reply <- verdict
			case <-desk.stop:
				return
			}
		}
		<-desk.stop
	}()
	t.Cleanup(func() { close(desk.stop); <-finished })
	return desk
}

func guardGoroutines(t *testing.T) {
	t.Helper()
	existing := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, existing) })
}

func TestWinnerWriterFlushesPrefixInOrderOnCrowning(t *testing.T) {
	guardGoroutines(t)
	desk := newCrownDesk(t, streamWinner)
	client := &recordingClient{}
	writer := newWinnerWriter(7, client, desk.requests, nil)

	for _, chunk := range []string{"role\n\n", "comment\n\n"} {
		if err := writer.Write([]byte(chunk), false); err != nil {
			t.Fatalf("Write(%q) = %v", chunk, err)
		}
	}
	if forwarded := client.forwarded(); len(forwarded) != 0 {
		t.Fatalf("pre-content bytes reached the client: %q", forwarded)
	}
	if err := writer.Write([]byte("content\n\n"), true); err != nil {
		t.Fatalf("Write(content) = %v", err)
	}
	if err := writer.Write([]byte("more\n\n"), true); err != nil {
		t.Fatalf("Write(more) = %v", err)
	}

	want := "role\n\ncomment\n\ncontent\n\nmore\n\n"
	if got := string(client.forwarded()); got != want {
		t.Fatalf("forwarded %q, want %q", got, want)
	}
	if claimed := <-desk.claimed; claimed != 7 {
		t.Fatalf("claimed nonce %d, want 7", claimed)
	}
	if len(desk.claimed) != 0 {
		t.Fatalf("crown claimed %d extra times", len(desk.claimed))
	}
}

func TestWinnerWriterCappedPrefixStillWins(t *testing.T) {
	guardGoroutines(t)
	tests := []struct {
		name       string
		prefixes   []int
		wantPrefix int
	}{
		{name: "at cap", prefixes: []int{filters.MaxStreamCarryBytes / 2, filters.MaxStreamCarryBytes / 2}, wantPrefix: filters.MaxStreamCarryBytes},
		{name: "one byte over cap", prefixes: []int{filters.MaxStreamCarryBytes, 1}, wantPrefix: 0},
		{name: "single oversized chunk", prefixes: []int{filters.MaxStreamCarryBytes + 1}, wantPrefix: 0},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			desk := newCrownDesk(t, streamWinner)
			client := &recordingClient{}
			writer := newWinnerWriter(1, client, desk.requests, nil)

			for _, size := range testCase.prefixes {
				if err := writer.Write(bytes.Repeat([]byte("p"), size), false); err != nil {
					t.Fatalf("Write(prefix) = %v", err)
				}
			}
			if err := writer.Write([]byte("content\n\n"), true); err != nil {
				t.Fatalf("Write(content) = %v", err)
			}

			forwarded := client.forwarded()
			if len(forwarded) != testCase.wantPrefix+len("content\n\n") {
				t.Fatalf("forwarded %d bytes, want %d", len(forwarded), testCase.wantPrefix+len("content\n\n"))
			}
			if !bytes.HasSuffix(forwarded, []byte("content\n\n")) {
				t.Fatal("client stream does not end at the crowning chunk")
			}
			if !bytes.Equal(forwarded[:testCase.wantPrefix], bytes.Repeat([]byte("p"), testCase.wantPrefix)) {
				t.Fatal("flushed prefix is not the buffered bytes")
			}
		})
	}
}

func TestWinnerWriterDiscardsLoserBytes(t *testing.T) {
	guardGoroutines(t)
	desk := newCrownDesk(t, streamSuppressed)
	client := &recordingClient{}
	writer := newWinnerWriter(2, client, desk.requests, nil)

	chunks := []struct {
		payload    string
		hasContent bool
	}{
		{payload: "role\n\n"},
		{payload: "content\n\n", hasContent: true},
		{payload: "more\n\n", hasContent: true},
		{payload: "tail\n\n"},
	}
	for _, chunk := range chunks {
		if err := writer.Write([]byte(chunk.payload), chunk.hasContent); err != nil {
			t.Fatalf("Write(%q) = %v", chunk.payload, err)
		}
	}

	if forwarded := client.forwarded(); len(forwarded) != 0 {
		t.Fatalf("loser bytes reached the client: %q", forwarded)
	}
	if writer.prefix != nil {
		t.Fatal("loser kept its buffered prefix")
	}
	if len(desk.claimed) != 1 {
		t.Fatalf("crown claimed %d times, want 1", len(desk.claimed))
	}
}

func TestWinnerWriterAbandonsClaimWhenRaceEnds(t *testing.T) {
	guardGoroutines(t)
	raceDone := make(chan struct{})
	close(raceDone)
	client := &recordingClient{}
	writer := newWinnerWriter(3, client, make(chan crownRequest), raceDone)

	if err := writer.Write([]byte("role\n\n"), false); err != nil {
		t.Fatalf("Write(role) = %v", err)
	}
	if err := writer.Write([]byte("content\n\n"), true); err != nil {
		t.Fatalf("Write(content) = %v", err)
	}
	if forwarded := client.forwarded(); len(forwarded) != 0 {
		t.Fatalf("bytes reached the client after the race ended: %q", forwarded)
	}
	if writer.verdict != streamSuppressed {
		t.Fatalf("verdict = %d, want suppressed", writer.verdict)
	}
}

func TestWinnerWriterAbandonForfeitsTheClient(t *testing.T) {
	guardGoroutines(t)
	desk := newCrownDesk(t, streamWinner)
	client := &recordingClient{}
	writer := newWinnerWriter(4, client, desk.requests, nil)

	if err := writer.Write([]byte("role\n\n"), false); err != nil {
		t.Fatalf("Write(role) = %v", err)
	}
	writer.Abandon()
	if err := writer.Write([]byte("content\n\n"), true); err != nil {
		t.Fatalf("Write(content) = %v", err)
	}
	if forwarded := client.forwarded(); len(forwarded) != 0 {
		t.Fatalf("an abandoned attempt reached the client: %q", forwarded)
	}
	if len(desk.claimed) != 0 {
		t.Fatal("an abandoned attempt claimed the crown")
	}
}

func TestWinnerWriterFlushOnlyAfterCrowning(t *testing.T) {
	guardGoroutines(t)
	desk := newCrownDesk(t, streamWinner)
	client := &flushingClient{}
	writer := newWinnerWriter(5, client, desk.requests, nil)

	writer.Flush()
	if client.flushes != 0 {
		t.Fatalf("flushed %d times before crowning", client.flushes)
	}
	if err := writer.Write([]byte("content\n\n"), true); err != nil {
		t.Fatalf("Write(content) = %v", err)
	}
	writer.Flush()
	if client.flushes != 1 {
		t.Fatalf("flushed %d times after crowning, want 1", client.flushes)
	}
}

func TestWinnerWriterReportsClientFailure(t *testing.T) {
	guardGoroutines(t)
	desk := newCrownDesk(t, streamWinner)
	failure := errors.New("client gone")
	client := &recordingClient{failure: failure}
	writer := newWinnerWriter(6, client, desk.requests, nil)

	if err := writer.Write([]byte("role\n\n"), false); err != nil {
		t.Fatalf("Write(role) = %v", err)
	}
	if err := writer.Write([]byte("content\n\n"), true); !errors.Is(err, failure) {
		t.Fatalf("Write(content) = %v, want %v", err, failure)
	}
}

func TestWinnerWriterWithoutClientConsumesBytes(t *testing.T) {
	guardGoroutines(t)
	desk := newCrownDesk(t, streamWinner)
	writer := newWinnerWriter(8, nil, desk.requests, nil)

	if err := writer.Write([]byte("role\n\n"), false); err != nil {
		t.Fatalf("Write(role) = %v", err)
	}
	if err := writer.Write([]byte("content\n\n"), true); err != nil {
		t.Fatalf("Write(content) = %v", err)
	}
	if writer.prefix != nil {
		t.Fatal("prefix outlived a crowned attempt with no client")
	}
	writer.Flush()
}
