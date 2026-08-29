package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"devshard/cmd/gateway/filters"
)

func TestTheBudgetBoundsEveryBufferedReplyTogether(t *testing.T) {
	budget := NewBufferBudget(1 << 10)
	first := newClientStream(httptest.NewRecorder(), "req-1", false, false, filters.LogprobIntent{}, budget)
	second := newClientStream(httptest.NewRecorder(), "req-2", false, false, filters.LogprobIntent{}, budget)

	if _, err := first.Write(bytes.Repeat([]byte("x"), 800)); err != nil {
		t.Fatalf("the first reply was refused with room to spare: %v", err)
	}

	_, err := second.Write(bytes.Repeat([]byte("y"), 800))
	if !errors.Is(err, ErrResponseBufferFull) {
		t.Fatalf("the second reply was admitted past the ceiling: %v", err)
	}
}

func TestAFullBufferIsAnsweredAsTheShardHavingNoRoom(t *testing.T) {
	if got := statusForError(ErrResponseBufferFull); got != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

func TestDiscardGivesTheBudgetBackOnEveryPath(t *testing.T) {
	budget := NewBufferBudget(1 << 10)
	stream := newClientStream(httptest.NewRecorder(), "req-1", false, false, filters.LogprobIntent{}, budget)
	if _, err := stream.Write(bytes.Repeat([]byte("x"), 900)); err != nil {
		t.Fatalf("Write(): %v", err)
	}

	stream.discard()

	if held := budget.Held(); held != 0 {
		t.Errorf("the budget still holds %d bytes after the reply was discarded", held)
	}
	stream.discard()
	if held := budget.Held(); held != 0 {
		t.Errorf("a second discard took the budget below zero: %d held", held)
	}
}

func TestAStreamingReplyIsNotChargedToTheBudget(t *testing.T) {
	budget := NewBufferBudget(1 << 10)
	stream := newClientStream(httptest.NewRecorder(), "req-1", true, false, filters.LogprobIntent{}, budget)

	if _, err := stream.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")); err != nil {
		t.Fatalf("Write(): %v", err)
	}

	if held := budget.Held(); held != 0 {
		t.Errorf("a streaming reply was charged %d bytes", held)
	}
}

func TestRetuningDoesNotRepossessWhatIsAlreadyHeld(t *testing.T) {
	budget := NewBufferBudget(1 << 20)
	budget.reserve(900)

	budget.Retune(512)

	if held := budget.Held(); held != 900 {
		t.Errorf("held = %d after the ceiling was lowered, want the 900 still in flight", held)
	}
	if budget.reserve(1) {
		t.Error("the lowered ceiling still admitted a new reply")
	}
}

func TestAZeroCeilingHoldsNothingBack(t *testing.T) {
	budget := NewBufferBudget(0)

	if !budget.reserve(1 << 30) {
		t.Error("an unlimited budget refused a reply")
	}
}

func TestTheHandlerGivesTheBudgetBackOnceTheReplyIsAnswered(t *testing.T) {
	live := newHarness(t)

	for _, target := range []struct {
		name string
		body string
	}{
		{name: "a reply that was served", body: chatBody},
		{name: "a body the filters refused", body: "{"},
	} {
		t.Run(target.name, func(t *testing.T) {
			live.request(t, http.MethodPost, "/v1/chat/completions", target.body,
				map[string]string{"Authorization": "Bearer " + clientKey})

			if held := live.server.buffers.Held(); held != 0 {
				t.Errorf("the gateway still holds %d buffered bytes after answering", held)
			}
		})
	}
}
