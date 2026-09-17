package engine

import (
	"context"
	"fmt"
	"testing"

	"devshard/cmd/gateway/limits"
	"devshard/transport"
)

// A body the gateway refused to send says nothing about the host: the host never saw the request. Classifying
// it as a transport fault charges the host a negative perf sample, pushes its cut-off breaker and counts
// against its failure rate for a decision this gateway made.
func TestABodyTheGatewayRefusedToSendIsNotChargedToTheHost(t *testing.T) {
	t.Parallel()

	terminal := classifyDispatchError(context.Background(), transport.ErrHostRequestTooLarge)

	if terminal != TerminalRequestTooLarge {
		t.Fatalf("terminal = %v, want TerminalRequestTooLarge", terminal)
	}
	if terminal.reason() != ReasonRequestTooLarge {
		t.Fatalf("reason = %q, want %q", terminal.reason(), ReasonRequestTooLarge)
	}
	verdict, moves := terminal.verdict()
	if !moves || verdict != limits.ModelOutcome {
		t.Fatalf("verdict = %v (moves %v), want a model outcome: the host's transport is not what refused", verdict, moves)
	}

	outcome := RaceOutcome{Model: testModel, Attempts: []AttemptOutcome{failedAttempt(TerminalRequestTooLarge)}}
	if exemption := outcome.sampleExemption(outcome.Attempts[0]); exemption != ExemptRequestTooLarge {
		t.Fatalf("sample exemption = %v, want ExemptRequestTooLarge", exemption)
	}
}

// SendOnly wraps the sentinel with the escrow and the nonce, so the classifier must read it through the wrap.
func TestARefusedBodyIsRecognisedThroughItsWrapping(t *testing.T) {
	t.Parallel()
	wrapped := fmt.Errorf("escrow %s nonce %d: %w", "escrow-1", 7, transport.ErrHostRequestTooLarge)

	if got := classifyDispatchError(context.Background(), wrapped); got != TerminalRequestTooLarge {
		t.Fatalf("terminal = %v, want the sentinel to survive wrapping", got)
	}
}
