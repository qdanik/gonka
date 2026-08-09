package engine

import (
	"devshard/logging"
)

// answer settles one attempt's claim on the client stream; a suspicious host's claim is held rather than
// refused, because a refusal is permanent. See gateway-speculative-race.md, "Crowning".
func (c *raceCoordinator) answer(claim crownRequest) {
	switch attempt := c.byNonce[claim.Nonce]; {
	case attempt == nil || c.winner != nil:
		claim.Reply <- streamSuppressed
	case attempt.suspicious:
		c.claims = append(c.claims, claim)
	default:
		c.crownWinner(attempt, crownFirstClaim)
		claim.Reply <- streamWinner
	}
}

// settleClaims answers the held claims once the race can tell whether a rival will serve; alone, a
// suspicious host is crowned. See gateway-speculative-race.md, "Crown denial".
func (c *raceCoordinator) settleClaims() {
	if len(c.claims) == 0 {
		return
	}
	if c.winner == nil {
		if c.rivalPossible() {
			return
		}
		c.crownWinner(c.byNonce[c.claims[0].Nonce], crownNoRival)
	}
	for _, claim := range c.claims {
		verdict := streamSuppressed
		if c.byNonce[claim.Nonce] == c.winner {
			verdict = streamWinner
		}
		claim.Reply <- verdict
	}
	c.claims = c.claims[:0]
}

// crownWinner is the single place one attempt becomes the client's answer, so the reason travels with it.
func (c *raceCoordinator) crownWinner(attempt *liveAttempt, reason string) {
	c.winner = attempt
	logging.Info("attempt crowned",
		"request", c.request.RequestID, "escrow", c.escrowID, "nonce", attempt.nonce,
		"participant", attempt.participant, "reason", reason)
}

// rivalPossible reports an attempt other than the held claimants that could still be crowned. See
// gateway-speculative-race.md, "Crown denial".
func (c *raceCoordinator) rivalPossible() bool {
	return c.pending > len(c.claims) || c.picking() || c.moreImmediate > 0
}

// attemptSink is one attempt's byte path. It runs only on that attempt's goroutine.
type attemptSink struct {
	winner     *winnerWriter
	hasContent bool
}

func (s *attemptSink) Write(chunk []byte) (int, error) {
	if err := s.winner.Write(chunk, s.hasContent); err != nil {
		return 0, err
	}
	return len(chunk), nil
}
func (s *attemptSink) Flush() { s.winner.Flush() }

// contentGate hands the sink beside it the one fact an io.Writer signature cannot carry. Classify is
// called immediately before the Write of the same chunk, on the same goroutine.
type contentGate struct {
	streamClassifier
	sink *attemptSink
}

func (g contentGate) Classify(chunk []byte) chunkFacts {
	facts := g.streamClassifier.Classify(chunk)
	g.sink.hasContent = facts.Content
	return facts
}
