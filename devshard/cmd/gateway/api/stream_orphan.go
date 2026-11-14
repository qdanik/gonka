package api

import (
	"context"
	"errors"

	"devshard/cmd/gateway/engine"
)

func (c *clientStream) recordTo(log *streamLog, settle func(engine.RaceOutcome)) {
	c.log, c.settle = log, settle
	c.writer.Header().Set(StreamIDHeader, log.id)
}

func (c *clientStream) rewriteFailure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rewriteErr
}

func (c *clientStream) socketDetached() bool { return !c.log.socketAttached() }

func (c *clientStream) hasDeparted(ctx context.Context, cause error) bool {
	return !errors.Is(cause, engine.ErrReplyRefused) && (ctx.Err() != nil || c.socketDetached())
}

func (c *clientStream) handedOff() bool {
	c.handoff.Lock()
	defer c.handoff.Unlock()
	return c.orphaned
}

func (c *clientStream) orphan() bool {
	c.handoff.Lock()
	if c.log == nil || (c.finished && !c.reported) {
		c.handoff.Unlock()
		return false
	}
	c.orphaned = true
	settleNow, final := c.reported && !c.settled, c.final
	c.settled = c.settled || settleNow
	c.handoff.Unlock()
	c.log.detachSocket()
	if settleNow {
		c.settle(final)
	}
	return true
}

func (c *clientStream) reportOutcome(outcome engine.RaceOutcome) {
	c.handoff.Lock()
	c.reported, c.final = true, outcome
	settleNow := c.orphaned && !c.settled
	c.settled = c.settled || settleNow
	c.handoff.Unlock()
	if !settleNow {
		return
	}
	if c.mu.TryLock() {
		c.mu.Unlock()
		c.settle(outcome)
		return
	}
	go c.settle(outcome)
}

func (c *clientStream) raceFinished() {
	c.handoff.Lock()
	c.finished = true
	abandon := c.orphaned && !c.settled
	c.settled = c.settled || abandon
	c.handoff.Unlock()
	if abandon {
		c.log.drop(orphanFailed)
	}
}

func (c *clientStream) finishDetached(outcome engine.RaceOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	defer func() { c.closed = true }()
	if failure := outcome.Failure(); failure != nil {
		_ = c.failStreamLocked(failure)
		return
	}
	_ = c.closeStreamLocked()
}

func departedVerdict(client *clientStream) string {
	if written, _ := client.delivered(); written > 0 {
		return deliveryFailedMidStream
	}
	return deliveryFailedBeforeFirstByte
}
