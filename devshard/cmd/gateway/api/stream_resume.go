package api

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/filters"
	"devshard/cmd/gateway/journal"
)

type resumeCursor struct {
	requested bool
	streamID  string
	position  int
}

const (
	positionBase = 10
	positionBits = 31
)

type replayCall struct {
	requestID string
	key       cacheKey
	cursor    resumeCursor
	model     string
	apiKeyID  string
	prompt    requestPrompt
}

func parseResumeCursor(value string) (resumeCursor, error) {
	if value == resumeAbsent {
		return resumeCursor{}, nil
	}
	at := strings.LastIndex(value, resumeCursorSeparator)
	if at <= 0 {
		return resumeCursor{}, fmt.Errorf("%w: want %s: <stream id>%s<events received>", ErrInvalidResumeCursor, ResumeHeader, resumeCursorSeparator)
	}
	position, err := strconv.ParseUint(value[at+1:], positionBase, positionBits)
	if err != nil {
		return resumeCursor{}, fmt.Errorf("%w: the event count must be a non-negative integer", ErrInvalidResumeCursor)
	}
	return resumeCursor{requested: true, streamID: value[:at], position: int(position)}, nil
}

func (e cachedResponse) fromEvent(position int) cachedResponse {
	if position == 0 {
		return e
	}
	offset := e.bounds[position-1]
	rest := make([]int, len(e.bounds)-position)
	for i, end := range e.bounds[position:] {
		rest[i] = end - offset
	}
	e.body, e.bounds = e.body[offset:], rest
	return e
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request, call replayCall) bool {
	if call.cursor.requested {
		s.resume(w, r, call)
		return true
	}
	if entry, hit := s.cache.get(call.key, s.now()); hit {
		s.replayStored(w, call, entry, false)
		return true
	}
	for _, log := range s.streams.newestFirst(call.key) {
		if reader, adopted := log.attach(0, false); adopted {
			s.replayLive(w, r, call, log, reader)
			return true
		}
	}
	return false
}

func (s *Server) resume(w http.ResponseWriter, r *http.Request, call replayCall) {
	cursor := call.cursor
	if log := s.streams.find(call.key, cursor.streamID); log != nil {
		if cursor.position > log.eventCount() {
			s.refuseResume(w, call.model, resumeResultInvalidCursor, ErrInvalidResumeCursor)
			return
		}
		if reader, attached := log.attach(cursor.position, true); attached {
			s.announceResume(w, call.model, resumeSourceLive, cursor.position)
			s.replayLive(w, r, call, log, reader)
			return
		}
	}
	entry, hit := s.cache.get(call.key, s.now())
	if !hit || entry.streamID != cursor.streamID {
		s.refuseResume(w, call.model, resumeResultUnavailable, ErrResumeUnavailable)
		return
	}
	if cursor.position > len(entry.bounds) {
		s.refuseResume(w, call.model, resumeResultInvalidCursor, ErrInvalidResumeCursor)
		return
	}
	s.announceResume(w, call.model, resumeSourceCache, cursor.position)
	s.replayStored(w, call, entry.fromEvent(cursor.position), true)
}

func (s *Server) announceResume(w http.ResponseWriter, model, source string, position int) {
	s.countResume(model, source)
	w.Header().Set(ResumeFromHeader, strconv.Itoa(position))
	w.Header().Set(ResumeSourceHeader, source)
}

func (s *Server) refuseResume(w http.ResponseWriter, model, result string, cause error) {
	s.countResume(model, result)
	s.writeErrorFor(w, cause)
}

func (s *Server) countResume(model, result string) {
	if s.streamEvents != nil {
		s.streamEvents.Resume(model, result)
	}
}

func (s *Server) replayStored(w http.ResponseWriter, call replayCall, entry cachedResponse, resumed bool) {
	if entry.streamID != noStreamID {
		w.Header().Set(StreamIDHeader, entry.streamID)
	}
	written := serveCached(w, call.requestID, entry)
	resumeOf := noStreamID
	if resumed {
		resumeOf = entry.streamID
	}
	hitCount := call.prompt.countSoFar()
	s.events.RequestFinished(journal.RequestLine{
		RequestID: call.requestID, Model: call.model, APIKeyID: call.apiKeyID, EscrowID: entry.escrowID,
		ClientStream: entry.stream, Bytes: written, CacheHit: true, Resumed: resumed, ResumeOf: resumeOf,
		Outcome: engine.RaceOutcome{InputTokens: hitCount.tokens, InputTokensSource: hitCount.source},
	})
}

func (s *Server) replayLive(w http.ResponseWriter, r *http.Request, call replayCall, log *streamLog, reader *logReader) {
	defer reader.release()
	escrowID := log.escrow()
	writeChatHeaders(w.Header(), call.requestID, escrowID, contentTypeEventStream, true)
	w.Header().Set(StreamIDHeader, log.id)
	w.WriteHeader(http.StatusOK)
	written := pumpLog(r.Context(), w, reader)
	hitCount := call.prompt.countSoFar()
	s.events.RequestFinished(journal.RequestLine{
		RequestID: call.requestID, Model: call.model, APIKeyID: call.apiKeyID, EscrowID: cmp.Or(escrowID, log.escrow()),
		ClientStream: true, Bytes: written, Resumed: call.cursor.requested, ResumeOf: log.id,
		Outcome: engine.RaceOutcome{InputTokens: hitCount.tokens, InputTokensSource: hitCount.source},
	})
}

func pumpLog(ctx context.Context, w http.ResponseWriter, reader *logReader) (written int64) {
	controller := http.NewResponseController(w)
	for {
		events, done, dropped, changed := reader.next()
		if len(events) > 0 {
			sent, err := w.Write(events)
			written += int64(sent)
			if err != nil {
				return written
			}
			_ = controller.Flush()
			continue
		}
		if done {
			if dropped {
				sent, _ := w.Write(errorEvent(ErrStreamDropped))
				written += int64(sent)
				_ = controller.Flush()
			}
			return written
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return written
		}
	}
}

func (s *Server) settleOrphan(client *clientStream, log *streamLog, model string) func(engine.RaceOutcome) {
	return func(outcome engine.RaceOutcome) {
		client.finishDetached(outcome)
		s.concludeStream(log, model, outcome.EscrowID, cmp.Or(outcome.Failure(), client.rewriteFailure()))
	}
}

func (s *Server) concludeStream(log *streamLog, model, escrowID string, unstorable error) {
	entry, refusal := log.conclude(escrowID, unstorable)
	if refusal == filters.CacheStorable {
		refusal = s.cache.put(log.key, entry, s.now())
	}
	s.countCacheResult(model, refusal)
	if refusal == filters.CacheRefusedUnfinished {
		s.events.ReplyNotCached(journal.RequestLine{RequestID: log.id, Model: model, EscrowID: cmp.Or(escrowID, log.escrow())})
	}
}

// StreamStats reports the streams nobody is reading, for the metrics collector.
func (s *Server) StreamStats() (orphans, bytes int64) { return s.streams.stats() }
