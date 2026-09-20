package engine

import (
	"bytes"
	"encoding/json"
	"strings"

	"common/completionapi"
)

// maxMissProofBytes bounds what one attempt retains as proof.
const maxMissProofBytes = 64 * 1024

// MissProof is the evidence a verifier recomputes: the host's own event lines, hashed. See race.md, "Error misses".
type MissProof struct {
	ResponsePayload []byte
	Complete        bool
	Truncated       bool
}

const (
	MissProofWhole     = "whole"
	MissProofPartial   = "partial"
	MissProofTruncated = "truncated"
)

func (p *MissProof) completeness() string {
	switch {
	case p == nil:
		return ""
	case p.Truncated:
		return MissProofTruncated
	case p.Complete:
		return MissProofWhole
	}
	return MissProofPartial
}

// missProver is the half of a classifier that carries proof.
type missProver interface {
	missProof() (MissProof, bool)
	releaseMissProof()
}

// errorStreamRetainer keeps one attempt's event lines while the stream is still speculative.
type errorStreamRetainer struct {
	maxBytes  int
	lines     []string
	bytesHeld int
	complete  bool
	truncated bool
	released  bool
}

func newErrorStreamRetainer(maxBytes int) *errorStreamRetainer {
	return &errorStreamRetainer{maxBytes: maxBytes}
}

// retain adds the data lines of one reassembled chunk, stopping at the bound rather than growing past it.
func (r *errorStreamRetainer) retain(events []byte) {
	if r == nil || r.released || r.truncated || len(events) == 0 {
		return
	}
	for _, line := range eventDataLines(events) {
		held := len(line) + 1
		if r.bytesHeld+held > r.maxBytes {
			r.truncated = true
			return
		}
		r.lines = append(r.lines, line)
		r.bytesHeld += held
		if strings.TrimSpace(strings.TrimPrefix(line, completionapi.DataPrefix)) == "[DONE]" {
			r.complete = true
		}
	}
}

// truncate marks the proof as having lost bytes, which no later line can undo.
func (r *errorStreamRetainer) truncate() {
	if r != nil {
		r.truncated = true
	}
}

// release drops what was retained; a later retain adds nothing, so the decision is one-way per attempt.
func (r *errorStreamRetainer) release() {
	if r == nil {
		return
	}
	r.lines, r.bytesHeld, r.released = nil, 0, true
}

// proof serializes the retained lines the way the executor serialized its own, and holds nothing back
// when the result is not a body a verifier would read as a terminal error.
func (r *errorStreamRetainer) proof() (MissProof, bool) {
	if r == nil || len(r.lines) == 0 {
		return MissProof{}, false
	}
	payload, err := json.Marshal(completionapi.SerializedStreamedResponse{Events: r.lines})
	if err != nil {
		return MissProof{}, false
	}
	if _, isError := completionapi.IsTerminalErrorResponse(payload); !isError {
		return MissProof{}, false
	}
	return MissProof{ResponsePayload: payload, Complete: r.complete, Truncated: r.truncated}, true
}

// eventDataLines returns the lines the executor hashed: its own data events, without the devshard
// envelope it wraps around them after the hash is taken.
func eventDataLines(events []byte) []string {
	var lines []string
	for _, raw := range bytes.Split(events, []byte("\n")) {
		line := strings.TrimRight(string(raw), "\r")
		if !strings.HasPrefix(line, completionapi.DataPrefix) || isDevshardEnvelopeLine(line) {
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

func isDevshardEnvelopeLine(line string) bool {
	data := strings.TrimPrefix(line, completionapi.DataPrefix)
	return strings.Contains(data, `"devshard_receipt"`) || strings.Contains(data, `"devshard_meta"`)
}
