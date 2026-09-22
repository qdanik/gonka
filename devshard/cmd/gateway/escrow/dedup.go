package escrow

import "sync"

// inFlightSet dedups concurrent operations by key. See README.md, "Keeping the request path off the chain".
type inFlightSet struct {
	mu   sync.Mutex
	keys map[string]bool
}

func (s *inFlightSet) enter(key string) (leave func(), busy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]bool)
	}
	if s.keys[key] {
		return nil, true
	}
	s.keys[key] = true
	return func() {
		s.mu.Lock()
		delete(s.keys, key)
		s.mu.Unlock()
	}, false
}

// markSet is the request-path-to-tick handoff. See README.md, "Keeping the request path off the chain".
type markSet struct {
	mu   sync.Mutex
	keys map[string]bool
}

// mark reports whether the key was new to this tick.
func (s *markSet) mark(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]bool)
	}
	if s.keys[key] {
		return false
	}
	s.keys[key] = true
	return true
}

func (s *markSet) drain() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	marks := s.keys
	s.keys = nil
	return marks
}

// depletionMarks is markSet with the reason kept, so the tick can tell a nonce cap from a balance floor.
type depletionMarks struct {
	mu      sync.Mutex
	reasons map[string]string
}

// mark reports whether the escrow was new to this tick; a nonce cap overwrites a balance reason, never the reverse.
func (s *depletionMarks) mark(escrowID, reason string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reasons == nil {
		s.reasons = make(map[string]string)
	}
	previous, seen := s.reasons[escrowID]
	if !seen || (reason == depletionReasonNonceCap && previous != depletionReasonNonceCap) {
		s.reasons[escrowID] = reason
	}
	return !seen
}

func (s *depletionMarks) forget(escrowID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reasons, escrowID)
}

func (s *depletionMarks) drain() map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	reasons := s.reasons
	s.reasons = nil
	return reasons
}
