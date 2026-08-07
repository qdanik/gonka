package escrow

import "sync"

// inFlightSet dedups concurrent operations by key: the first caller enters and gets a leave func to
// call when done; a caller whose key is already in flight gets busy=true and should no-op.
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

// markSet is the request-path-to-tick handoff: a hook marks a key without doing I/O and the next
// tick takes the whole set at once. drain steals the map rather than copying it, so a key marked
// while the tick is running belongs to the following tick and cannot be dropped.
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
