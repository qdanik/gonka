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
