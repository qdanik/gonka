package perf

// ring is a fixed-capacity circular buffer that overwrites the oldest element
// once full. Not internally locked — callers hold their own mutex.
type ring[T any] struct {
	buf   []T
	head  int
	count int
}

func newRing[T any](capacity int) *ring[T] {
	if capacity < 1 {
		capacity = 1
	}
	return &ring[T]{buf: make([]T, capacity)}
}

func (r *ring[T]) add(value T) {
	if r.count < len(r.buf) {
		r.buf[(r.head+r.count)%len(r.buf)] = value
		r.count++
		return
	}
	r.buf[r.head] = value
	r.head = (r.head + 1) % len(r.buf)
}

func (r *ring[T]) len() int {
	return r.count
}

// each iterates oldest to newest without allocating.
func (r *ring[T]) each(fn func(T)) {
	for i := range r.count {
		fn(r.buf[(r.head+i)%len(r.buf)])
	}
}
