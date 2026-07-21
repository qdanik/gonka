package perf

import "testing"

// ringContents drains a ring via each() into a slice for assertions.
func ringContents[T any](r *ring[T]) []T {
	out := make([]T, 0, r.len())
	r.each(func(v T) { out = append(out, v) })
	return out
}

func assertRingOrder(t *testing.T, r *ring[int], want []int) {
	t.Helper()
	got := ringContents(r)
	if len(got) != len(want) {
		t.Fatalf("each() yielded %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("each() yielded %v, want %v", got, want)
		}
	}
}

func TestRingAddOrder(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		values   []int
		want     []int
		wantLen  int
	}{
		{
			name:     "below capacity keeps insertion order",
			capacity: 5,
			values:   []int{1, 2, 3},
			want:     []int{1, 2, 3},
			wantLen:  3,
		},
		{
			name:     "exactly at capacity keeps insertion order",
			capacity: 3,
			values:   []int{1, 2, 3},
			want:     []int{1, 2, 3},
			wantLen:  3,
		},
		{
			name:     "past capacity keeps newest N in chronological order",
			capacity: 3,
			values:   []int{1, 2, 3, 4, 5},
			want:     []int{3, 4, 5},
			wantLen:  3,
		},
		{
			name:     "many wraps still yield chronological order",
			capacity: 4,
			values:   []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			want:     []int{8, 9, 10, 11},
			wantLen:  4,
		},
		{
			name:     "capacity one keeps only the latest value",
			capacity: 1,
			values:   []int{1, 2, 3},
			want:     []int{3},
			wantLen:  1,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			r := newRing[int](testCase.capacity)
			for _, v := range testCase.values {
				r.add(v)
			}
			if r.len() != testCase.wantLen {
				t.Fatalf("len() = %d, want %d", r.len(), testCase.wantLen)
			}
			assertRingOrder(t, r, testCase.want)
		})
	}
}

func TestRingLenSaturatesAtCapacity(t *testing.T) {
	r := newRing[int](3)
	for i, wantLen := range []int{1, 2, 3, 3, 3, 3} {
		r.add(i)
		if r.len() != wantLen {
			t.Fatalf("after %d adds, len() = %d, want %d", i+1, r.len(), wantLen)
		}
	}
}

func TestRingEachOnEmptyRingIsNoOp(t *testing.T) {
	r := newRing[int](4)
	called := false
	r.each(func(int) { called = true })
	if called {
		t.Fatal("each() invoked fn on an empty ring")
	}
	if r.len() != 0 {
		t.Fatalf("len() = %d, want 0", r.len())
	}
}

func TestRingCapacityFloor(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
	}{
		{name: "zero capacity floors to one", capacity: 0},
		{name: "negative capacity floors to one", capacity: -5},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			r := newRing[int](testCase.capacity)
			r.add(1)
			r.add(2)
			if r.len() != 1 {
				t.Fatalf("len() = %d, want 1 (floored capacity)", r.len())
			}
			assertRingOrder(t, r, []int{2})
		})
	}
}

// TestRingEachDoesNotAllocate pins the zero-allocation contract: stat
// computation must scan the ring without a per-read slice allocation.
func TestRingEachDoesNotAllocate(t *testing.T) {
	r := newRing[int](256)
	for i := range 300 {
		r.add(i)
	}
	var sum int
	allocs := testing.AllocsPerRun(1000, func() {
		sum = 0
		r.each(func(v int) { sum += v })
	})
	if allocs != 0 {
		t.Fatalf("each() allocated %.0f times per run, want 0", allocs)
	}
	_ = sum
}

// TestRingEachStructValueDoesNotAllocate covers the non-primitive T case
// (the real callers iterate structs, not ints).
func TestRingEachStructValueDoesNotAllocate(t *testing.T) {
	type sample struct {
		key   string
		value float64
	}
	r := newRing[sample](256)
	for i := range 300 {
		r.add(sample{key: "host", value: float64(i)})
	}
	var total float64
	allocs := testing.AllocsPerRun(1000, func() {
		total = 0
		r.each(func(s sample) { total += s.value })
	})
	if allocs != 0 {
		t.Fatalf("each() allocated %.0f times per run, want 0", allocs)
	}
	_ = total
}
