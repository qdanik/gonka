package safemath

import (
	"math"
	"testing"
)

func TestAddSaturatingStopsAtTheBound(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		first, second int64
		want          int64
	}{
		{"ordinary", 2, 3, 5},
		{"at the ceiling", math.MaxInt64, 1, math.MaxInt64},
		{"past the ceiling", math.MaxInt64 - 1, 5, math.MaxInt64},
		{"both at the ceiling", math.MaxInt64, math.MaxInt64, math.MaxInt64},
		{"at the floor", math.MinInt64, -1, math.MinInt64},
		{"both at the floor", math.MinInt64, math.MinInt64, math.MinInt64},
		{"the two bounds meet", math.MaxInt64, math.MinInt64, -1},
		{"negative back inside", math.MaxInt64, -1, math.MaxInt64 - 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := AddSaturating(testCase.first, testCase.second); got != testCase.want {
				t.Fatalf("AddSaturating(%d, %d) = %d, want %d: a counter that wraps hands every comparison below it a number it believes",
					testCase.first, testCase.second, got, testCase.want)
			}
		})
	}
}

func TestMulSaturatingStopsAtTheBound(t *testing.T) {
	t.Parallel()

	for _, testCase := range []struct {
		name          string
		first, second int64
		want          int64
	}{
		{"ordinary", 3, 4, 12},
		{"by zero", math.MaxInt64, 0, 0},
		{"zero by", 0, math.MinInt64, 0},
		{"past the ceiling", math.MaxInt64, 2, math.MaxInt64},
		{"past the floor", math.MinInt64, 2, math.MinInt64},
		{"two negatives overflow upward", math.MinInt64, -2, math.MaxInt64},
		{"the floor negated", math.MinInt64, -1, math.MaxInt64},
		{"the floor negated the other way", -1, math.MinInt64, math.MaxInt64},
		{"the ceiling negated", math.MaxInt64, -1, -math.MaxInt64},
		{"identity at the floor", math.MinInt64, 1, math.MinInt64},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			if got := MulSaturating(testCase.first, testCase.second); got != testCase.want {
				t.Fatalf("MulSaturating(%d, %d) = %d, want %d: a window sized by a wrapped product never admits anything again",
					testCase.first, testCase.second, got, testCase.want)
			}
		})
	}
}

// The helper exists so a quantity the gateway does not choose cannot wrap a counter, so the property has to
// hold for every pair, not only the ones a table names.
func TestSaturatingArithmeticNeverWraps(t *testing.T) {
	t.Parallel()

	interesting := []int64{math.MinInt64, math.MinInt64 + 1, -(1 << 40), -2, -1, 0, 1, 2, 1 << 40, math.MaxInt64 - 1, math.MaxInt64}
	for _, first := range interesting {
		for _, second := range interesting {
			if sum := AddSaturating(first, second); (first > 0 && second > 0 && sum < first) || (first < 0 && second < 0 && sum > first) {
				t.Errorf("AddSaturating(%d, %d) = %d wrapped past its own operand", first, second, sum)
			}
			product := MulSaturating(first, second)
			if first != 0 && second != 0 && (first > 0) == (second > 0) && product < 0 {
				t.Errorf("MulSaturating(%d, %d) = %d, want a positive product for two operands of one sign", first, second, product)
			}
			if first != 0 && second != 0 && (first > 0) != (second > 0) && product > 0 {
				t.Errorf("MulSaturating(%d, %d) = %d, want a negative product for operands of opposite signs", first, second, product)
			}
		}
	}
}
