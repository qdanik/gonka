package perf

import (
	"testing"
	"time"
)

func TestSampleReceiptMsComputesGapInMilliseconds(t *testing.T) {
	s := Sample{SendTime: testEpoch, ReceiptTime: testEpoch.Add(150 * time.Millisecond)}
	if got := s.ReceiptMs(); got != 150 {
		t.Fatalf("ReceiptMs() = %v, want 150", got)
	}
}

func TestSampleReceiptMsZeroWhenSendTimeZero(t *testing.T) {
	s := Sample{ReceiptTime: testEpoch}
	if got := s.ReceiptMs(); got != 0 {
		t.Fatalf("ReceiptMs() with zero SendTime = %v, want 0", got)
	}
}

func TestSampleReceiptMsZeroWhenReceiptTimeZero(t *testing.T) {
	s := Sample{SendTime: testEpoch}
	if got := s.ReceiptMs(); got != 0 {
		t.Fatalf("ReceiptMs() with zero ReceiptTime = %v, want 0", got)
	}
}

func TestSampleReceiptMsZeroWhenGapNonPositive(t *testing.T) {
	s := Sample{SendTime: testEpoch, ReceiptTime: testEpoch.Add(-time.Millisecond)}
	if got := s.ReceiptMs(); got != 0 {
		t.Fatalf("ReceiptMs() with negative gap = %v, want 0", got)
	}
}

func TestSampleTotalMsComputesGapInMilliseconds(t *testing.T) {
	s := Sample{SendTime: testEpoch, Completed: testEpoch.Add(2500 * time.Millisecond)}
	if got := s.TotalMs(); got != 2500 {
		t.Fatalf("TotalMs() = %v, want 2500", got)
	}
}

func TestSampleTotalMsZeroWhenSendTimeZero(t *testing.T) {
	s := Sample{Completed: testEpoch}
	if got := s.TotalMs(); got != 0 {
		t.Fatalf("TotalMs() with zero SendTime = %v, want 0", got)
	}
}

func TestSampleTotalMsZeroWhenCompletedZero(t *testing.T) {
	s := Sample{SendTime: testEpoch}
	if got := s.TotalMs(); got != 0 {
		t.Fatalf("TotalMs() with zero Completed = %v, want 0", got)
	}
}

func TestSampleTotalMsZeroWhenGapNonPositive(t *testing.T) {
	s := Sample{SendTime: testEpoch, Completed: testEpoch.Add(-time.Millisecond)}
	if got := s.TotalMs(); got != 0 {
		t.Fatalf("TotalMs() with negative gap = %v, want 0", got)
	}
}

func TestSampleCTTFLComputesMsPerInputToken(t *testing.T) {
	s := Sample{
		ReceiptTime: testEpoch,
		FirstToken:  testEpoch.Add(100 * time.Millisecond),
		InputTokens: 50,
	}
	if got := s.CTTFL(); got != 2 {
		t.Fatalf("CTTFL() = %v, want 2 (100ms / 50 tokens)", got)
	}
}

func TestSampleCTTFLZeroWhenInputTokensZero(t *testing.T) {
	s := Sample{ReceiptTime: testEpoch, FirstToken: testEpoch.Add(100 * time.Millisecond)}
	if got := s.CTTFL(); got != 0 {
		t.Fatalf("CTTFL() with zero InputTokens = %v, want 0", got)
	}
}

func TestSampleCTTFLZeroWhenReceiptTimeZero(t *testing.T) {
	s := Sample{FirstToken: testEpoch, InputTokens: 10}
	if got := s.CTTFL(); got != 0 {
		t.Fatalf("CTTFL() with zero ReceiptTime = %v, want 0", got)
	}
}

func TestSampleCTTFLZeroWhenFirstTokenZero(t *testing.T) {
	s := Sample{ReceiptTime: testEpoch, InputTokens: 10}
	if got := s.CTTFL(); got != 0 {
		t.Fatalf("CTTFL() with zero FirstToken = %v, want 0", got)
	}
}

func TestSampleCTTFLZeroWhenGapNonPositive(t *testing.T) {
	s := Sample{ReceiptTime: testEpoch, FirstToken: testEpoch.Add(-time.Millisecond), InputTokens: 10}
	if got := s.CTTFL(); got != 0 {
		t.Fatalf("CTTFL() with non-positive gap = %v, want 0", got)
	}
}
