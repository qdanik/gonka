package perf

import "time"

// Sample is one host's outcome for a single inference request.
type Sample struct {
	ParticipantKey string
	Model          string
	Responsive     bool
	SendTime       time.Time
	ReceiptTime    time.Time
	FirstToken     time.Time
	InputTokens    uint64
}

// ReceiptMs is (ReceiptTime-SendTime) in ms, or 0 if either is zero or the gap is <=0.
func (s Sample) ReceiptMs() float64 {
	if s.SendTime.IsZero() || s.ReceiptTime.IsZero() {
		return 0
	}
	ms := float64(s.ReceiptTime.Sub(s.SendTime).Milliseconds())
	if ms <= 0 {
		return 0
	}
	return ms
}

// CTTFL is (FirstToken-ReceiptTime)ms per input token, or 0 if either timestamp/token count is zero or the gap is <=0.
func (s Sample) CTTFL() float64 {
	if s.ReceiptTime.IsZero() || s.FirstToken.IsZero() || s.InputTokens == 0 {
		return 0
	}
	gapMs := float64(s.FirstToken.Sub(s.ReceiptTime).Milliseconds())
	if gapMs <= 0 {
		return 0
	}
	return gapMs / float64(s.InputTokens)
}
