package shadow

import "time"

// How a probe ended; the wire strings of the metric and the journal line.
const (
	OutcomeServed = "served"
	OutcomeFailed = "failed"
)

// The probe's size is fixed at ProbeInputTokens of prompt and at most ProbeMaxTokens of answer; the rest is how it is built, bounded and read. See README.md.
const (
	ProbeInputTokens  = 64
	ProbeMaxTokens    = 64
	probeTimeout      = 2 * time.Minute
	fitAttempts       = 3
	fallbackBytes     = 64
	probeWord         = "hello "
	maxCaptureBytes   = 64 << 10
	probeBodyFormat   = `{"messages":[{"role":"user","content":%q}],"max_tokens":%d,"stream":true,"stream_options":{"include_usage":true}}`
	countedBodyFormat = `{"messages":[{"role":"user","content":%q}]}`
	dataPrefix        = "data:"
	doneMarker        = "[DONE]"
)
