package api

// The strings the HTTP boundary puts on the wire and into the record: response headers a client reads,
// the verdict a finished request is logged under, and the trigger a capture file is named for. Declared
// here so a rename reaches the log, the tests and the docs through the compiler.

// Headers a client can read off any chat reply.
const (
	EscrowHeader    = "X-Devshard-ID"
	RequestIDHeader = "X-Request-Id"
)

// How a finished request was delivered. A stream commits 200 on its first byte, so this is the only
// place that tells a reply the client can retry from one it cannot.
const (
	deliveryServed                = "served"
	deliveryFailedMidStream       = "failed_mid_stream"
	deliveryFailedBeforeFirstByte = "failed_before_first_byte"
)

// Why a request was captured. Capture has these two triggers and no others.
const (
	captureFilterRejected = "filter_rejected"
	captureAttemptsFailed = "all_attempts_failed"
)
