package limits

// The strings this package puts on the wire. They are a contract with everything downstream, so they
// are declared here once and referenced by name. See README.md, "When a host stops taking work".

// Which trigger cut a host off.
const (
	cutoffReasonConsecutiveFaults = "consecutive_transport_faults"
	cutoffReasonProbeFailed       = "half_open_probe_failed"
)

// Which cap turned a request away, as the metric label and the log field name it.
const (
	RejectionConcurrentRequests = "concurrent_requests"
	RejectionInputTokens        = "input_tokens"
	RejectionQueueDepth         = "queue_depth"
	RejectionUnnamed            = "unnamed"
)
