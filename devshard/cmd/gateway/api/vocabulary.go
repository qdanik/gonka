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

// The cache's own refusals, beside the ones filters can name from the body alone.
const (
	cacheRefusedTooLarge    = "too_large"
	cacheRefusedWriteFailed = "write_failed"
	cacheRefusedRaceFailed  = "race_failed"
	cacheRefusedNoEscrow    = "no_escrow"
	cacheRefusedInvalidated = "invalidated"
)

const (
	cacheResultStored   = "stored"
	cacheResultBypassed = "bypassed"
)

// The headers of the client's cache contract and the grammar of Cache-Control. See README.md, "What the client controls".
const (
	CacheControlHeader      = "Cache-Control"
	CacheStatusHeader       = "X-Devshard-Cache"
	AgeHeader               = "Age"
	directiveListSeparator  = ","
	directiveValueSeparator = "="
)

type cacheDirective string

// The Cache-Control request directives the gateway acts on; every other directive is ignored.
const (
	cacheDirectiveNone    cacheDirective = ""
	cacheDirectiveNoCache cacheDirective = "no-cache"
	cacheDirectiveNoStore cacheDirective = "no-store"
)

// CacheInvalidationReason names the signal that removed a cached reply.
type CacheInvalidationReason string

// The reasons a cached reply is removed, as devshard_gateway_cache_invalidations_total labels them.
const (
	CacheReasonInvalidVerdict    CacheInvalidationReason = "invalid_verdict"
	CacheReasonRecordInvalidated CacheInvalidationReason = "record_invalidated"
	CacheReasonServedMismatch    CacheInvalidationReason = "served_mismatch"
	cacheReasonHostPinned        CacheInvalidationReason = "host_pinned"
	cacheReasonEpoch             CacheInvalidationReason = "epoch"
)

type cacheEffect string

// What a removal did: dropped a stored reply, recorded the verdict for a reply not yet stored, or found the verdict already recorded.
const (
	cacheEffectDropped cacheEffect = "dropped"
	cacheEffectRevoked cacheEffect = "revoked"
	cacheEffectAbsent  cacheEffect = "absent"
)

// How a cache lookup ended; a hit is also the value of the reply header that says so.
const (
	cacheHit  = "hit"
	cacheMiss = "miss"
)

// A reply with no winning nonce, which the cache stores but cannot index.
const servedNonceNone uint64 = 0

// Why a request was captured. Capture has these two triggers and no others.
const (
	captureFilterRejected = "filter_rejected"
	captureAttemptsFailed = "all_attempts_failed"
)

// How a chain inference record reads in an admin answer, one name per protocol status.
const (
	statusNamePending     = "pending"
	statusNameStarted     = "started"
	statusNameFinished    = "finished"
	statusNameChallenged  = "challenged"
	statusNameValidated   = "validated"
	statusNameInvalidated = "invalidated"
	statusNameTimedOut    = "timed_out"
	statusNameUnknown     = "unknown"
)

// What phase an escrow session is in, as an admin answer names it.
const (
	phaseNameActive     = "active"
	phaseNameFinalizing = "finalizing"
	phaseNameSettlement = "settlement"
	phaseNameUnknown    = "unknown"
)

type (
	filterSource string
	limitsSource string
)

// Where a model filter entry's profile comes from, and where its read-only limits come from.
const (
	filterSourceStored     filterSource = "stored"
	filterSourceBuiltIn    filterSource = "built_in"
	filterSourceDefault    filterSource = "default"
	filterSourceUnreadable filterSource = "unreadable"
	limitsSourceModel      limitsSource = "model_limits"
	limitsSourceGlobal     limitsSource = "global"
)

// The model-filter routes' error codes and the query parameter that skips the chain check.
const (
	errorCodeModelNotOnChain        = "model_not_on_chain"
	errorCodeChainModelsUnavailable = "chain_models_unavailable"
	allowOffChainParameter          = "allow_off_chain"
	allowOffChainAbsent             = ""
)

// The OpenAI error type and code a prompt past the model's context is answered with.
const (
	errorTypeInvalidRequest        = "invalid_request_error"
	errorCodeContextLengthExceeded = "context_length_exceeded"
)

// A stored filter row the gateway read without a problem.
const problemNone = ""

// What a request record names as its caller when no client API key matched.
const (
	callerAdmin     = "admin"
	callerAnonymous = "none"
)

// What a client reads when a host's error ends its request. See README.md, "Errors and statuses".
const (
	hostRejectedRequestMessage = "an upstream host rejected the request"
	hostFailedMessage          = "an upstream host failed to answer the request"
	hostsOverloadedMessage     = "upstream hosts are overloaded; retry the request"
	hostExcerptSeparator       = ": "
)
