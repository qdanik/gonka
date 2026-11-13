package affinity

// Key names one conversation within one caller's namespace; it is a digest, so no client string is kept.
type Key string

// Status is what a lookup found.
type Status int

// StatusHit, StatusNew, StatusExpired, StatusPinAged, StatusShareCapped and StatusBelowMinCached are what a lookup found; NoKey is what a request with nothing to key on gets, NoParticipant what a lookup answers when it holds no host.
const (
	StatusHit Status = iota
	StatusNew
	StatusExpired
	StatusPinAged
	StatusShareCapped
	StatusBelowMinCached
	NoKey         Key    = ""
	NoParticipant string = ""
)

// Exception is why a request was dispatched without the host guarantee; ExceptionNone is a request that kept it or never had a key. See routing.md, "Host affinity".
type Exception string

const (
	ExceptionNone           Exception = ""
	ExceptionNewKey         Exception = "new_key"
	ExceptionExpired        Exception = "expired"
	ExceptionGateBlocked    Exception = "gate_blocked"
	ExceptionNotInGroup     Exception = "not_in_group"
	ExceptionPinAged        Exception = "pin_aged"
	ExceptionShareCapped    Exception = "share_capped"
	ExceptionMaxBurns       Exception = "max_burns"
	ExceptionBelowMinCached Exception = "below_min_cached"
)
