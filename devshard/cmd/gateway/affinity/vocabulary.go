package affinity

// Key names one conversation within one caller's namespace; it is a digest, so no client string is kept.
type Key string

// Status is what a lookup found.
type Status int

// StatusHit, StatusNew, StatusExpired, StatusPinAged and StatusShareCapped are what a lookup found; NoKey is what a request with nothing to key on gets, NoParticipant what a lookup answers when it holds no host.
const (
	StatusHit Status = iota
	StatusNew
	StatusExpired
	StatusPinAged
	StatusShareCapped
	NoKey         Key    = ""
	NoParticipant string = ""
)
