package share

// Kind is one counted event of a host; the first six also decay, the rest are epoch totals only.
type Kind int

const (
	Assigned Kind = iota
	Offered
	Served
	Failed
	Passed
	Excused
	ProbesAnswered
	ProbesMissed
	VotesOwed
	PassUnvoted

	decayedKinds = int(Excused) + 1
	kinds        = int(PassUnvoted) + 1
)
