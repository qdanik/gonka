package liquidity

// Class names what one part of an escrow's money is waiting for; the metrics label carries it. See README.md, "Classes".
type Class string

const (
	ClassFree      Class = "free"
	ClassReturning Class = "returning"
	ClassLate      Class = "late"
	ClassStuck     Class = "stuck"
)
