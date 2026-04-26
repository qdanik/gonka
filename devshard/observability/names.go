package observability

type tracerID string
type spanID string

type tracerNames struct {
	Transport tracerID
}

type requestSpanNames struct {
	Request spanID
}

type spanNames struct {
	Request requestSpanNames
}

var tracerName = tracerNames{
	Transport: "devshard.transport",
}

var spanName = spanNames{
	Request: requestSpanNames{
		Request: "devshard.request",
	},
}