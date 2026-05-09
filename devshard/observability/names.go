package observability

type tracerID string
type spanID string

type tracerNames struct {
	Transport tracerID
	Inference tracerID
}

type requestSpanNames struct {
	Request spanID
}

type inferenceSpanNames struct {
	Execution spanID
}

type spanNames struct {
	Request   requestSpanNames
	Inference inferenceSpanNames
}

var tracerName = tracerNames{
	Transport: "devshard.transport",
	Inference: "devshard.inference",
}

var spanName = spanNames{
	Request: requestSpanNames{
		Request: "devshard.request",
	},
	Inference: inferenceSpanNames{
		Execution: "devshard.inference.execution",
	},
}
