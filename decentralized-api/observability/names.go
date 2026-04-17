package observability

type tracerID string
type spanID string

type tracerNames struct {
	Public        tracerID
	EventListener tracerID
	Validation    tracerID
	Chain         tracerID
}

type inferenceSpanNames struct {
	Request                spanID
	Transfer               spanID
	ForwardExecutor        spanID
	Execute                spanID
	FinishSubmit           spanID
	ValidationEvent        spanID
	StatusUpdateEvent      spanID
	ValidationSample       spanID
	ValidationExecute      spanID
	PayloadRetrieve        spanID
	PayloadRetrieveAttempt spanID
	PayloadFetch           spanID
	CompareLogits          spanID
}

type mlNodeSpanNames struct {
	ChatCompletions           spanID
	ChatCompletionsValidation spanID
}

type chainSpanNames struct {
	TxBroadcast   spanID
	TxConfirmation spanID
	StoreQuery    spanID
	GRPCQuery     spanID
}

type spanNames struct {
	Inference inferenceSpanNames
	MLNode    mlNodeSpanNames
	Chain     chainSpanNames
}

var tracerName = tracerNames{
	Public:        "decentralized-api.public",
	EventListener: "decentralized-api.event-listener",
	Validation:    "decentralized-api.validation",
	Chain:         "decentralized-api.chain",
}

var spanName = spanNames{
	Inference: inferenceSpanNames{
		Request:                "inference.request",
		Transfer:               "inference.transfer",
		ForwardExecutor:        "inference.transfer.forward_executor",
		Execute:                "inference.executor.execute",
		FinishSubmit:           "inference.finish.submit",
		ValidationEvent:        "inference.validation.event",
		StatusUpdateEvent:      "inference.status_update.event",
		ValidationSample:       "inference.validation.sample",
		ValidationExecute:      "inference.validation.execute",
		PayloadRetrieve:        "inference.payload.retrieve",
		PayloadRetrieveAttempt: "inference.payload.retrieve.attempt",
		PayloadFetch:           "inference.payload.fetch",
		CompareLogits:          "inference.validation.compare_logits",
	},
	MLNode: mlNodeSpanNames{
		ChatCompletions:           "mlnode.chat.completions",
		ChatCompletionsValidation: "mlnode.chat.completions.validation",
	},
	Chain: chainSpanNames{
		TxBroadcast:    "chain.tx.broadcast",
		TxConfirmation: "chain.tx.confirmation",
		StoreQuery:     "chain.store.query",
		GRPCQuery:      "chain.grpc.query",
	},
}