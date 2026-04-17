package observability

import "testing"

func TestNestedTracerNamesExposeExpectedValues(t *testing.T) {
	if tracerName.Public != "decentralized-api.public" {
		t.Fatalf("unexpected public tracer name: %q", tracerName.Public)
	}
	if tracerName.Chain != "decentralized-api.chain" {
		t.Fatalf("unexpected chain tracer name: %q", tracerName.Chain)
	}
}

func TestNestedSpanNamesExposeExpectedValues(t *testing.T) {
	if spanName.Inference.CompareLogits != "inference.validation.compare_logits" {
		t.Fatalf("unexpected compare logits span name: %q", spanName.Inference.CompareLogits)
	}
	if spanName.Chain.GRPCQuery != "chain.grpc.query" {
		t.Fatalf("unexpected grpc query span name: %q", spanName.Chain.GRPCQuery)
	}
}