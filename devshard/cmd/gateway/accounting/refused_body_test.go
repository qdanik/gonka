package accounting

import (
	"testing"

	"devshard/cmd/gateway/engine"
)

// The ledger's rates answer "is this host failing". A body the gateway refused to send never reached the host,
// so counting it there reports the gateway's own decision as the host's fault — which docs/accounting.md
// forbids by construction for ghosts, and for the same reason.
func TestABodyTheGatewayRefusedToSendLeavesTheHostsRatesAlone(t *testing.T) {
	t.Parallel()

	refused := CounterKey{Terminal: engine.ReasonRequestTooLarge}

	if !excused(refused) {
		t.Fatal("a body the gateway refused to send counted against the host's failure rate")
	}
	if !offRecord(refused) {
		t.Fatal("a body the gateway refused to send was kept on the record")
	}
}
