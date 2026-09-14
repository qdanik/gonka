package warmup

import (
	"fmt"
	"testing"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/internal/logcapture"
)

func TestAProbeTheLedgerRefusedIsLogged(t *testing.T) {
	logged := logcapture.Install(t)
	refusal := fmt.Errorf("%w: %s", accounting.ErrUnknownEscrow, "escrow-9")
	warmup := &Prober{ledger: &spyLedger{reject: refusal}, now: warmupClock()}

	warmup.record("escrow-9", 7, true, nil)

	logged.RequireLine(t, logcapture.Entry{Level: "warn", Msg: "escrow warmup could not settle its nonce", Fields: []any{
		"escrow", "escrow-9", "nonce", uint64(7), "error", fmt.Errorf("%w: %s", accounting.ErrUnknownEscrow, "escrow-9"),
	}})
}
