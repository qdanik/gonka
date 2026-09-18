package nonces

import (
	"testing"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/registry"
	"devshard/state"
	"devshard/types"
	"devshard/user"
)

type retiringSession struct {
	registry.EscrowSession
	machine *state.StateMachine
}

func (s retiringSession) SnapshotState() types.EscrowState { return s.machine.SnapshotState() }
func (s retiringSession) UserSession() *user.Session       { return nil }

func newOpenedLedger(t *testing.T, escrow *liveEscrow) *Recorder {
	t.Helper()
	service, err := accounting.NewService(accounting.Settings{Now: func() time.Time { return time.Unix(0, 0).UTC() }})
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	if err := service.Book.OpenEscrow(accounting.EscrowMetadata{
		EscrowID: tokenEscrowID, Model: "llama", CreationEpoch: 7, Slots: escrow.machine.SnapshotState().Group,
	}); err != nil {
		t.Fatalf("OpenEscrow(): %v", err)
	}
	return &Recorder{service: service}
}

// The sweep reads an escrow only while it is routable, and deactivating one stops that for good. Whatever
// the chain finished since the last sweep is counted here or nowhere.
func TestRetiringAnEscrowCountsWhatTheChainFinishedSinceTheLastSweep(t *testing.T) {
	t.Parallel()
	escrow := newLiveEscrow(t, 3)
	ledger := newOpenedLedger(t, escrow)
	inference := escrow.start(t, 60, 64)
	escrow.finish(t, inference, 60, 64, 57, 31)

	ledger.EscrowRetiring(tokenEscrowID, retiringSession{machine: escrow.machine})

	totals := foldRecords(ledger.service.Book.Query(accounting.QueryFilter{}))
	if totals.InputTokens != 57 {
		t.Fatalf("input_tokens = %d after the escrow retired, want the 57 the finish carried", totals.InputTokens)
	}
	if totals.CountedNonces != 1 {
		t.Fatalf("counted_nonces = %d, want the one inference the chain finished", totals.CountedNonces)
	}
}

func TestARetiredEscrowIsMarkedRetiredInTheLedger(t *testing.T) {
	t.Parallel()
	escrow := newLiveEscrow(t, 3)
	ledger := newOpenedLedger(t, escrow)

	ledger.EscrowRetiring(tokenEscrowID, retiringSession{machine: escrow.machine})

	for _, record := range ledger.service.Book.Query(accounting.QueryFilter{}) {
		for _, latest := range record.LatestNonces {
			if !latest.Retired {
				t.Fatalf("escrow %s is still serving in the ledger after it retired", latest.EscrowID)
			}
		}
	}
}

// A retirement the registry cannot hand a session for -- one already closed, or never published --
// still has to leave the ledger consistent.
func TestRetiringWithoutASessionStillRetiresTheEscrow(t *testing.T) {
	t.Parallel()
	escrow := newLiveEscrow(t, 3)
	ledger := newOpenedLedger(t, escrow)

	ledger.EscrowRetiring(tokenEscrowID, nil)

	records := ledger.service.Book.Query(accounting.QueryFilter{})
	if len(records) == 0 {
		t.Fatal("the ledger forgot the escrow it retired")
	}
	for _, record := range records {
		for _, latest := range record.LatestNonces {
			if !latest.Retired {
				t.Fatalf("escrow %s is still serving in the ledger after it retired", latest.EscrowID)
			}
		}
	}
}

// The composition root binds the retirement observer whether or not accounting is switched on, so the
// bound method has to survive a recorder that was never opened.
func TestARecorderThatWasNeverOpenedStillAnswersARetirement(t *testing.T) {
	t.Parallel()
	var ledger *Recorder

	retiring := ledger.EscrowRetiring

	retiring(tokenEscrowID, nil)
}
