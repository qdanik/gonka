package accounting

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestTwoEpochsExportAsDistinctSeries guards the pairing of the aggregation key with the label set. A
// participant holds the same slot through a rotation, so two epochs carry the same participant and
// model; without the epoch label the two records collide and the whole scrape is rejected.
func TestTwoEpochsExportAsDistinctSeries(t *testing.T) {
	book := newTestBook(t, 2)
	openTestEscrow(t, book, secondTestEscrow, testEpoch+1, 2)
	if err := book.RecordGhost(testEscrow, 0, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(%s): %v", testEscrow, err)
	}
	if err := book.RecordGhost(secondTestEscrow, 0, "poc_unavailable_host"); err != nil {
		t.Fatalf("RecordGhost(%s): %v", secondTestEscrow, err)
	}

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(book)); err != nil {
		t.Fatalf("Register(): %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}

	epochs := make(map[string]int)
	for _, family := range families {
		if family.GetName() != "devshard_gateway_nonces_assigned" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "epoch" {
					epochs[label.GetValue()]++
				}
			}
		}
	}
	if len(epochs) != 2 {
		t.Fatalf("assigned series carried epochs %v, want one series per epoch", epochs)
	}
}

// Findings are the ledger's verdict on a host, and until they are exported an alert has nothing to
// fire on: the JSON API is not served unless an operator sets a listen address.
func TestFindingsAreExportedAsTheirOwnSeries(t *testing.T) {
	const groupSize = 2
	book := newTestBook(t, groupSize)
	// Slot 0's assigned count is the highest nonce over the group size, so it must clear the
	// twenty-nonce floor a finding needs before it is raised at all.
	for nonce := uint64(0); nonce <= 40; nonce += groupSize {
		if err := book.RecordGhost(testEscrow, nonce, "participant_state_diverged_no_send"); err != nil {
			t.Fatalf("RecordGhost(%d): %v", nonce, err)
		}
	}

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(book)); err != nil {
		t.Fatalf("Register(): %v", err)
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather(): %v", err)
	}

	labels := map[string]string{}
	value := 0.0
	found := false
	for _, family := range families {
		if family.GetName() != "devshard_gateway_nonce_finding" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["code"] == FindingStateDiverged {
				value, found = metric.GetGauge().GetValue(), true
			}
		}
	}
	if !found {
		t.Fatalf("no %q series for the burns that raised it", FindingStateDiverged)
	}
	if labels["severity"] != string(SeverityWarning) {
		t.Errorf("severity = %q, want %q", labels["severity"], SeverityWarning)
	}
	if value <= 0 {
		t.Errorf("value = %v, want the rate that raised the finding", value)
	}
}

// The collector rebuilds from the ledger, so a pruned retired epoch leaves the next scrape.
func TestPruningARetiredEpochRemovesItsSeries(t *testing.T) {
	const currentEpoch = 10
	service, err := NewService(Settings{
		RetentionEpochs: 2,
		CurrentEpoch:    func(context.Context) (uint64, error) { return currentEpoch, nil },
		Now:             func() time.Time { return time.Unix(0, 0).UTC() },
	})
	if err != nil {
		t.Fatalf("NewService(): %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	openTestEscrow(t, service.Book, "escrow-retired", currentEpoch-3, 1)
	service.Book.RetireEscrow("escrow-retired")
	openTestEscrow(t, service.Book, "escrow-current", currentEpoch, 1)

	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(NewCollector(service.Book)); err != nil {
		t.Fatalf("Register(): %v", err)
	}
	if before, err := testutil.GatherAndCount(registry, "devshard_gateway_nonces_assigned"); err != nil || before != 2 {
		t.Fatalf("assigned series before pruning = %d (%v), want one per epoch", before, err)
	}

	service.prune(t.Context())

	if after, err := testutil.GatherAndCount(registry, "devshard_gateway_nonces_assigned"); err != nil || after != 1 {
		t.Fatalf("assigned series after pruning = %d (%v), want only the current epoch's", after, err)
	}
}
