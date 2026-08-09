package accounting

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
