package store

import (
	"context"
	"testing"
	"time"
)

func TestRotationStatusUpsertByModelRoleReplaces(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	first := RotationStatus{
		Model:       "model-a",
		Role:        "temp",
		Stage:       "prepare_temp",
		Epoch:       3,
		Completed:   false,
		CreateError: "model not served by network",
		UpdatedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := testStore.SaveRotationStatus(ctx, first); err != nil {
		t.Fatalf("SaveRotationStatus() first: %v", err)
	}

	second := first
	second.Stage = "finish_regular"
	second.Epoch = 4
	second.Completed = true
	second.CreateError = ""
	second.UpdatedAt = time.Date(2026, 1, 1, 0, 5, 0, 0, time.UTC)
	if err := testStore.SaveRotationStatus(ctx, second); err != nil {
		t.Fatalf("SaveRotationStatus() second: %v", err)
	}

	statuses, err := testStore.LoadRotationStatuses(ctx)
	if err != nil {
		t.Fatalf("LoadRotationStatuses(): %v", err)
	}
	if len(statuses) != 1 {
		t.Fatalf("LoadRotationStatuses() = %+v, want exactly 1 row (upsert must replace)", statuses)
	}
	got := statuses[0]
	if got.Stage != second.Stage || got.Epoch != second.Epoch || got.Completed != second.Completed || got.CreateError != second.CreateError {
		t.Fatalf("LoadRotationStatuses()[0] = %+v, want replaced fields from %+v", got, second)
	}
	if !got.UpdatedAt.Equal(second.UpdatedAt) {
		t.Fatalf("UpdatedAt = %v, want %v", got.UpdatedAt, second.UpdatedAt)
	}
}

func TestRotationStatusDistinguishesRoleWithinSameModel(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	temp := RotationStatus{Model: "model-a", Role: "temp", Stage: "prepare_temp", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	regular := RotationStatus{Model: "model-a", Role: "regular", Stage: "finish_regular", UpdatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := testStore.SaveRotationStatus(ctx, temp); err != nil {
		t.Fatalf("SaveRotationStatus(temp): %v", err)
	}
	if err := testStore.SaveRotationStatus(ctx, regular); err != nil {
		t.Fatalf("SaveRotationStatus(regular): %v", err)
	}

	statuses, err := testStore.LoadRotationStatuses(ctx)
	if err != nil {
		t.Fatalf("LoadRotationStatuses(): %v", err)
	}
	if len(statuses) != 2 {
		t.Fatalf("LoadRotationStatuses() = %+v, want 2 distinct rows (same model, different role)", statuses)
	}
}

func TestLoadRotationStatusesDeterministicOrder(t *testing.T) {
	testStore := openTestStore(t)
	ctx := context.Background()

	updatedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []RotationStatus{
		{Model: "model-b", Role: "regular", Stage: "s", UpdatedAt: updatedAt},
		{Model: "model-a", Role: "temp", Stage: "s", UpdatedAt: updatedAt},
		{Model: "model-a", Role: "regular", Stage: "s", UpdatedAt: updatedAt},
	}
	for _, row := range rows {
		if err := testStore.SaveRotationStatus(ctx, row); err != nil {
			t.Fatalf("SaveRotationStatus(%s/%s): %v", row.Model, row.Role, err)
		}
	}

	loaded, err := testStore.LoadRotationStatuses(ctx)
	if err != nil {
		t.Fatalf("LoadRotationStatuses(): %v", err)
	}
	wantOrder := []string{"model-a/regular", "model-a/temp", "model-b/regular"}
	if len(loaded) != len(wantOrder) {
		t.Fatalf("LoadRotationStatuses() = %d rows, want %d", len(loaded), len(wantOrder))
	}
	for i, want := range wantOrder {
		if got := loaded[i].Model + "/" + loaded[i].Role; got != want {
			t.Fatalf("LoadRotationStatuses()[%d] = %q, want %q (full order %+v)", i, got, want, loaded)
		}
	}
}
