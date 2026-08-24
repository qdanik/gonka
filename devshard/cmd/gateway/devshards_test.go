package main

import (
	"context"
	"testing"

	"devshard/cmd/gateway/store"
)

// The prefix an escrow was created under outranks the one this gateway is currently serving.
func TestEscrowRoutePrefixPrefersThePinOverTheRunningGateway(t *testing.T) {
	pinned := store.DevshardRecord{EscrowID: "58128", RoutePrefix: "/devshard/v3"}
	if got := escrowRoutePrefix(pinned, "/devshard/v4"); got != "/devshard/v3" {
		t.Fatalf("escrowRoutePrefix(pinned) = %q, want %q", got, "/devshard/v3")
	}

	unpinned := store.DevshardRecord{EscrowID: "58128"}
	if got := escrowRoutePrefix(unpinned, "/devshard/v4"); got != "/devshard/v4" {
		t.Fatalf("escrowRoutePrefix(unpinned) = %q, want the gateway's own %q", got, "/devshard/v4")
	}
}

type recordingRegistry struct {
	existing []store.DevshardRecord
	upserted []store.DevshardRecord
}

func (r *recordingRegistry) ListDevshards(context.Context) ([]store.DevshardRecord, error) {
	return r.existing, nil
}

func (r *recordingRegistry) UpsertDevshard(_ context.Context, record store.DevshardRecord) error {
	r.upserted = append(r.upserted, record)
	return nil
}

// A seed naming no key variable can never be signed for, and every other path that registers a
// devshard rejects that outright.
func TestSeedDevshardsRejectsASeedThatNamesNoKeyVariable(t *testing.T) {
	registry := &recordingRegistry{}
	if err := seedDevshards(context.Background(), registry, `[{"escrow_id":"58128","model":"Qwen/Test"}]`); err == nil {
		t.Fatal("seedDevshards accepted a seed with no private_key_env")
	}
	if len(registry.upserted) != 0 {
		t.Fatalf("seedDevshards stored %d records, want none", len(registry.upserted))
	}

	complete := `[{"escrow_id":"58128","model":"Qwen/Test","private_key_env":"GATEWAY_PRIVATE_KEY"}]`
	if err := seedDevshards(context.Background(), registry, complete); err != nil {
		t.Fatalf("seedDevshards(complete) = %v, want nil", err)
	}
	if len(registry.upserted) != 1 {
		t.Fatalf("seedDevshards stored %d records, want 1", len(registry.upserted))
	}
}
