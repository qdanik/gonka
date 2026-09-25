package main

import (
	"context"
	"testing"

	"devshard/cmd/gateway/store"
)

// Test flow:
//  1. Call escrowRoutePrefix with a pinned record naming its own route prefix.
//  2. Assert the pinned prefix wins over the gateway's own running prefix.
//  3. Call escrowRoutePrefix with an unpinned record.
//  4. Assert it falls back to the gateway's own running prefix.
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

// Test flow:
//  1. Call seedDevshards with a seed JSON that names no private_key_env.
//  2. Assert it returns an error and stores nothing in the registry.
//  3. Call seedDevshards again with the same seed plus private_key_env set.
//  4. Assert it succeeds and stores exactly one record.
func TestSeedDevshardsRejectsASeedThatNamesNoKeyVariable(t *testing.T) {
	registry := &recordingRegistry{}
	if err := seedDevshards(context.Background(), registry, `[{"id":"58128","model":"Qwen/Test"}]`); err == nil {
		t.Fatal("seedDevshards accepted a seed with no private_key_env")
	}
	if len(registry.upserted) != 0 {
		t.Fatalf("seedDevshards stored %d records, want none", len(registry.upserted))
	}

	complete := `[{"id":"58128","model":"Qwen/Test","private_key_env":"DEVSHARD_PRIVATE_KEY"}]`
	if err := seedDevshards(context.Background(), registry, complete); err != nil {
		t.Fatalf("seedDevshards(complete) = %v, want nil", err)
	}
	if len(registry.upserted) != 1 {
		t.Fatalf("seedDevshards stored %d records, want 1", len(registry.upserted))
	}
}

// Test flow:
//  1. Call seedDevshards with a devshardctl-shaped seed that names a per-escrow route prefix.
//  2. Assert the stored record carries that prefix, so the escrow's hosts are dialled under it as devshardctl did.
func TestSeedDevshardsKeepsTheSeedsRoutePrefix(t *testing.T) {
	registry := &recordingRegistry{}
	seed := `[{"id":"58128","model":"Qwen/Test","private_key_env":"DEVSHARD_PRIVATE_KEY","route_prefix":"/v4"}]`

	if err := seedDevshards(context.Background(), registry, seed); err != nil {
		t.Fatalf("seedDevshards() = %v, want nil", err)
	}

	if len(registry.upserted) != 1 || registry.upserted[0].RoutePrefix != "/v4" {
		t.Fatalf("stored %+v, want one record with route prefix /v4", registry.upserted)
	}
}
