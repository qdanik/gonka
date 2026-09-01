package config

import (
	"strings"
	"testing"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/env"
)

func TestEngineTimingsReachTheSnapshotFromAnOverride(t *testing.T) {
	t.Parallel()
	receipt, floor, ceiling, stall, grace := int64(7_000), int64(1_500), int64(25_000), int64(45_000), int64(120_000)

	built, err := Build(env.Values{}, Overrides{
		EngineReceiptTimeoutMS:    &receipt,
		EngineFirstTokenFloorMS:   &floor,
		EngineFirstTokenCeilingMS: &ceiling,
		EngineInterChunkStallMS:   &stall,
		EngineLoserGraceMS:        &grace,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, field := range []struct {
		name string
		got  int64
		want int64
	}{
		{"receipt_timeout_ms", built.Engine.ReceiptTimeoutMS, receipt},
		{"first_token_floor_ms", built.Engine.FirstTokenFloorMS, floor},
		{"first_token_ceiling_ms", built.Engine.FirstTokenCeilingMS, ceiling},
		{"inter_chunk_stall_ms", built.Engine.InterChunkStallMS, stall},
		{"loser_grace_ms", built.Engine.LoserGraceMS, grace},
	} {
		if field.got != field.want {
			t.Errorf("engine_%s = %d, want %d", field.name, field.got, field.want)
		}
	}
}

func TestEngineTimingsReachTheSnapshotFromTheEnvironment(t *testing.T) {
	t.Parallel()
	built, err := Build(env.Values{
		EngineReceiptTimeoutMS:     int64Pointer(9_000),
		EngineFirstTokenFloorMS:    int64Pointer(1_200),
		EngineFirstTokenCeilingMS:  int64Pointer(21_000),
		EngineInterChunkStallMS:    int64Pointer(20_000),
		EngineLoserGraceMS:         int64Pointer(90_000),
		ChainSnapshotMaxAgeSeconds: int64Pointer(45),
	}, Overrides{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, field := range []struct {
		name string
		got  int64
		want int64
	}{
		{"engine_receipt_timeout_ms", built.Engine.ReceiptTimeoutMS, 9_000},
		{"engine_first_token_floor_ms", built.Engine.FirstTokenFloorMS, 1_200},
		{"engine_first_token_ceiling_ms", built.Engine.FirstTokenCeilingMS, 21_000},
		{"engine_inter_chunk_stall_ms", built.Engine.InterChunkStallMS, 20_000},
		{"engine_loser_grace_ms", built.Engine.LoserGraceMS, 90_000},
		{"chain_snapshot_max_age_seconds", built.Chain.SnapshotMaxAgeSeconds, 45},
	} {
		if field.got != field.want {
			t.Errorf("%s = %d, want %d", field.name, field.got, field.want)
		}
	}
}

func TestAnEngineOverrideOutranksTheEnvironment(t *testing.T) {
	t.Parallel()
	fromEnv, fromAdmin := int64(9_000), int64(3_000)
	values := env.Values{
		EngineReceiptTimeoutMS:     &fromEnv,
		EngineFirstTokenFloorMS:    &fromEnv,
		EngineFirstTokenCeilingMS:  &fromEnv,
		EngineInterChunkStallMS:    &fromEnv,
		EngineLoserGraceMS:         &fromEnv,
		ChainSnapshotMaxAgeSeconds: int64Pointer(90),
	}
	overrides := Overrides{
		EngineReceiptTimeoutMS:     &fromAdmin,
		EngineFirstTokenFloorMS:    &fromAdmin,
		EngineFirstTokenCeilingMS:  int64Pointer(12_000),
		EngineInterChunkStallMS:    &fromAdmin,
		EngineLoserGraceMS:         int64Pointer(30_000),
		ChainSnapshotMaxAgeSeconds: int64Pointer(45),
	}

	built, err := Build(values, overrides)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	for _, field := range []struct {
		name string
		got  int64
		want int64
	}{
		{"engine_receipt_timeout_ms", built.Engine.ReceiptTimeoutMS, fromAdmin},
		{"engine_first_token_floor_ms", built.Engine.FirstTokenFloorMS, fromAdmin},
		{"engine_first_token_ceiling_ms", built.Engine.FirstTokenCeilingMS, 12_000},
		{"engine_inter_chunk_stall_ms", built.Engine.InterChunkStallMS, fromAdmin},
		{"engine_loser_grace_ms", built.Engine.LoserGraceMS, 30_000},
		{"chain_snapshot_max_age_seconds", built.Chain.SnapshotMaxAgeSeconds, 45},
	} {
		if field.got != field.want {
			t.Errorf("%s = %d, want the admin value %d", field.name, field.got, field.want)
		}
	}
}

func TestTheBoundsThemselvesAreAccepted(t *testing.T) {
	t.Parallel()
	built, err := Build(env.Values{}, Overrides{
		EngineReceiptTimeoutMS:     int64Pointer(maxEngineTimingMS),
		EngineInterChunkStallMS:    int64Pointer(maxEngineTimingMS),
		EngineLoserGraceMS:         int64Pointer(maxEngineTimingMS),
		ChainSnapshotMaxAgeSeconds: int64Pointer(int64(chain.DefaultObserverPollInterval.Seconds()) * snapshotAgePollMultiple),
	})
	if err != nil {
		t.Fatalf("Build refused the documented limits: %v", err)
	}
	if built.Chain.SnapshotMaxAgeSeconds != 35 {
		t.Errorf("chain_snapshot_max_age_seconds = %d, want the floor itself accepted", built.Chain.SnapshotMaxAgeSeconds)
	}
}

func TestEngineOverridesAreValidatedLikeTheDefaults(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name      string
		overrides Overrides
		wantError string
	}{
		{
			name:      "a grace shorter than the stall it must outlive",
			overrides: Overrides{EngineLoserGraceMS: int64Pointer(1_000), EngineInterChunkStallMS: int64Pointer(30_000)},
			wantError: "engine_loser_grace_ms: 1000 must be >= engine_inter_chunk_stall_ms 30000",
		},
		{
			name:      "a ceiling under its own floor",
			overrides: Overrides{EngineFirstTokenFloorMS: int64Pointer(10_000), EngineFirstTokenCeilingMS: int64Pointer(2_000)},
			wantError: "engine_first_token_ceiling_ms: 2000 must be >= engine_first_token_floor_ms 10000",
		},
		{
			name:      "a receipt deadline of zero",
			overrides: Overrides{EngineReceiptTimeoutMS: int64Pointer(0)},
			wantError: "engine_receipt_timeout_ms: 0 must be >= 1",
		},
		{
			name:      "a receipt deadline wide enough to overflow its own duration",
			overrides: Overrides{EngineReceiptTimeoutMS: int64Pointer(maxEngineTimingMS + 1)},
			wantError: "engine_receipt_timeout_ms: 86400001 must be <= 86400000",
		},
		{
			name:      "a ceiling wide enough to overflow its own duration",
			overrides: Overrides{EngineFirstTokenCeilingMS: int64Pointer(maxEngineTimingMS + 1)},
			wantError: "engine_first_token_ceiling_ms: 86400001 must be <= 86400000",
		},
		{
			name:      "a floor wide enough to overflow its own duration",
			overrides: Overrides{EngineFirstTokenFloorMS: int64Pointer(maxEngineTimingMS + 1)},
			wantError: "engine_first_token_floor_ms: 86400001 must be <= 86400000",
		},
		{
			name:      "a stall wide enough to overflow its own duration",
			overrides: Overrides{EngineInterChunkStallMS: int64Pointer(maxEngineTimingMS + 1)},
			wantError: "engine_inter_chunk_stall_ms: 86400001 must be <= 86400000",
		},
		{
			name:      "a grace wide enough to overflow its own duration",
			overrides: Overrides{EngineLoserGraceMS: int64Pointer(maxEngineTimingMS + 1)},
			wantError: "engine_loser_grace_ms: 86400001 must be <= 86400000",
		},
		{
			name:      "a snapshot age wide enough to overflow its own duration",
			overrides: Overrides{ChainSnapshotMaxAgeSeconds: int64Pointer(maxSnapshotAgeSeconds + 1)},
			wantError: "chain_snapshot_max_age_seconds: 86401 must be between 0 (disabled) and 86400",
		},
		{
			name:      "a snapshot age under the observer's own refresh cadence",
			overrides: Overrides{ChainSnapshotMaxAgeSeconds: int64Pointer(2)},
			wantError: "chain_snapshot_max_age_seconds: 2 must be 0 or at least 35",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := Build(env.Values{}, testCase.overrides)
			if err == nil {
				t.Fatalf("Build accepted %s", testCase.name)
			}
			if !strings.Contains(err.Error(), testCase.wantError) {
				t.Errorf("error = %v, want it to name %s", err, testCase.wantError)
			}
		})
	}
}
