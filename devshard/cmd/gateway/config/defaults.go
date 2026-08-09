package config

import (
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/filters"
)

func Defaults() Config {
	return Config{
		Server: Server{
			Port:                       8080,
			MaxConcurrentRuntimeBuilds: 16,
		},
		Chain: Chain{
			GRPCEndpoint:     "localhost:9090",
			PublicAPIBaseURL: "http://localhost:9000",
		},
		Tx: Tx{
			FeeDenom:       chain.DefaultFeeDenom,
			FeeAmount:      int64(chain.DefaultFeeAmount),
			GasLimit:       int64(chain.DefaultGasLimit),
			PollIntervalMS: chain.DefaultPollInterval.Milliseconds(),
			PollTimeoutMS:  chain.DefaultPollTimeout.Milliseconds(),
		},
		Limits: Limits{
			DefaultMaxTokens: int64(filters.DefaultRequestMaxTokens),
			MaxTokensCap:     int64(filters.RequestMaxTokensCap),
			Concurrency: Concurrency{
				MaxRequests:               1536,
				RequestsPer10000Weight:    24,
				PoCRequestsPer10000Weight: 48,
			},
			MaxInputTokensInFlight: 0,
			// The wait budget a request spends looking for capacity. A shard that refuses on the first
			// hiccup is what 429 meant to a client that had exceeded nothing.
			AcquireWaitMS: 300_000,
			// Wait budget divided by the measured time one request holds a slot (~105 s at p50): deeper
			// than this and the queue provably cannot reach the newcomer before its budget runs out.
			QueueDepthPerSlot: 4,
			// A window opens near what a host is known to take rather than discovering it: starting at 4
			// capped three participants at twelve concurrent requests, whatever the group size.
			AIMD: AIMD{
				InitialWindow: 64,
				MaxWindow:     256,
			},
			Breaker: Breaker{
				TripThreshold: 3,
				BaseOpenMS:    5_000,
				MaxOpenMS:     60_000,
			},
		},
		Modes: Modes{
			PoCMode: PoCModeOff,
		},
		Rotation: Rotation{
			PrePoCBlocks: 300,
		},
		Cache: Cache{
			ChatCacheMaxBytes: 256 << 20,
		},
		Accounting: Accounting{
			RetentionHours:   168,
			RetentionMaxRows: 1_000_000,
		},
		NonceAccounting: NonceAccounting{
			SnapshotSeconds: 300,
		},
		Capture: Capture{
			SampleRate: 0.01,
			MaxBytes:   1 << 30,
		},
		Stream: Stream{
			DrainTimeoutSeconds:         2_400,
			ClassifyMaxAttemptBytes:     1 << 20,
			ClassifyMaxParticipantBytes: 10 << 20,
			ClassifyMaxGlobalBytes:      100 << 20,
		},
		Perf: Perf{
			EWMAHalfLifeSeconds:      600,
			ConsecutiveFailThreshold: 5,
			FailureRateThreshold:     0.15,
			FailureRateMinVolume:     20,
			EjectionBaseSeconds:      30,
			EjectionMaxSeconds:       600,
			MaxEjectionFraction:      0.5,
			MinAvailableHosts:        1,
			HostStalenessSeconds:     3_600,
		},
		Engine: Engine{
			ReceiptTimeoutMS:       5_000,
			FirstTokenFloorMS:      1_000,
			FirstTokenCeilingMS:    30_000,
			InterChunkStallMS:      30_000,
			LoserGraceMS:           600_000,
			MaxSpeculativeAttempts: 0,
		},
		Scheduler: Scheduler{
			HoldGraceMS: 2_000,
		},
	}
}
