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
			AdmissionQueueWaitMS:   300_000,
			AdmissionQueuePerSlot:  4,
			HostInflight: HostInflight{
				Initial: 64,
				Max:     256,
			},
			HostCutoff: HostCutoff{
				AfterFailures: 3,
				BaseMS:        5_000,
				MaxMS:         60_000,
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
			MatchWaitMS:    2_000,
			WarmNewEscrows: true,
		},
	}
}
