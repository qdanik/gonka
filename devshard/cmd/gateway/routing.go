package main

import (
	"errors"
	"time"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/journal"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/nonces"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/warmup"
)

type routingDeps struct {
	Sessions     registry.SessionFactory
	ReadOnly     registry.SessionFactory
	Capacity     *limits.Capacity
	Participants *limits.ParticipantLimiter
	Hosts        *perf.Tracker
	Snapshots    *chain.PhaseObserver
	Config       *config.Holder
	Depletion    *depletionNotice
	Dispatches   *metrics.DispatchRecorder
	Ledger       *nonces.Recorder
	Journal      *journal.Journal
	Now          func() time.Time
}

// newRouting joins the escrow set to the picker through the capacity model; an unjoined escrow serves nothing.
func newRouting(deps routingDeps) (*registry.Registry, *scheduler.Scheduler, *warmup.Prober, error) {
	if deps.Journal == nil {
		return nil, nil, nil, errors.New("routing: Journal is required")
	}
	// The warmup needs the registry it observes, so it is handed the registry once that exists.
	registryDeps := registry.Deps{
		ServingSessions:  deps.Sessions,
		ReadOnlySessions: deps.ReadOnly,
		Membership:       deps.Capacity,
		Exhaustion:       deps.Depletion,
		Narrator:         deps.Journal,
		Retiring:         deps.Ledger.EscrowRetiring,
		Now:              deps.Now,
	}
	// A nil warmup must not reach the interface field: a typed nil there is non-nil to a nil check.
	prober := warmup.New(deps.Config, deps.Ledger.Book(), deps.Snapshots, deps.Now)
	if prober != nil {
		registryDeps.Publications = prober
	}
	escrows := registry.New(registryDeps)
	prober.Serve(escrows)
	prober.SetNarrator(deps.Journal)
	router, err := scheduler.NewScheduler(scheduler.Deps{
		Escrows:           escrows,
		Capacity:          deps.Capacity,
		Limiter:           deps.Participants,
		Perf:              deps.Hosts,
		Snapshots:         deps.Snapshots,
		Config:            deps.Config,
		Observer:          tracedDispatches{recorder: deps.Dispatches, events: deps.Journal, ledger: deps.Ledger},
		Now:               deps.Now,
		OnEscrowExhausted: escrows.Exhausted,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return escrows, router, prober, nil
}
