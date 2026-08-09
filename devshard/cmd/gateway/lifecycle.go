package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"devshard/cmd/gateway/internal/logkey"
	"devshard/logging"
)

func (g *gateway) serve(ctx context.Context) error {
	backgroundCtx, stopBackground := context.WithCancel(ctx)
	defer stopBackground()

	g.observer.Start(backgroundCtx)
	configuration := g.config.Load()
	if err := seedDevshards(ctx, g.store, configuration.Server.DevshardsJSON); err != nil {
		return errors.Join(err, g.shutdown(shutdownGracePeriod))
	}
	if err := g.publishEscrows(ctx); err != nil {
		return errors.Join(err, g.shutdown(shutdownGracePeriod))
	}
	g.manager.Start(backgroundCtx)
	republished := g.republishOnDevshardWrites(backgroundCtx)

	serveResult := make(chan error, 1)
	go func() { serveResult <- g.server.ListenAndServe() }()
	logging.Info("gateway started",
		logkey.Version, Version, logkey.Port, configuration.Server.Port,
		logkey.StorageDir, configuration.Server.StorageDir, logkey.EscrowBuilders, g.builders)

	var listenErr error
	select {
	case listenErr = <-serveResult:
	case <-ctx.Done():
	}
	stopBackground()
	shutdownErr := g.shutdown(shutdownGracePeriod)
	<-republished
	if listenErr == nil {
		listenErr = <-serveResult
	}
	if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
		return errors.Join(fmt.Errorf("http server: %w", listenErr), shutdownErr)
	}
	logging.Info("gateway stopped")
	return shutdownErr
}

// needsQuiesced marks a step that destroys state the steps above it may still be using. See
// gateway-invariants.md, "6. Shutdown order is a contract".
type shutdownStep struct {
	name          string
	stop          func(context.Context) error
	needsQuiesced bool
}
type httpListener interface {
	Shutdown(ctx context.Context) error
}
type stopper interface{ Stop() }

// idleConnections is satisfied by *http.Client: the pooled chain client keeps sockets open between
// polls, and nothing above it in the order closes them.
type idleConnections interface{ CloseIdleConnections() }

// shutdownOrder is the eight-step contract every shutdown follows. See gateway-architecture.md,
// "Shutdown" and gateway-invariants.md, "6. Shutdown order is a contract".
func shutdownOrder(listener httpListener, races, dispatchers, escrowLifecycle, chainObserver stopper, sessions, storage io.Closer, publicAPI idleConnections) []shutdownStep {
	return []shutdownStep{
		{name: "http server", stop: listener.Shutdown},
		{name: "races", stop: waitFor(races)},
		{name: "dispatchers", stop: waitFor(dispatchers)},
		{name: "escrow lifecycle", stop: waitFor(escrowLifecycle)},
		{name: "chain observer", stop: waitFor(chainObserver)},
		{name: "escrow sessions", stop: closeOf(sessions), needsQuiesced: true},
		{name: "store", stop: closeOf(storage)},
		// Last: every step above can still reach the public API, and an idle socket closed under one of
		// them is a socket the next poll has to re-dial. The chain's own gRPC connection is not closed
		// here and cannot be: common/chain owns it and exposes no Close, so it lives until the process
		// exits. That is why the tests ignore its goroutines rather than waiting for them.
		{name: "public api connections", stop: closeIdle(publicAPI)},
	}
}

// waitFor bounds a drain by the shutdown budget without cancelling the work inside it. See
// gateway-architecture.md, "Shutdown".
func waitFor(component stopper) func(context.Context) error {
	return func(ctx context.Context) error {
		stopped := make(chan struct{})
		go func() { defer close(stopped); component.Stop() }()
		select {
		case <-stopped:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("abandoned with work still running: %w", ctx.Err())
		}
	}
}
func closeOf(component io.Closer) func(context.Context) error {
	return func(context.Context) error { return component.Close() }
}
func closeIdle(component idleConnections) func(context.Context) error {
	return func(context.Context) error {
		component.CloseIdleConnections()
		return nil
	}
}

// stopAll runs every step even after a failure, except one marked needsQuiesced, which is skipped and
// the skip reported. See gateway-architecture.md, "Shutdown".
func stopAll(ctx context.Context, steps []shutdownStep) error {
	var problems []error
	quiesced := true
	for _, step := range steps {
		if step.needsQuiesced && !quiesced {
			problems = append(problems, fmt.Errorf("skipping %s: work above it is still running", step.name))
			continue
		}
		if err := step.stop(ctx); err != nil {
			problems = append(problems, fmt.Errorf("stopping %s: %w", step.name, err))
			quiesced = false
		}
	}
	return errors.Join(problems...)
}
func (g *gateway) shutdown(grace time.Duration) error {
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), grace)
	defer cancelDrain()
	return stopAll(drainCtx, shutdownOrder(g.server, g.races, g.router, g.manager, g.observer, g.escrows, g.store, g.publicAPI))
}

// bootBudget ties the two halves of a bounded boot: the concurrent-build limit and the idle pool those
// builds reuse. See gateway-architecture.md, "Boot".
type bootBudget struct {
	builders int
	client   *http.Client
}

func newBootBudget(builders int) bootBudget {
	if builders < 1 {
		builders = 1
	}
	pooled := http.DefaultTransport.(*http.Transport).Clone()
	pooled.MaxIdleConns = builders
	pooled.MaxIdleConnsPerHost = builders
	return bootBudget{
		builders: builders,
		client:   &http.Client{Timeout: chainRequestTimeout, Transport: pooled},
	}
}
