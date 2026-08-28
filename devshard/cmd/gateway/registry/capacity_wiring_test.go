// This is the one black-box test in the package: it reaches for limits and scheduler, which the
// registry itself must never import, to pin the wiring an in-package test cannot see. Missing that
// wiring makes the gateway boot green and serve nothing. See gateway-routing-and-nonces.md,
// "Membership: what the capacity model is told".
package registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"common/completionapi"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/internal/testutil"
	"devshard/signing"
	"devshard/state"
	"devshard/user"
)

const (
	wiredEscrowID = "11"
	wiredModel    = "qwen"
)

type openLimiter struct{}

func (openLimiter) Available(string, string) bool { return true }
func (openLimiter) Acquire(string, string) bool   { return true }
func (openLimiter) Release(string, string)        {}

type capablePerf struct{}

func (capablePerf) CannotServe(string, string, bool, uint64) (string, bool) { return "", false }
func (capablePerf) Ejected(string, string) bool                             { return false }

type fixedSnapshots struct{ snapshot chain.PhaseSnapshot }

func (f fixedSnapshots) Snapshot() chain.PhaseSnapshot { return f.snapshot }

// liveSessionFactory builds a real *user.Session over in-memory storage with nil host clients, so the
// participant keys the registry derives membership from are real group addresses.
func liveSessionFactory(t *testing.T) (registry.SessionFactory, []string) {
	t.Helper()
	signers := []*signing.Secp256k1Signer{testutil.MustGenerateKey(t), testutil.MustGenerateKey(t)}
	group := testutil.MakeGroup(signers)
	sessionConfig := testutil.DefaultConfig(len(group))
	creator := testutil.MustGenerateKey(t)
	verifier := signing.NewSecp256k1Verifier()
	store := testutil.MustMemoryStore(t, wiredEscrowID, creator.Address(), sessionConfig, group, 1_000_000)

	machine, err := state.NewStateMachine(wiredEscrowID, sessionConfig, group, 1_000_000, creator.Address(), verifier, store)
	if err != nil {
		t.Fatalf("NewStateMachine = %v, want nil", err)
	}
	session, err := user.NewSession(machine, creator, wiredEscrowID, group, make([]user.HostClient, len(group)), verifier, user.WithStorage(store))
	if err != nil {
		t.Fatalf("NewSession = %v, want nil", err)
	}

	participants := []string{signers[0].Address(), signers[1].Address()}
	return func(context.Context, string) (registry.EscrowSession, error) {
		return registry.NewSessionHandle(session, machine), nil
	}, participants
}

func TestPushedMembershipIsWhatMakesTheGatewayRouteAtAll(t *testing.T) {
	t.Parallel()

	sessions, participants := liveSessionFactory(t)
	weights := map[string]float64{participants[0]: 40, participants[1]: 60}
	capacity := limits.NewCapacity(nil)
	capacity.Update(chain.PhaseSnapshot{CurrentWeights: weights, FullWeights: weights})

	escrows := registry.New(registry.Deps{ServingSessions: sessions, Membership: capacity, Now: time.Now})
	t.Cleanup(func() { _ = escrows.Close() })
	if err := escrows.Add(context.Background(), wiredEscrowID, wiredModel); err != nil {
		t.Fatalf("Add = %v, want nil", err)
	}

	// Each participant holds one of the two slots and serves no other escrow, so its share is 1 and the
	// escrow's weight is the sum of the two hosts' current weights.
	if got, want := capacity.EscrowWeight(wiredEscrowID, wiredModel), 100.0; got != want {
		t.Fatalf("EscrowWeight = %v, want %v", got, want)
	}

	settings := config.Defaults()
	router := scheduler.NewScheduler(scheduler.Deps{
		Escrows:   escrows,
		Capacity:  capacity,
		Limiter:   openLimiter{},
		Perf:      capablePerf{},
		Snapshots: fixedSnapshots{},
		Config:    config.NewHolder(&settings),
	})
	t.Cleanup(router.Stop)

	assignment, err := router.Pick(context.Background(), scheduler.RequestProfile{
		Model:  wiredModel,
		Params: user.InferenceParams{Model: wiredModel, Prompt: []byte(`{"messages":[]}`), InputLength: 15, MaxTokens: completionapi.MinTokensFloor},
	})
	if err != nil {
		t.Fatalf("Pick = %v, want an assignment; a zero escrow weight scores every candidate +Inf", err)
	}
	if assignment.Escrow != wiredEscrowID {
		t.Errorf("assigned escrow = %q, want %q", assignment.Escrow, wiredEscrowID)
	}
	if assignment.Nonce == nil || assignment.Nonce.Nonce() == 0 {
		t.Errorf("assignment carries no committed nonce: %+v", assignment.Nonce)
	}
}

func TestWithoutTheMembershipPushEveryEscrowScoresUnusable(t *testing.T) {
	t.Parallel()

	sessions, participants := liveSessionFactory(t)
	weights := map[string]float64{participants[0]: 40, participants[1]: 60}
	capacity := limits.NewCapacity(nil)
	capacity.Update(chain.PhaseSnapshot{CurrentWeights: weights, FullWeights: weights})

	// Membership left nil is exactly what the gateway did before this package existed.
	escrows := registry.New(registry.Deps{ServingSessions: sessions, Now: time.Now})
	t.Cleanup(func() { _ = escrows.Close() })
	if err := escrows.Add(context.Background(), wiredEscrowID, wiredModel); err != nil {
		t.Fatalf("Add = %v, want nil", err)
	}

	if got := capacity.EscrowWeight(wiredEscrowID, wiredModel); got != 0 {
		t.Fatalf("EscrowWeight without a membership push = %v, want 0", got)
	}
	if got := len(escrows.Candidates(wiredModel)); got != 1 {
		t.Fatalf("len(Candidates) = %d, want 1: the escrow is published and looks healthy", got)
	}

	settings := config.Defaults()
	router := scheduler.NewScheduler(scheduler.Deps{
		Escrows:   escrows,
		Capacity:  capacity,
		Limiter:   openLimiter{},
		Perf:      capablePerf{},
		Snapshots: fixedSnapshots{},
		Config:    config.NewHolder(&settings),
	})
	t.Cleanup(router.Stop)

	_, err := router.Pick(context.Background(), scheduler.RequestProfile{Model: wiredModel})

	if !errors.Is(err, scheduler.ErrNoEscrowCapacity) {
		t.Fatalf("Pick = %v, want ErrNoEscrowCapacity: the gateway boots green and serves nothing", err)
	}
}

func TestMembershipSharesFeedTheWeightSplitOfASharedParticipant(t *testing.T) {
	t.Parallel()
	shared := testutil.MustGenerateKey(t)
	exclusive := testutil.MustGenerateKey(t)
	capacity := limits.NewCapacity(nil)
	weights := map[string]float64{shared.Address(): 90, exclusive.Address(): 10}
	capacity.Update(chain.PhaseSnapshot{CurrentWeights: weights, FullWeights: weights})

	// The shared participant holds one slot in each of two escrows; limits must see half of it per
	// escrow, not the whole of it twice.
	capacity.SetEscrowMembership("1", map[string]float64{shared.Address(): 0.5, exclusive.Address(): 1})
	capacity.SetEscrowMembership("2", map[string]float64{shared.Address(): 0.5})

	if got, want := capacity.EscrowWeight("1", wiredModel), 55.0; got != want {
		t.Errorf("EscrowWeight(1) = %v, want %v", got, want)
	}
	if got, want := capacity.EscrowWeight("2", wiredModel), 45.0; got != want {
		t.Errorf("EscrowWeight(2) = %v, want %v", got, want)
	}
	// Raw slot counts would hand the shared host's full weight to both escrows.
	capacity.SetEscrowMembership("1", map[string]float64{shared.Address(): 1, exclusive.Address(): 1})
	if got, want := capacity.EscrowWeight("1", wiredModel), 100.0; got != want {
		t.Errorf("EscrowWeight(1) with raw slot counts = %v, want %v -- the double count", got, want)
	}
}
