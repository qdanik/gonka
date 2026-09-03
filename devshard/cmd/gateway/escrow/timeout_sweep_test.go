package escrow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"devshard/cmd/gateway/config"
)

type recordingSweeper struct {
	mu      sync.Mutex
	calls   int
	grace   time.Duration
	budget  int
	blockOn chan struct{}
}

func (s *recordingSweeper) SweepExecutionTimeouts(_ context.Context, grace time.Duration, budgetPerEscrow int) (int, int, int) {
	s.mu.Lock()
	s.calls++
	s.grace, s.budget = grace, budgetPerEscrow
	block := s.blockOn
	s.mu.Unlock()
	if block != nil {
		<-block
	}
	return 1, 1, 0
}

func (s *recordingSweeper) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func managerWithSweeper(t *testing.T, sweeper TimeoutSweeper, cfg *config.Config) *Manager {
	t.Helper()
	deps := testManagerDeps(t, newFakeStore(), &fakeTxClient{}, &fakeSnapshotSource{}, cfg)
	deps.Timeouts = sweeper
	return mustManager(t, deps)
}

func TestSweepPassesTheConfiguredBudgetAndGrace(t *testing.T) {
	cfg := config.Defaults()
	cfg.TimeoutSweep.BudgetPerTick = 3
	cfg.TimeoutSweep.GraceSeconds = 90
	sweeper := &recordingSweeper{}
	manager := managerWithSweeper(t, sweeper, &cfg)

	manager.sweepTimeouts(context.Background())
	manager.sweepWork.Wait()

	require.Equal(t, 1, sweeper.callCount())
	require.Equal(t, 90*time.Second, sweeper.grace)
	require.Equal(t, 3, sweeper.budget)
}

// A zero budget is the off switch an operator reaches for without a rebuild.
func TestSweepIsOffWithoutABudget(t *testing.T) {
	cfg := config.Defaults()
	cfg.TimeoutSweep.BudgetPerTick = 0
	sweeper := &recordingSweeper{}
	manager := managerWithSweeper(t, sweeper, &cfg)

	manager.sweepTimeouts(context.Background())
	manager.sweepWork.Wait()

	require.Zero(t, sweeper.callCount())
}

// One vote round can outlast the tick, and a second sweep over the same escrows would double the
// load the budget exists to bound.
func TestASecondTickDoesNotStartASecondSweep(t *testing.T) {
	cfg := config.Defaults()
	cfg.TimeoutSweep.BudgetPerTick = 2
	release := make(chan struct{})
	sweeper := &recordingSweeper{blockOn: release}
	manager := managerWithSweeper(t, sweeper, &cfg)

	manager.sweepTimeouts(context.Background())
	require.Eventually(t, func() bool { return sweeper.callCount() == 1 }, time.Second, time.Millisecond)
	manager.sweepTimeouts(context.Background())
	close(release)
	manager.sweepWork.Wait()

	require.Equal(t, 1, sweeper.callCount())
}

// Stop is a barrier for the sweep too: a vote round must not outlive the manager that started it.
func TestStopWaitsForASweepInFlight(t *testing.T) {
	cfg := config.Defaults()
	cfg.TimeoutSweep.BudgetPerTick = 2
	release := make(chan struct{})
	sweeper := &recordingSweeper{blockOn: release}
	manager := managerWithSweeper(t, sweeper, &cfg)
	manager.sweepTimeouts(context.Background())
	require.Eventually(t, func() bool { return sweeper.callCount() == 1 }, time.Second, time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		manager.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("Stop() returned while a sweep was still voting")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-stopped
}
