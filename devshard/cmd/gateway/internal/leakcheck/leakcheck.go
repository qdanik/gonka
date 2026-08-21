// Package leakcheck runs the goroutine-leak check with the goroutines that are not leaks ignored, in
// one place rather than in every package that checks.
package leakcheck

import (
	"testing"

	"go.uber.org/goleak"
)

// timerRoutine is started by a dependency of the generated chain types in its own init(), so it is
// running before any test begins and belongs to no test.
const timerRoutine = "github.com/desertbit/timer.timerRoutine"

// VerifyNone fails the test if any goroutine it started is still running.
func VerifyNone(t *testing.T) {
	goleak.VerifyNone(t, goleak.IgnoreTopFunction(timerRoutine))
}

// VerifyNoneStarted snapshots the goroutines already running and returns the check to defer, so the
// verdict covers only what this test started. Use as `defer leakcheck.VerifyNoneStarted(t)()`: the
// snapshot must be taken now, which a plain deferred call would postpone until the test is over.
func VerifyNoneStarted(t *testing.T) func() {
	existing := goleak.IgnoreCurrent()
	return func() { goleak.VerifyNone(t, goleak.IgnoreTopFunction(timerRoutine), existing) }
}

// VerifyTestMain fails the package's test run if any goroutine outlives it.
func VerifyTestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction(timerRoutine))
}
