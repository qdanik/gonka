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
