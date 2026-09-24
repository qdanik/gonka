package hostping

import (
	"context"
	"testing"

	"common/probe"

	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/registry"
)

type countingSink struct{ targets chan int }

func (s countingSink) Observe(probe.Result) {}
func (s countingSink) Forget(string)        {}
func (s countingSink) TickStarted()         {}
func (s countingSink) TickSkipped()         {}
func (s countingSink) TargetCount(count int) {
	select {
	case s.targets <- count:
	default:
	}
}

func runnableSettings() config.HostPing {
	return config.HostPing{IntervalMS: 40, TimeoutMS: 10, Concurrency: 2}
}

// A ping nobody asked for costs a goroutine and a connection per host per tick, so off means absent.
func TestAPingerIsAbsentWhenTheProbeIsOff(t *testing.T) {
	settings := runnableSettings()
	settings.Disabled = true

	if pinger := New(settings, dialsHeld{{BaseURL: "http://a:8080"}}, countingSink{}); pinger != nil {
		t.Fatal("New() returned a pinger for a probe that is switched off")
	}
}

// The probe primitive refuses a schedule whose wave could outlast its own period; that refusal must
// cost the gateway the ping, not its boot.
func TestAScheduleThatDoesNotHoldCostsThePingNotTheBoot(t *testing.T) {
	settings := config.HostPing{IntervalMS: 10, TimeoutMS: 10, Concurrency: 1}

	if pinger := New(settings, dialsHeld{{BaseURL: "http://a:8080"}}, countingSink{}); pinger != nil {
		t.Fatal("New() accepted a wave that can outlast its own period")
	}
}

// The wave reads the live escrows each time rather than a list handed to it once.
func TestEachWaveReadsWhoIsLiveNow(t *testing.T) {
	sink := countingSink{targets: make(chan int, 1)}
	pinger := New(runnableSettings(), dialsHeld{
		{ParticipantKey: "gonka1abc", BaseURL: "http://127.0.0.1:1/"},
	}, sink)
	if pinger == nil {
		t.Fatal("New() returned nothing for a runnable schedule")
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	pinger.Start(ctx)

	if got := <-sink.targets; got != 1 {
		t.Fatalf("targets = %d, want the one live host", got)
	}
}

var _ liveDials = dialsHeld(nil)
var _ = registry.HostDial{}
