package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"devshard/cmd/gateway/config"
)

func bootStepNames(steps []bootStep) []string {
	names := make([]string, 0, len(steps))
	for _, step := range steps {
		names = append(names, step.name)
	}
	return names
}

// The boot order is a contract, the way the shutdown order is: the observer before anything that scores
// a host, the ledger before the emitters above it, the listener last. See operations.md, "Boot".
func TestBootStartsTheChainObserverFirstAndTheListenerLast(t *testing.T) {
	steps := (&gateway{}).bootOrder(context.Background(), context.Background(), &config.Config{}, &bootState{})

	want := []string{
		"chain observer", "warmup prober", "seed devshards", "publish escrows",
		"nonce ledger", "escrow lifecycle", "devshard write republish", "http listener",
	}
	assertSame(t, "boot sequence", bootStepNames(steps), want)
}

// A seed or a publish that fails must shut the gateway down rather than serve what it managed to build.
func TestBootStopsAtTheFirstStepThatFails(t *testing.T) {
	var sequence []string
	record := func(name string) bootStep {
		return bootStep{name: name, start: func() error { sequence = append(sequence, name); return nil }}
	}
	steps := []bootStep{
		record("chain observer"),
		{name: "seed devshards", start: func() error { return errors.New("escrow names no key variable") }},
		record("http listener"),
	}

	err := startAll(steps)

	if err == nil || !strings.Contains(err.Error(), "seed devshards") {
		t.Fatalf("startAll() = %v, want the failing step named", err)
	}
	assertSame(t, "boot sequence", sequence, []string{"chain observer"})
}
