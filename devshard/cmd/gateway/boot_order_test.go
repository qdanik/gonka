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

// Test flow:
//  1. Build the boot order from an empty gateway and config.
//  2. Collect the resulting step names.
//  3. Assert they match the full boot sequence, chain observer first and http listener last.
func TestBootStartsTheChainObserverFirstAndTheListenerLast(t *testing.T) {
	steps := (&gateway{}).bootOrder(context.Background(), context.Background(), &config.Config{}, &bootState{})

	want := []string{
		"chain observer", "warmup prober", "host pings", "seed devshards", "publish escrows",
		"nonce ledger", "escrow lifecycle", "devshard write republish", "http listener",
	}
	assertSame(t, "boot sequence", bootStepNames(steps), want)
}

// Test flow:
//  1. Build a boot step list where "seed devshards" fails with an error.
//  2. Run the steps through startAll.
//  3. Assert startAll returns an error naming "seed devshards".
//  4. Assert only the step before the failing one ran.
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
