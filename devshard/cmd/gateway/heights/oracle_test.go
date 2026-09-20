package heights

import (
	"testing"

	"devshard/cmd/gateway/config"
)

// The follower is the gateway's own reading of mainnet, kept for the trust label on a carried tip.
// It is not what the gateway stamps from, so it stays off unless asked for.
func TestNoFollowerUnlessItIsAskedFor(t *testing.T) {
	oracle, err := NewOracle(config.HeightSync{Enabled: true}, OracleSources{CometRPC: "http://127.0.0.1:26657"})

	if err != nil {
		t.Fatalf("NewOracle() = %v", err)
	}
	if oracle != nil {
		t.Fatal("NewOracle() built a follower nobody asked for")
	}
}

// Height sync off means the follower is off too, whatever the oracle flag says on its own.
func TestNoFollowerWhileHeightSyncIsOff(t *testing.T) {
	oracle, err := NewOracle(config.HeightSync{ChainOracle: true}, OracleSources{CometRPC: "http://127.0.0.1:26657"})

	if err != nil {
		t.Fatalf("NewOracle() = %v", err)
	}
	if oracle != nil {
		t.Fatal("NewOracle() built a follower for a gateway that carries no heights")
	}
}

// A follower with nothing to follow is not a follower: it would answer every read with a miss and
// label every carried tip untrusted.
func TestNoFollowerWithoutASource(t *testing.T) {
	oracle, err := NewOracle(config.HeightSync{Enabled: true, ChainOracle: true}, OracleSources{})

	if err != nil {
		t.Fatalf("NewOracle() = %v", err)
	}
	if oracle != nil {
		t.Fatal("NewOracle() built a follower with no source behind it")
	}
}

// The follower owns a live subscription and a dialled client, so closing it has to be possible and
// has to be safe twice: shutdown runs it, and a failed boot may too.
func TestClosingTheFollowerTwiceIsSafe(t *testing.T) {
	oracle, err := NewOracle(config.HeightSync{Enabled: true, ChainOracle: true},
		OracleSources{CometRPC: "http://127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewOracle() = %v", err)
	}
	if oracle == nil {
		t.Fatal("NewOracle() built nothing for a named Comet endpoint")
	}

	if err := oracle.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if err := oracle.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

// A nil follower is what an off gateway holds, and shutdown closes it like any other.
func TestClosingNothingIsFine(t *testing.T) {
	var oracle *Oracle

	if err := oracle.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
}

// A nil *Oracle assigned into an interface field is not nil, and the transport would call it on every
// carried tip. The courier must leave the field unset instead.
func TestACourierWithoutAFollowerHoldsNoOracle(t *testing.T) {
	var absent *Oracle

	courier := Courier(config.HeightSync{Enabled: true}, absent)

	if courier == nil {
		t.Fatal("Courier() = nil for an enabled height sync")
	}
	if courier.HeightSyncLogOracle != nil {
		t.Fatal("the courier holds a follower that does not exist")
	}
}

// A follower that does exist is what labels a carried tip trusted, so the courier has to carry it.
func TestACourierCarriesTheFollowerItWasGiven(t *testing.T) {
	oracle, err := NewOracle(config.HeightSync{Enabled: true, ChainOracle: true},
		OracleSources{CometRPC: "http://127.0.0.1:1"})
	if err != nil || oracle == nil {
		t.Fatalf("NewOracle() = %v, %v", oracle, err)
	}
	t.Cleanup(func() { _ = oracle.Close() })

	courier := Courier(config.HeightSync{Enabled: true}, oracle)

	if courier.HeightSyncLogOracle == nil {
		t.Fatal("the courier dropped the follower it was given")
	}
}
