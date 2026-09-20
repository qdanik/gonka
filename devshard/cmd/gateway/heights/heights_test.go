package heights

import (
	"context"
	"testing"
	"time"

	"devshard/cmd/gateway/config"
	"devshard/heightsync"
)

type cadenceSpy struct{ started int }

func (s *cadenceSpy) StartHeartbeatLoop() { s.started++ }

// A quiet escrow syncs only because the user side opens a heartbeat turn; nothing else can. The
// cadence is started per session, once.
func TestAHeightSyncingSessionOpensItsOwnCadence(t *testing.T) {
	cadence := &cadenceSpy{}

	StartCadence(cadence, config.HeightSync{Enabled: true})

	if cadence.started != 1 {
		t.Fatalf("the cadence started %d times, want 1", cadence.started)
	}
}

// Height sync is opt-in across the fleet. A gateway whose hosts do not carry it would log a skipped
// heartbeat every interval and stamp nothing, so the cadence stays absent rather than idling.
func TestAGatewayWithoutHeightSyncOpensNoCadence(t *testing.T) {
	cadence := &cadenceSpy{}

	StartCadence(cadence, config.HeightSync{})

	if cadence.started != 0 {
		t.Fatalf("the cadence started %d times, want none", cadence.started)
	}
}

// The gateway stamps its own anchors from what the hosts told it, so the scheduler has to read the
// very cache the clients write into. A scheduler over a second, empty cache would stamp nothing and
// the fault would show only as a quiet escrow that never syncs.
func TestTheCourierStampsFromTheCacheItsClientsFill(t *testing.T) {
	courier := Courier(config.HeightSync{Enabled: true}, nil)

	if courier == nil {
		t.Fatal("Courier() = nil for an enabled height sync")
	}
	if courier.HeightSyncPeerTips == nil {
		t.Fatal("the courier carries no peer-tip cache, so the seed has nowhere to land")
	}
	courier.HeightSyncPeerTips.RecordOriginWithBlob(&heightsync.HeightSyncSection{
		MainnetHeight:         4242,
		MainnetBlockHashHex:   "ab",
		OriginatorSenderID:    "host-a",
		OriginatorTimestampMs: time.Now().UnixMilli(),
	}, []byte("origin-blob"), []byte("origin-signature"))

	section, err, oracleMiss := courier.HeightSync.Decide(context.Background(), heightsync.DecideHints{Nonce: 1})

	if err != nil {
		t.Fatalf("Decide() = %v", err)
	}
	if oracleMiss {
		t.Fatal("the scheduler missed its oracle, so it is not reading the cache the clients fill")
	}
	if section == nil || section.MainnetHeight != 4242 {
		t.Fatalf("the anchor carries %+v, want the height the cache holds", section)
	}
}

// Height sync is opt-in: a gateway without it dials as it always did, carrying no envelope.
func TestAGatewayWithoutHeightSyncCarriesNoCourier(t *testing.T) {
	if courier := Courier(config.HeightSync{}, nil); courier != nil {
		t.Fatalf("Courier() = %+v, want nothing", courier)
	}
}

// A height nobody signed is a height anybody could have claimed. The cache holds it and refuses to
// serve it, so the gateway never carries an unattributable tip into the log.
func TestAnUnsignedTipIsNeverStamped(t *testing.T) {
	courier := Courier(config.HeightSync{Enabled: true}, nil)
	courier.HeightSyncPeerTips.RecordOrigin(&heightsync.HeightSyncSection{
		MainnetHeight:         4242,
		MainnetBlockHashHex:   "ab",
		OriginatorSenderID:    "host-a",
		OriginatorTimestampMs: time.Now().UnixMilli(),
	})

	section, _, oracleMiss := courier.HeightSync.Decide(context.Background(), heightsync.DecideHints{Nonce: 1})

	if !oracleMiss || section != nil {
		t.Fatalf("the scheduler stamped %+v from an unsigned tip", section)
	}
}
