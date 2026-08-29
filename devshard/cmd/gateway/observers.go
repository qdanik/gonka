package main

import (
	"sync"

	"devshard/cmd/gateway/burns"
	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/metrics"
	"devshard/cmd/gateway/nonces"
	"devshard/cmd/gateway/scheduler"
	"devshard/logging"
)

// tracedDispatches narrates dispatch events and forwards them on. See README.md, "Two readers of one fact".
type tracedDispatches struct {
	recorder *metrics.DispatchRecorder
	ledger   *nonces.Recorder
	charges  *burns.Accountant
}

// GhostBurned logs the nonce and never labels it: a counter keyed by nonce would grow without end.
func (t tracedDispatches) GhostBurned(escrowID string, burned scheduler.Burn) {
	logging.Warn("nonce burned for nobody", logkey.Escrow, escrowID, logkey.Nonce, burned.Nonce,
		logkey.Host, logkey.ShortHost(burned.Participant), logkey.Reason, burned.Reason)
	t.recorder.GhostBurned(escrowID, burned.Participant, burned.Reason)
	t.ledger.RecordGhost(escrowID, burned.Nonce, burned.Reason)
	t.charges.Burned(escrowID, burned)
}

// BurnBudgetExhausted is rare and changes what the escrow does: queued callers now wait rather than spend.
func (t tracedDispatches) BurnBudgetExhausted(escrowID string) {
	logging.Warn("escrow stopped burning nonces at its budget", logkey.Escrow, escrowID)
	t.recorder.BurnBudgetExhausted(escrowID)
}

// NonceHeld and EscrowRetired pass straight through: both are already written down where they happen.
func (t tracedDispatches) NonceHeld(escrowID string) { t.recorder.NonceHeld(escrowID) }

func (t tracedDispatches) EscrowRetired(escrowID string) { t.recorder.EscrowRetired(escrowID) }

// phaseNarrator writes a line only when something changed; the observer polls twelve times a minute.
type phaseNarrator struct {
	mu      sync.Mutex
	started bool
	epoch   uint64
	phase   chain.EpochPhase
	blocked bool
	reason  chain.BlockReason
}

// phaseChange separates deciding from saying, so writing nothing when nothing moved can be tested.
type phaseChange struct {
	first bool
	epoch bool
	block bool
}

// advance is called synchronously on every publish, so it holds its lock only to compare and update.
func (n *phaseNarrator) advance(snapshot chain.PhaseSnapshot) phaseChange {
	n.mu.Lock()
	defer n.mu.Unlock()
	change := phaseChange{
		first: !n.started,
		epoch: snapshot.EpochIndex != n.epoch || snapshot.EpochPhase != n.phase,
		block: snapshot.RequestsBlocked != n.blocked || snapshot.BlockReason != n.reason,
	}
	n.started, n.epoch, n.phase = true, snapshot.EpochIndex, snapshot.EpochPhase
	n.blocked, n.reason = snapshot.RequestsBlocked, snapshot.BlockReason
	return change
}

func (n *phaseNarrator) observe(snapshot chain.PhaseSnapshot) {
	change := n.advance(snapshot)
	first := change.first
	epochChanged := change.epoch
	blockChanged := change.block

	if first || epochChanged {
		logging.Info("chain epoch",
			logkey.Epoch, snapshot.EpochIndex, logkey.Phase, snapshot.EpochPhase,
			logkey.Height, snapshot.BlockHeight, logkey.SwitchHeight, snapshot.EpochSwitchBlockHeight)
	}
	if !first && !blockChanged {
		return
	}
	if snapshot.RequestsBlocked {
		logging.Warn("chain blocked requests", logkey.Reason, snapshot.BlockReason, logkey.Epoch, snapshot.EpochIndex, logkey.Height, snapshot.BlockHeight)
		return
	}
	if !first {
		logging.Info("chain unblocked requests", logkey.Epoch, snapshot.EpochIndex, logkey.Height, snapshot.BlockHeight)
	}
}

// nonceAccountedRaces hands one race outcome to both readers of it. See README.md, "Two readers of one fact".
type nonceAccountedRaces struct {
	recorder *metrics.RaceRecorder
	ledger   *nonces.Recorder
}

func (r nonceAccountedRaces) RecordRace(outcome engine.RaceOutcome) {
	r.recorder.RecordRace(outcome)
	r.ledger.RecordRace(outcome)
}

func (r nonceAccountedRaces) RecordTimeout(event engine.TimeoutEvent) {
	r.recorder.RecordTimeout(event)
	r.ledger.RecordTimeout(event)
}

// RecordClassifyOverflow passes straight through: an overflowing classifier says nothing about a nonce.
func (r nonceAccountedRaces) RecordClassifyOverflow(participant, model string) {
	r.recorder.RecordClassifyOverflow(participant, model)
}
