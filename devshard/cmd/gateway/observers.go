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

// tracedDispatches narrates the dispatch events an operator would otherwise have to infer from a
// counter's slope, and forwards every one to the recorder. It wraps rather than living inside metrics
// because counting and narrating are different jobs, and the scheduler stays free of a logger.
type tracedDispatches struct {
	recorder *metrics.DispatchRecorder
	ledger   *nonces.Recorder
	charges  *burns.Accountant
}

// GhostBurned is a nonce that cost money on chain and will serve nobody. The nonce is logged and never
// labelled: a counter keyed by it would grow without end.
func (t tracedDispatches) GhostBurned(escrowID string, burned scheduler.Burn) {
	logging.Warn("nonce burned for nobody", logkey.Escrow, escrowID, logkey.Nonce, burned.Nonce,
		logkey.Host, logkey.ShortHost(burned.Participant), logkey.Reason, burned.Reason)
	t.recorder.GhostBurned(escrowID, burned.Participant, burned.Reason)
	t.ledger.RecordGhost(escrowID, burned.Nonce, burned.Reason)
	t.charges.Burned(escrowID, burned)
}

// BurnBudgetExhausted means the escrow stopped burning nonces to answer requests it cannot serve, so
// queued callers now wait rather than spend. Rare, and it changes what the escrow does.
func (t tracedDispatches) BurnBudgetExhausted(escrowID string) {
	logging.Warn("escrow stopped burning nonces at its budget", logkey.Escrow, escrowID)
	t.recorder.BurnBudgetExhausted(escrowID)
}

// NonceHeld and EscrowRetired pass straight through: a held nonce is the ordinary case, and retirement
// is already written down where it happens.
func (t tracedDispatches) NonceHeld(escrowID string) { t.recorder.NonceHeld(escrowID) }

func (t tracedDispatches) EscrowRetired(escrowID string) { t.recorder.EscrowRetired(escrowID) }

// phaseNarrator turns the observer's five-second poll into a line only when something an operator cares
// about actually changed. Subscribing without this would write the same snapshot twelve times a minute.
type phaseNarrator struct {
	mu      sync.Mutex
	started bool
	epoch   uint64
	phase   chain.EpochPhase
	blocked bool
	reason  chain.BlockReason
}

// phaseChange is what one publish is worth saying about, separated from saying it so the decision can
// be tested: the whole value of this type is writing nothing when nothing moved.
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

// nonceAccountedRaces hands one race outcome to both readers of it. The recorder asks how the fleet
// performed; the ledger asks where each nonce went. Neither question belongs inside the other, and the
// engine should know about neither.
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
