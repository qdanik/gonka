// Package warmup spends one nonce on a newly published escrow so every host in its group learns the
// escrow exists, and settles that nonce itself: nothing else ever will.
package warmup

import (
	"context"
	"fmt"
	"time"

	"common/completionapi"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/host"
	"devshard/logging"
	"devshard/user"
)

const (
	probeTimeout   = 2 * time.Minute
	catchUpTimeout = 2 * time.Minute
	budget         = 20 * time.Minute
	probeMaxTokens = uint64(completionapi.MinTokensFloor)
)

var probePrompt = fmt.Appendf(nil, `{"messages":[{"role":"user","content":"."}],"max_tokens":%d}`, probeMaxTokens)

// Posters resolves the vote poster for one escrow and the params its nonce committed.
type Posters func(escrowID string, params any) (engine.TimeoutPoster, bool)

type Escrows interface {
	Acquire(escrowID string) (registry.EscrowSession, func(), bool)
}

type ledger interface {
	RecordRace(escrowID string, attempts []accounting.Attempt) error
}

// EscrowPublished is announced under the registry's lock, so it must return without doing work.
type Prober struct {
	escrows  Escrows
	ledger   ledger
	posters  Posters
	timeouts Timeouts
	probe    func(ctx context.Context, session registry.EscrowSession, params user.InferenceParams, nonceCommitted func()) (uint64, bool, error)
	catchUp  func(ctx context.Context, session registry.EscrowSession) error
	stop     <-chan struct{}
	now      func() time.Time
}

func New(configuration *config.Holder, ledger *accounting.Book, now func() time.Time) *Prober {
	if configuration == nil || !configuration.Load().Scheduler.WarmNewEscrows {
		return nil
	}
	warmup := &Prober{probe: dispatchProbe, catchUp: catchUpAllHosts, now: now}
	if ledger != nil {
		warmup.ledger = ledger
	}
	return warmup
}

func (w *Prober) Start(ctx context.Context) {
	if w != nil {
		w.stop = ctx.Done()
	}
}

func (w *Prober) EscrowPublished(escrowID, model string) {
	if w == nil || w.escrows == nil {
		return
	}
	go w.warm(escrowID, model)
}

func (w *Prober) warm(escrowID, model string) {
	session, release, live := w.escrows.Acquire(escrowID)
	if !live {
		return
	}
	defer release()
	if session.Nonce() != 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	go func() {
		select {
		case <-w.stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	// The probe's diff is signed before its send, so the group can be taught now instead of an inference later.
	var catchUp chan error
	onNonceCommitted := func() {
		catchUp = make(chan error, 1)
		go func(done chan<- error) {
			catchUpCtx, cancelCatchUp := context.WithTimeout(ctx, catchUpTimeout)
			defer cancelCatchUp()
			done <- w.catchUp(catchUpCtx, session)
		}(catchUp)
	}

	params := probeParams(model, w.now().Unix())
	nonce, acknowledged, probeErr := w.probe(ctx, session, params, onNonceCommitted)

	var catchUpErr error
	if catchUp != nil {
		catchUpErr = <-catchUp
	}

	if nonce == 0 {
		logging.Warn("escrow warmup found no nonce to spend", logkey.Escrow, escrowID, logkey.Error, probeErr)
		return
	}
	w.record(escrowID, nonce, acknowledged, probeErr)
	if probeErr != nil {
		w.settleRefusedProbe(ctx, escrowID, model, params, nonce)
	}

	logging.Info("escrow warmed", logkey.Escrow, escrowID, logkey.Model, model,
		logkey.Nonce, nonce, logkey.Served, probeErr == nil, logkey.CatchUpError, catchUpErr)
}

func catchUpAllHosts(ctx context.Context, session registry.EscrowSession) error {
	return session.UserSession().CatchUpAllHosts(ctx)
}

func probeParams(model string, startedAt int64) user.InferenceParams {
	return user.InferenceParams{
		Model:       model,
		Prompt:      probePrompt,
		InputLength: uint64(len(probePrompt)),
		MaxTokens:   probeMaxTokens,
		StartedAt:   startedAt,
	}
}

func dispatchProbe(ctx context.Context, session registry.EscrowSession, params user.InferenceParams, nonceCommitted func()) (uint64, bool, error) {
	sendCtx, cancelSend := context.WithTimeout(ctx, probeTimeout)
	defer cancelSend()

	prepared, err := session.PrepareInferenceFn(func(user.HostBinding) (user.InferenceParams, bool, error) {
		return params, false, nil
	})
	if err != nil || prepared == nil {
		return 0, false, err
	}
	nonce, hostIdx := prepared.Nonce(), prepared.HostIdx()
	nonceCommitted()

	response, sendErr := session.UserSession().SendOnly(sendCtx, prepared, nil, nil)
	if sendErr != nil {
		return nonce, false, sendErr
	}
	return nonce, executorAcknowledged(response), session.UserSession().ProcessResponse(hostIdx, response, nonce)
}

func executorAcknowledged(response *host.HostResponse) bool {
	return response != nil && len(response.Receipt) > 0
}

func (w *Prober) record(escrowID string, nonce uint64, acknowledged bool, probeErr error) {
	if w.ledger == nil {
		return
	}
	attempt := accounting.Attempt{
		Nonce:        nonce,
		Sent:         true,
		Finished:     probeErr == nil,
		Acknowledged: acknowledged,
		Usage:        accounting.UsageLoser,
		Phase:        accounting.PhaseNormal,
		Terminal:     accounting.TerminalWarmupProbe,
	}
	if err := w.ledger.RecordRace(escrowID, []accounting.Attempt{attempt}); err != nil {
		logging.Warn("escrow warmup could not settle its nonce", logkey.Escrow, escrowID, logkey.Nonce, nonce, logkey.Error, err)
	}
}

// Serve and Settle are late bindings: the registry exists only after the warmup it publishes to, and
// the vote path only after the sessions the race shares with it.
func (w *Prober) Serve(escrows Escrows) {
	if w != nil {
		w.escrows = escrows
	}
}

func (w *Prober) Settle(posters Posters, timeouts Timeouts) {
	if w != nil {
		w.posters, w.timeouts = posters, timeouts
	}
}
