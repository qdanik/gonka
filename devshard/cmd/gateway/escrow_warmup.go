package main

import (
	"context"
	"fmt"
	"time"

	"common/completionapi"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/internal/logkey"
	"devshard/cmd/gateway/registry"
	"devshard/host"
	"devshard/logging"
	"devshard/user"
)

const (
	warmupProbeTimeout   = 2 * time.Minute
	warmupCatchUpTimeout = 2 * time.Minute
	warmupBudget         = 20 * time.Minute
	warmupMaxTokens      = uint64(completionapi.MinTokensFloor)
)

var warmupPrompt = fmt.Appendf(nil, `{"messages":[{"role":"user","content":"."}],"max_tokens":%d}`, warmupMaxTokens)

type warmupEscrows interface {
	Acquire(escrowID string) (registry.EscrowSession, func(), bool)
}

type warmupLedger interface {
	RecordRace(escrowID string, attempts []accounting.Attempt) error
}

// EscrowPublished is announced under the registry's lock, so it must return without doing work.
type escrowWarmup struct {
	escrows warmupEscrows
	ledger  warmupLedger
	probe   func(ctx context.Context, session registry.EscrowSession, model string, startedAt int64, nonceCommitted func()) (uint64, bool, error)
	catchUp func(ctx context.Context, session registry.EscrowSession) error
	stop    <-chan struct{}
	now     func() time.Time
}

func newEscrowWarmup(configuration *config.Holder, ledger *accounting.Book, now func() time.Time) *escrowWarmup {
	if configuration == nil || !configuration.Load().Scheduler.WarmNewEscrows {
		return nil
	}
	warmup := &escrowWarmup{probe: dispatchProbe, catchUp: catchUpAllHosts, now: now}
	if ledger != nil {
		warmup.ledger = ledger
	}
	return warmup
}

func (w *escrowWarmup) start(ctx context.Context) {
	if w != nil {
		w.stop = ctx.Done()
	}
}

func (w *escrowWarmup) EscrowPublished(escrowID, model string) {
	if w == nil || w.escrows == nil {
		return
	}
	go w.warm(escrowID, model)
}

func (w *escrowWarmup) warm(escrowID, model string) {
	session, release, live := w.escrows.Acquire(escrowID)
	if !live {
		return
	}
	defer release()
	if session.Nonce() != 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), warmupBudget)
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
			catchUpCtx, cancelCatchUp := context.WithTimeout(ctx, warmupCatchUpTimeout)
			defer cancelCatchUp()
			done <- w.catchUp(catchUpCtx, session)
		}(catchUp)
	}

	nonce, acknowledged, probeErr := w.probe(ctx, session, model, w.now().Unix(), onNonceCommitted)

	var catchUpErr error
	if catchUp != nil {
		catchUpErr = <-catchUp
	}

	if nonce == 0 {
		logging.Warn("escrow warmup found no nonce to spend", logkey.Escrow, escrowID, logkey.Error, probeErr)
		return
	}
	w.record(escrowID, nonce, acknowledged, probeErr)

	logging.Info("escrow warmed", logkey.Escrow, escrowID, logkey.Model, model,
		logkey.Nonce, nonce, logkey.Served, probeErr == nil, logkey.CatchUpError, catchUpErr)
}

func catchUpAllHosts(ctx context.Context, session registry.EscrowSession) error {
	return session.UserSession().CatchUpAllHosts(ctx)
}

func dispatchProbe(ctx context.Context, session registry.EscrowSession, model string, startedAt int64, nonceCommitted func()) (uint64, bool, error) {
	sendCtx, cancelSend := context.WithTimeout(ctx, warmupProbeTimeout)
	defer cancelSend()

	prepared, err := session.PrepareInferenceFn(func(user.HostBinding) (user.InferenceParams, bool, error) {
		return user.InferenceParams{
			Model:       model,
			Prompt:      warmupPrompt,
			InputLength: uint64(len(warmupPrompt)),
			MaxTokens:   warmupMaxTokens,
			StartedAt:   startedAt,
		}, false, nil
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

func (w *escrowWarmup) record(escrowID string, nonce uint64, acknowledged bool, probeErr error) {
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
