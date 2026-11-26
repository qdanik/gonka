package shadow

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"devshard/cmd/gateway/accounting"
	"devshard/cmd/gateway/engine"
	"devshard/cmd/gateway/registry"
	"devshard/cmd/gateway/scheduler"
	"devshard/cmd/gateway/warmup"
	"devshard/user"
)

// PassBinding is everything the pass runner needs that only exists after the scheduler is built.
type PassBinding struct {
	Escrows  Escrows
	Lifetime context.Context
	Voter    warmup.Voter
	Probes   warmup.Probes
	Timeout  func(model string) time.Duration
	Cap      func(participant, model string) int
	Faulted  func(participant, model string, launchedAt time.Time)
}

// PassRunner satisfies scheduler.PassSender: it sends the one-token probe of a pass, or posts its vote when no probe may go. See docs/proposals/gateway-weight-proportional-load.md, "A pass is a one-token probe or an owed vote".
type PassRunner struct {
	binding PassBinding
	now     func() time.Time
	send    sendFunc

	mu       sync.Mutex
	inFlight map[probeKey]int
}

type probeKey struct{ participant, model string }

var _ scheduler.PassSender = (*PassRunner)(nil)

func NewPassRunner(now func() time.Time) *PassRunner {
	return &PassRunner{now: now, send: dispatch, inFlight: map[probeKey]int{}}
}

// Bind is called once, before the gateway serves; a runner never bound takes no pass.
func (r *PassRunner) Bind(binding PassBinding) {
	r.binding = binding
}

func (r *PassRunner) Pass(request scheduler.PassRequest) bool {
	prepared, isPrepared := request.Assignment.Nonce.(*user.PreparedInference)
	if r.binding.Escrows == nil || r.binding.Voter.Posters == nil || !isPrepared {
		return false
	}
	pass := sentPass{request: request, prepared: prepared, params: registry.PlaceholderParams(request.Model, request.StartedAt)}
	if request.Send && r.takeProbe(request) {
		if session, release, held := r.binding.Escrows.AcquireCommitted(request.Assignment.Escrow); held {
			request.Assignment.ReleaseEscrow()
			pass.session, pass.release = session, release
			go r.probe(pass)
			return true
		}
		r.returnProbe(request)
	}
	session, release, held := r.binding.Escrows.HoldForVote(request.Assignment.Escrow)
	if !held {
		return false
	}
	request.Assignment.ReleaseEscrow()
	pass.session, pass.release = session, release
	go r.owe(pass)
	return true
}

type sentPass struct {
	request  scheduler.PassRequest
	prepared *user.PreparedInference
	params   user.InferenceParams
	session  registry.EscrowSession
	release  func()
}

func (r *PassRunner) takeProbe(request scheduler.PassRequest) bool {
	key := probeKey{request.Assignment.Host, request.Model}
	capacity := r.binding.Cap(key.participant, key.model)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inFlight[key] >= capacity {
		return false
	}
	r.inFlight[key]++
	return true
}

func (r *PassRunner) returnProbe(request scheduler.PassRequest) {
	key := probeKey{request.Assignment.Host, request.Model}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inFlight[key]--; r.inFlight[key] <= 0 {
		delete(r.inFlight, key)
	}
}

func (r *PassRunner) probe(pass sentPass) {
	request := pass.request
	sendCtx, cancelSend := context.WithTimeout(r.binding.Lifetime, r.binding.Timeout(request.Model))
	defer cancelSend()

	launched := r.now()
	answer := newCapture(r.now)
	meter := engine.NewStreamMeter(r.now, request.Assignment.Host, pass.prepared)
	var acknowledged atomic.Bool
	pass.prepared.OnSend(func() {
		meter.Sent()
		answer.Sent()
	})
	response, err := r.send(sendCtx, pass.session, pass.prepared, io.MultiWriter(answer, meter), func() {
		acknowledged.Store(true)
		meter.Receipt()
	})
	r.returnProbe(request)
	answered := acknowledged.Load() || (response != nil && len(response.Receipt) > 0)
	var confirmedAt int64
	if response != nil {
		confirmedAt = response.ConfirmedAt
	}
	read := meter.Outcome(confirmedAt, err)
	r.record(request, pass.prepared.Nonce(), answer, answered, read)
	pass.session.ArmQuietSettle()
	if err == nil {
		pass.release()
		request.Resolved(scheduler.PassAnswered)
		return
	}
	if r.binding.Faulted != nil {
		r.binding.Faulted(request.Assignment.Host, request.Model, launched)
	}
	if _, releaseVote, held := r.binding.Escrows.HoldForVote(request.Assignment.Escrow); held {
		pass.release()
		pass.release = releaseVote
	}
	r.vote(pass, answered, scheduler.PassMissed)
}

func (r *PassRunner) owe(pass sentPass) {
	r.vote(pass, false, scheduler.PassOwed)
}

func (r *PassRunner) vote(pass sentPass, answered bool, posted scheduler.PassOutcome) {
	defer pass.release()
	request := pass.request
	event, voted := r.binding.Voter.Settle(r.binding.Lifetime, request.Assignment.Escrow, request.Model, pass.params, pass.prepared.Nonce(), answered)
	pass.session.ArmQuietSettle()
	if !voted || event.Action != engine.TimeoutActionCompleted {
		request.Resolved(scheduler.PassUnvoted)
		return
	}
	request.Resolved(posted)
}

func (r *PassRunner) record(request scheduler.PassRequest, nonce uint64, answer *capture, answered bool, read engine.AttemptOutcome) {
	if r.binding.Probes == nil {
		return
	}
	line := accounting.Attempt{
		Nonce:             nonce,
		OutputTokens:      int64(answer.completionTokens()),
		FirstContentDelay: read.FirstContentDelay(),
		Sent:              true,
		Acknowledged:      answered,
		Usage:             accounting.UsageLoser,
		Phase:             accounting.PhaseNormal,
		Terminal:          accounting.TerminalBusyProbe,
	}
	r.binding.Probes.ProbeRecorded(request.Assignment.Escrow, line.WithTiming(read))
}
