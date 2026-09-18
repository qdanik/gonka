package scheduler

// Stop shuts every escrow's actor down and is safe to call more than once.
func (s *Scheduler) Stop() {
	s.registryMu.Lock()
	s.stopped = true
	running := make([]*dispatcher, 0, len(s.dispatchers))
	for _, active := range s.dispatchers {
		running = append(running, active)
	}
	clear(s.dispatchers)
	s.registryMu.Unlock()

	for _, active := range running {
		active.stop()
	}
}

func (s *Scheduler) dispatcherFor(escrow Escrow) (*dispatcher, error) {
	s.registryMu.Lock()
	defer s.registryMu.Unlock()
	if s.stopped {
		return nil, ErrDispatcherStopped
	}

	// A reopened escrow needs its own dispatcher: the old one still holds the retired session.
	target, known := s.dispatchers[escrow.ID]
	if !known || target.isStopped() || target.sessionID != escrow.SessionID {
		target = newDispatcher(dispatcherDeps{
			escrowID:            escrow.ID,
			sessionID:           escrow.SessionID,
			session:             escrow.Session,
			snapshots:           s.snapshots,
			predicates:          s.predicates(escrow),
			acquireSlot:         s.acquireSlot(escrow),
			holdEscrow:          escrow.Hold,
			observer:            s.observer,
			now:                 s.now,
			matchWait:           s.matchWait(),
			maxConsecutiveBurns: s.maxConsecutiveBurns,
			retirementReserve:   s.retirementReserve,
			newTimer:            s.newTimer,
			retire:              s.retire,
			idleGrace:           idleDispatcherGrace,
			submitBuffer:        s.submitBuffer,
			onExhausted:         s.onEscrowExhausted,
		})
		s.dispatchers[escrow.ID] = target
		target.start()
	}
	// Claimed under the registry lock and released when Pick returns, so an actor cannot retire while its caller holds a waiter or an assignment.
	target.pendingSubmits.Add(1)
	return target, nil
}

func (s *Scheduler) retire(idle *dispatcher) bool {
	s.registryMu.Lock()
	defer s.registryMu.Unlock()
	if idle.pendingSubmits.Load() != 0 || !idle.markStopped() {
		return false
	}
	if s.dispatchers[idle.escrowID] == idle {
		delete(s.dispatchers, idle.escrowID)
	}
	if s.observer != nil {
		s.observer.EscrowRetired(idle.escrowID)
	}
	// The block and the spent replay outlive the actor on purpose. See README, "Dispatcher lifecycle".
	return true
}
