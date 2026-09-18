package engine

func (o RaceOutcome) failure() error {
	switch {
	case o.Succeeded:
		return nil
	case len(o.Attempts) == 0:
		return ErrAllAttemptsFailed
	// The crowned attempt's bytes are already on the wire, so no other payload can take their place.
	case o.winnerStreamed():
		if hostErr := o.hostError(); hostErr != nil {
			return hostErr
		}
		return ErrWinnerIncomplete
	}
	if hostErr := o.hostError(); hostErr != nil {
		return hostErr
	}
	if o.everyAttempt(AttemptOutcome.emptyStream) {
		return ErrEmptyStream
	}
	return ErrAllAttemptsFailed
}

func (o RaceOutcome) winnerStreamed() bool {
	for _, attempt := range o.Attempts {
		if o.IsWinner(attempt) && attempt.ContentChunks > 0 {
			return true
		}
	}
	return false
}

// hostError prefers the crowned attempt's refusal, the answer the client asked for, then a trusted refusal or rejection that rules out a retry. See race.md, "Escalation".
func (o RaceOutcome) hostError() *HostApplicationError {
	var found *AttemptOutcome
	foundRetryRuledOut := false
	for index := range o.Attempts {
		attempt := &o.Attempts[index]
		if attempt.ErrorSource == "" {
			continue
		}
		if o.IsWinner(*attempt) {
			found = attempt
			break
		}
		retryRuledOut := rulesOutRetry(*attempt)
		if found == nil || (retryRuledOut && !foundRetryRuledOut) {
			found, foundRetryRuledOut = attempt, retryRuledOut
		}
	}
	if found == nil {
		return nil
	}
	return &HostApplicationError{
		Code:    found.ErrorCode,
		Type:    found.ErrorType,
		Message: found.ErrorMessage,
		Payload: found.ErrorPayload,
	}
}

func (o RaceOutcome) everyAttempt(holds func(AttemptOutcome) bool) bool {
	for _, attempt := range o.Attempts {
		if !holds(attempt) {
			return false
		}
	}
	return true
}
