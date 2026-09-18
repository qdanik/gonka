package scheduler

import (
	"slices"
	"strings"
)

// refusedByAllowlist stays nil when nobody narrowed routing, so the drain reads "no allowlist" as no rung at all.
func refusedByAllowlist(allowlist []string) func(participant string) bool {
	if len(allowlist) == 0 {
		return nil
	}
	allowed := allowedParticipants(allowlist)
	return func(participant string) bool { return !allowed(participant) }
}

// allowedParticipants answers true for everybody when the list is empty.
func allowedParticipants(allowlist []string) func(participant string) bool {
	if len(allowlist) == 0 {
		return func(string) bool { return true }
	}
	allowed := make(map[string]bool, len(allowlist))
	for _, participant := range allowlist {
		allowed[strings.TrimSpace(participant)] = true
	}
	return func(participant string) bool { return allowed[participant] }
}

// reachableByAllowlist is built once per pick, and an empty allowlist skips the walk entirely.
func reachableByAllowlist(allowlist []string) func(Escrow) bool {
	if len(allowlist) == 0 {
		return func(Escrow) bool { return true }
	}
	allowed := allowedParticipants(allowlist)
	return func(candidate Escrow) bool {
		return candidate.Session != nil &&
			slices.ContainsFunc(candidate.Session.ParticipantKeys(), allowed)
	}
}

func (s *Scheduler) participantAllowlist() []string {
	if s.settings == nil {
		return nil
	}
	return s.settings.Load().Scheduler.ParticipantAllowlist
}
