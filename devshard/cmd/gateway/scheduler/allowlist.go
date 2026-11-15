package scheduler

import (
	"slices"
	"strings"

	"devshard/cmd/gateway/config"
)

// refusedByAllowlist stays nil when nobody narrowed routing, so the drain reads "no allowlist" as no rung at all.
func refusedByAllowlist(allowlist, unthrottled []string) func(participant string) bool {
	if len(allowlist) == 0 {
		return nil
	}
	admitted := admittedParticipants(allowlist, unthrottled)
	return func(participant string) bool { return !admitted(participant) }
}

// admittedParticipants answers true for everybody when the allowlist is empty, and admits an unthrottled participant whether or not it names one.
func admittedParticipants(allowlist, unthrottled []string) func(participant string) bool {
	if len(allowlist) == 0 {
		return func(string) bool { return true }
	}
	admitted := namedParticipants(allowlist)
	for _, participant := range unthrottled {
		admitted[strings.TrimSpace(participant)] = true
	}
	return func(participant string) bool { return admitted[participant] }
}

// waivesThrottling stays nil when no list waives anything, so an unset list adds no rung of its own.
func waivesThrottling(unthrottled []string) func(participant string) bool {
	if len(unthrottled) == 0 {
		return nil
	}
	named := namedParticipants(unthrottled)
	return func(participant string) bool { return named[participant] }
}

func namedParticipants(list []string) map[string]bool {
	named := make(map[string]bool, len(list))
	for _, participant := range list {
		named[strings.TrimSpace(participant)] = true
	}
	return named
}

// reachableByAllowlist is built once per pick, and an empty trust list with no shadow list skips the walk entirely.
func reachableByAllowlist(participants config.Participants) func(Escrow) bool {
	if len(participants.Trust) == 0 && len(participants.Shadow) == 0 {
		return func(Escrow) bool { return true }
	}
	admitted := admittedParticipants(participants.Trust, participants.Unthrottled)
	shadow := namedParticipants(participants.Shadow)
	routable := func(participant string) bool { return admitted(participant) && !shadow[participant] }
	return func(candidate Escrow) bool {
		return candidate.Session != nil &&
			slices.ContainsFunc(candidate.Session.ParticipantKeys(), routable)
	}
}

func markedShadow(shadow []string) func(participant string) bool {
	if len(shadow) == 0 {
		return nil
	}
	named := namedParticipants(shadow)
	return func(participant string) bool { return named[participant] }
}

func (s *Scheduler) participants() config.Participants {
	if s.settings == nil {
		return config.Participants{}
	}
	return s.settings.Load().Scheduler.Participants
}
