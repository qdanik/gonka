package config

type retiredParticipantKeys struct {
	ParticipantAllowlist    *[]string `json:"participant_allowlist"`
	UnthrottledParticipants *[]string `json:"unthrottled_participants"`
}

func (r retiredParticipantKeys) foldInto(overrides Overrides) Overrides {
	if r.ParticipantAllowlist == nil && r.UnthrottledParticipants == nil {
		return overrides
	}
	var folded ParticipantOverrides
	if overrides.Participants != nil {
		folded = *overrides.Participants
	}
	if folded.Trust == nil {
		folded.Trust = r.ParticipantAllowlist
	}
	if folded.Unthrottled == nil {
		folded.Unthrottled = r.UnthrottledParticipants
	}
	overrides.Participants = &folded
	return overrides
}
