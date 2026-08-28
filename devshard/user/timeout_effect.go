package user

import (
	"devshard/types"
)

// timeoutTookEffect also accepts a record already timed out: a peer's diff may have carried the
// timeout before ours was composed.
func (s *Session) timeoutTookEffect(diff types.Diff, nonce uint64) bool {
	if HasMsgTimeout(diff.Txs, nonce) {
		return true
	}
	record, found := s.sm.GetInference(nonce)
	return found && record.Status == types.StatusTimedOut
}
