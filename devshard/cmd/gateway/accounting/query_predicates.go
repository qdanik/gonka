package accounting

// A nonce whose timeout was raised but has settled on no outcome is still owed one. The caller has
// already asked timeoutOutcomeOf and been told the round is unsettled, so this only names the
// dispositions a timeout can be owed for.
func awaitingTimeout(key CounterKey) bool {
	return key.Disposition == DispositionUnfinishedRefused ||
		key.Disposition == DispositionUnfinishedExecution
}

// namesNoReason is the ledger's check on itself: a nonce whose cause it could not name. It matches the
// fallbacks the producers actually emit -- an engine terminal with no string of its own, an attempt the
// race never classified, and a burn kind with no reason. "unreported" is not one of them: that names a
// race that reported nothing, which is a fact rather than a gap.
func namesNoReason(key CounterKey) bool {
	return key.Terminal == TerminalUnnamed ||
		key.Terminal == TerminalUnclassified ||
		(key.Disposition == DispositionGhost && key.GhostReason == "")
}
