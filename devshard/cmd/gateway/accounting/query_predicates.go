package accounting

// A nonce whose timeout was raised but has settled on no outcome is still owed one. The caller has
// already asked timeoutOutcomeOf and been told the round is unsettled, so this only names the
// dispositions a timeout can be owed for.
func awaitingTimeout(key CounterKey) bool {
	return key.Disposition == DispositionUnfinishedRefused ||
		key.Disposition == DispositionUnfinishedExecution
}

// namesNoReason counts a bucket whose producer reached the ledger without naming why. It is the one
// number that says how much of the breakdown is guesswork rather than measurement.
func namesNoReason(key CounterKey) bool {
	const unnamed = "unknown"
	return key.GhostReason == unnamed ||
		key.TimeoutReason == unnamed ||
		key.Terminal == unnamed
}
