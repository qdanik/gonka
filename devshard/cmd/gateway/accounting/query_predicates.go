package accounting

// Names only the dispositions a timeout can be owed for; the caller has already found the round unsettled.
func awaitingTimeout(key CounterKey) bool {
	return key.Disposition == DispositionUnfinishedRefused ||
		key.Disposition == DispositionUnfinishedExecution
}

// The ledger's check on itself. See README.md, "The ledger's check on itself".
func namesNoReason(key CounterKey) bool {
	return key.Terminal == TerminalUnnamed ||
		key.Terminal == TerminalUnclassified ||
		(key.Disposition == DispositionGhost && key.GhostReason == "")
}
