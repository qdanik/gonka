package scheduler

// AffinityHint is the extension point for KV-cache affinity: the per-request handle a later revision
// will rank hosts with. It carries nothing and changes no decision today.
type AffinityHint struct{}
