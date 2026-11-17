package user

import "context"

type receiptDiscoveryKey struct{}

type receiptDiscovery struct {
	serving func() func(participant string) bool
}

// WithReceiptDiscovery makes each refusal vote round under ctx ask the verifiers that can serve first and end at the first recovered receipt; serving is read when a round starts and may be nil.
func WithReceiptDiscovery(ctx context.Context, serving func() func(participant string) bool) context.Context {
	return context.WithValue(ctx, receiptDiscoveryKey{}, receiptDiscovery{serving: serving})
}

func withoutReceiptDiscovery(ctx context.Context) context.Context {
	return context.WithValue(ctx, receiptDiscoveryKey{}, nil)
}

func receiptDiscoveryFrom(ctx context.Context) (receiptDiscovery, bool) {
	discovery, found := ctx.Value(receiptDiscoveryKey{}).(receiptDiscovery)
	return discovery, found
}
