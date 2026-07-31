package escrow

import (
	"context"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/store"
	"devshard/signing"
)

// Satisfied by *chain.TxClient.
type escrowTxClient interface {
	CreateEscrow(ctx context.Context, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(txHash string) error) (chain.CreateEscrowResult, error)
	SettleEscrow(ctx context.Context, signer *signing.Secp256k1Signer, input chain.SettlementInput) (chain.SettleEscrowResult, error)
	GetTxEscrowID(ctx context.Context, txHash string) (escrowID uint64, found bool, err error)
	GetEscrow(ctx context.Context, escrowID string) (chain.EscrowInfo, bool, error)
}

// Satisfied by *store.Store.
type escrowStore interface {
	ListDevshards(ctx context.Context) ([]store.DevshardRecord, error)
	UpsertDevshard(ctx context.Context, record store.DevshardRecord) error
	SetDevshardActive(ctx context.Context, escrowID string, active bool) error
	SetDevshardSettlementPending(ctx context.Context, escrowID string, pending bool) error
	DeleteDevshard(ctx context.Context, escrowID string) error
	SaveCommitment(ctx context.Context, c store.Commitment) error
	LoadCommitments(ctx context.Context) ([]store.Commitment, error)
	DeleteCommitment(ctx context.Context, txHash string) error
	SaveRotationStatus(ctx context.Context, s store.RotationStatus) error
	WithRetry(ctx context.Context, fn func() error) error
}

// Satisfied by *chain.PhaseObserver.
type snapshotSource interface {
	Snapshot() chain.PhaseSnapshot
}

// SignerSource resolves the signer holding the key named by a PrivateKeyEnv.
type SignerSource interface {
	SignerFor(privateKeyEnv string) (*signing.Secp256k1Signer, error)
}

// api/ wires this to the live engine runtime; escrow only consumes it. Retire is what makes IsBusy
// monotone, so an idle answer stays true until the settlement it gates is broadcast.
type SettlementSource interface {
	Retire(escrowID string) error // synchronous: no nonce can be committed on the escrow after it returns
	IsBusy(escrowID string) bool
	Finalize(ctx context.Context, escrowID string) error // idempotent: no-op if already finalized
	BuildSettlement(ctx context.Context, escrowID string) (chain.SettlementInput, error)
}
