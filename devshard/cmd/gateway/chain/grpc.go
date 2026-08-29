package chain

import (
	"context"
	"fmt"
	"strings"

	commonchain "common/chain"
	cmtservice "github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	cryptocodec "github.com/cosmos/cosmos-sdk/crypto/codec"
	sdk "github.com/cosmos/cosmos-sdk/types"
	txtypes "github.com/cosmos/cosmos-sdk/types/tx"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	inferencetypes "github.com/productscience/inference/x/inference/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reader is the chain state the observer polls, an interface so tests answer it without a connection.
type Reader interface {
	MaxNonce(ctx context.Context) (uint64, bool, error)
	PreservedNodes(ctx context.Context) (*PreservedNodes, bool, error)
}

// Transport is the chain access a transaction needs: the reads a signed body carries, the broadcast, and the query that says whether it executed.
type Transport interface {
	ChainID(ctx context.Context) (string, error)
	Account(ctx context.Context, address string) (Account, error)
	Broadcast(ctx context.Context, txBytes []byte) (string, error)
	Tx(ctx context.Context, txHash string) (TxResult, bool, error)
	Escrow(ctx context.Context, escrowID uint64) (EscrowInfo, bool, error)
}

// Account is the pair a signed transaction needs; an unordered tx signs the sequence as zero, so only the number reaches the wire.
type Account struct {
	Number   uint64
	Sequence uint64
}

// TxResult is a committed transaction as the gateway reads it: the code that says whether it executed, and the events an escrow id is recovered from.
type TxResult struct {
	Code      uint32
	Codespace string
	RawLog    string
	Events    []TxEvent
}

type TxEvent struct {
	Type       string
	Attributes []TxAttribute
}

type TxAttribute struct {
	Key   string
	Value string
}

// PreservedNodes is the PoC preserved set, keyed the way routing reads it.
type PreservedNodes struct {
	EpisodeAnchorHeight int64
	Models              []PreservedModel
}

type PreservedModel struct {
	ModelID      string
	Participants []PreservedParticipant
}

type PreservedParticipant struct {
	ParticipantID string
	NodeIDs       []string
}

// GRPCChain answers both interfaces over the one connection the gateway dials. See README.md, "The gRPC transport".
type GRPCChain struct {
	client   *commonchain.Client
	registry codectypes.InterfaceRegistry
	chainID  string
}

func NewGRPCChain(client *commonchain.Client, chainID string) *GRPCChain {
	registry := codectypes.NewInterfaceRegistry()
	cryptocodec.RegisterInterfaces(registry)
	authtypes.RegisterInterfaces(registry)
	return &GRPCChain{client: client, registry: registry, chainID: strings.TrimSpace(chainID)}
}

func (g *GRPCChain) conn() grpc.ClientConnInterface { return g.client.Conn() }

// ChainID asks the node unless a value was configured; a mismatch invalidates every signature, so the configured value wins.
func (g *GRPCChain) ChainID(ctx context.Context) (string, error) {
	if g.chainID != "" {
		return g.chainID, nil
	}
	nodeInfo, err := g.client.CometServiceClient().GetNodeInfo(ctx, &cmtservice.GetNodeInfoRequest{})
	if err != nil {
		return "", fmt.Errorf("fetch chain id: %w", err)
	}
	network := strings.TrimSpace(nodeInfo.GetDefaultNodeInfo().GetNetwork())
	if network == "" {
		return "", fmt.Errorf("chain id not reported by node")
	}
	return network, nil
}

// Account reads through the account's own registered type, so a vesting or module account answers correctly.
func (g *GRPCChain) Account(ctx context.Context, address string) (Account, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return Account{}, fmt.Errorf("address is required")
	}
	response, err := authtypes.NewQueryClient(g.conn()).Account(ctx, &authtypes.QueryAccountRequest{Address: address})
	if err != nil {
		return Account{}, fmt.Errorf("fetch account %s: %w", address, err)
	}
	var account sdk.AccountI
	if err := g.registry.UnpackAny(response.GetAccount(), &account); err != nil {
		return Account{}, fmt.Errorf("unpack account %s: %w", address, err)
	}
	return Account{Number: account.GetAccountNumber(), Sequence: account.GetSequence()}, nil
}

func (g *GRPCChain) Broadcast(ctx context.Context, txBytes []byte) (string, error) {
	response, err := txtypes.NewServiceClient(g.conn()).BroadcastTx(ctx, &txtypes.BroadcastTxRequest{
		TxBytes: txBytes,
		Mode:    txtypes.BroadcastMode_BROADCAST_MODE_SYNC,
	})
	if err != nil {
		return "", fmt.Errorf("broadcast tx: %w", err)
	}
	result := response.GetTxResponse()
	if result == nil {
		return "", fmt.Errorf("broadcast response carried no result")
	}
	if result.Code != 0 {
		return "", fmt.Errorf("broadcast tx failed code=%d codespace=%s raw_log=%s",
			result.Code, result.Codespace, result.RawLog)
	}
	txHash := strings.TrimSpace(result.TxHash)
	if txHash == "" {
		return "", fmt.Errorf("broadcast response missing txhash")
	}
	return txHash, nil
}

// Tx reports found=false for a transaction the node has not indexed yet, which a poller must tell apart from a failure.
func (g *GRPCChain) Tx(ctx context.Context, txHash string) (TxResult, bool, error) {
	response, err := txtypes.NewServiceClient(g.conn()).GetTx(ctx, &txtypes.GetTxRequest{Hash: txHash})
	if err != nil {
		if isNotFoundStatus(err) {
			return TxResult{}, false, nil
		}
		return TxResult{}, false, fmt.Errorf("fetch tx %s: %w", txHash, err)
	}
	result := response.GetTxResponse()
	if result == nil {
		return TxResult{}, false, nil
	}
	converted := TxResult{Code: result.Code, Codespace: result.Codespace, RawLog: result.RawLog}
	for _, event := range result.Events {
		attributes := make([]TxAttribute, 0, len(event.Attributes))
		for _, attribute := range event.Attributes {
			attributes = append(attributes, TxAttribute{Key: attribute.Key, Value: attribute.Value})
		}
		converted.Events = append(converted.Events, TxEvent{Type: event.Type, Attributes: attributes})
	}
	return converted, true, nil
}

// Escrow reports found=false only when the chain says the escrow is absent; a transport failure is an error.
func (g *GRPCChain) Escrow(ctx context.Context, escrowID uint64) (EscrowInfo, bool, error) {
	response, err := g.client.InferenceQueryClient().DevshardEscrow(ctx, &inferencetypes.QueryGetDevshardEscrowRequest{Id: escrowID})
	if err != nil {
		return EscrowInfo{}, false, fmt.Errorf("fetch escrow %d: %w", escrowID, err)
	}
	if !response.GetFound() || response.GetEscrow() == nil {
		return EscrowInfo{}, false, nil
	}
	return EscrowInfo{
		EscrowID: fmt.Sprintf("%d", escrowID),
		Balance:  response.GetEscrow().GetAmount(),
	}, true, nil
}

// MaxNonce reports fetched=false when the chain carries no devshard escrow params -- not enabled, rather than unread.
func (g *GRPCChain) MaxNonce(ctx context.Context) (uint64, bool, error) {
	response, err := g.client.InferenceQueryClient().Params(ctx, &inferencetypes.QueryParamsRequest{})
	if err != nil {
		return 0, false, fmt.Errorf("fetch devshard escrow params: %w", err)
	}
	params := response.GetParams().DevshardEscrowParams
	if params == nil {
		return 0, false, nil
	}
	return uint64(params.GetMaxNonce()), true, nil
}

// PreservedNodes reports found=false when the chain holds no snapshot for the current episode, which routing reads as "no preserved set".
func (g *GRPCChain) PreservedNodes(ctx context.Context) (*PreservedNodes, bool, error) {
	response, err := g.client.InferenceQueryClient().PreservedNodesSnapshot(ctx, &inferencetypes.QueryPreservedNodesSnapshotRequest{})
	if err != nil {
		return nil, false, fmt.Errorf("fetch preserved nodes snapshot: %w", err)
	}
	snapshot := response.GetSnapshot()
	if !response.GetFound() || snapshot == nil {
		return nil, false, nil
	}
	converted := &PreservedNodes{EpisodeAnchorHeight: snapshot.GetEpisodeAnchorHeight()}
	for _, modelNodes := range snapshot.GetModelPreservedNodes() {
		model := PreservedModel{ModelID: modelNodes.GetModelId()}
		for _, participant := range modelNodes.GetParticipants() {
			model.Participants = append(model.Participants, PreservedParticipant{
				ParticipantID: participant.GetParticipantId(),
				NodeIDs:       append([]string(nil), participant.GetNodeIds()...),
			})
		}
		converted.Models = append(converted.Models, model)
	}
	return converted, true, nil
}

// isNotFoundStatus also matches the message text, for nodes that answer Unknown instead of the typed code.
func isNotFoundStatus(err error) bool {
	if status.Code(err) == codes.NotFound {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}
