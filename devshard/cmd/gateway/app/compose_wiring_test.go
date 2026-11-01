package app

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	sdktx "github.com/cosmos/cosmos-sdk/types/tx"

	"devshard/cmd/gateway/chain"
	"devshard/cmd/gateway/config"
	"devshard/cmd/gateway/env"
	"devshard/cmd/gateway/limits"
	"devshard/cmd/gateway/perf"
	"devshard/cmd/gateway/store"
	"devshard/transport"
	"devshard/user"
)

const (
	registryWriteFeeAmount = uint64(611_111)
	registryWriteGasLimit  = uint64(322_222)
	sharedFeeAmount        = uint64(933_333)
	sharedGasLimit         = uint64(844_444)
	sharedFeeDenom         = "ufee"
	registryWriteHeight    = int64(1)
	registryWriteAccount   = uint64(7)
	registryWriteDeadline  = 30 * time.Second
	identityKeyVariable    = "REGISTRY_WRITE_IDENTITY_KEY"
	decimalBase            = 10
)

type registryWriteTransport struct {
	chainWithoutADial
	broadcasts chan []byte
}

func (*registryWriteTransport) ChainID(context.Context) (string, error) {
	return "registry-write-chain", nil
}

func (*registryWriteTransport) Account(context.Context, string) (chain.Account, error) {
	return chain.Account{Number: registryWriteAccount}, nil
}

func (r *registryWriteTransport) Broadcast(_ context.Context, tx []byte) (string, error) {
	r.broadcasts <- tx
	return "", errNoChainDialed
}

func decodedFee(t *testing.T, tx []byte) *sdktx.Fee {
	t.Helper()
	var raw sdktx.TxRaw
	if err := raw.Unmarshal(tx); err != nil {
		t.Fatalf("decoding tx: %v", err)
	}
	var authInfo sdktx.AuthInfo
	if err := authInfo.Unmarshal(raw.AuthInfoBytes); err != nil {
		t.Fatalf("decoding auth info: %v", err)
	}
	return authInfo.Fee
}

// Test flow:
//  1. Build the host capability lookup the nonce ledger reads, over a real host tracker.
//  2. Record two output overruns for one participant and model on the tracker.
//  3. Assert the capability the ledger reads for that participant and model carries both overruns.
func TestTheLedgersHostCapabilityCarriesTheTrackersOutputOverruns(t *testing.T) {
	configuration := config.Defaults()
	hosts := perf.NewTracker(config.NewHolder(&configuration), fixedClock)
	participants := limits.NewParticipantLimiter(limits.ParticipantConfigFromLimits(configuration.Limits), fixedClock)
	lookup := hostCapabilities(hosts, participants, limits.NewCapacity(participants.Available))

	hosts.RecordOutputOverrun("validator-a", "model-a")
	hosts.RecordOutputOverrun("validator-a", "model-a")

	if got := lookup("validator-a", "model-a").OutputOverruns; got != 2 {
		t.Fatalf("capability output overruns = %d, want 2", got)
	}
}

// Test flow:
//  1. Compose a gateway and take the context its HTTP server hands each request.
//  2. Call the request-ending func the shutdown steps receive.
//  3. Assert the request context is cancelled, so a stream still open after the graceful share ends.
func TestTheShutdownStepsEndTheRequestsTheServerStarted(t *testing.T) {
	gatewayEnvironment(t)
	composed := composedGateway(t)
	requestCtx := composed.server.BaseContext(nil)
	endRequests := composed.partsToStop().endRequests
	if endRequests == nil {
		t.Fatal("the shutdown steps carry no request-ending func")
	}

	endRequests()

	if !errors.Is(requestCtx.Err(), context.Canceled) {
		t.Fatalf("request context error = %v, want context.Canceled", requestCtx.Err())
	}
}

// Test flow:
//  1. Set the registry fee amount and gas limit and the shared ones in the environment, all four distinct, and switch the peer channel on.
//  2. Compose a gateway over a transport that captures what it is asked to broadcast, and start its peer channel with the signer on the chain's allowlist.
//  3. Assert the signed registry write carries the registry fee amount and gas limit, in the shared denom.
func TestTheRegistrySettingsReachTheSignedRegistryWrite(t *testing.T) {
	gatewayEnvironment(t)
	signer, err := fixedSigners{}.SignerFor(identityKeyVariable)
	if err != nil {
		t.Fatalf("building signer: %v", err)
	}
	for name, value := range map[string]string{
		"DEVSHARD_REGISTRY_TX_FEE_AMOUNT": strconv.FormatUint(registryWriteFeeAmount, decimalBase),
		"DEVSHARD_REGISTRY_TX_GAS_LIMIT":  strconv.FormatUint(registryWriteGasLimit, decimalBase),
		"DEVSHARD_TX_FEE_AMOUNT":          strconv.FormatUint(sharedFeeAmount, decimalBase),
		"DEVSHARD_TX_GAS_LIMIT":           strconv.FormatUint(sharedGasLimit, decimalBase),
		"DEVSHARD_TX_FEE_DENOM":           sharedFeeDenom,
		"DEVSHARD_PEER_ENABLED":           "true",
		"DEVSHARD_PEER_REGISTRY_CONTRACT": "gonka1contract",
		"DEVSHARD_PEER_INSTANCE_ID":       "instance-a",
		"DEVSHARD_PEER_LISTEN":            "127.0.0.1:0",
		"DEVSHARD_PEER_ADVERTISE_URL":     "https://peer.example:8443",
		"DEVSHARD_PEER_IDENTITY_KEY_ENV":  identityKeyVariable,
		identityKeyVariable:               signer.PrivateKeyHex(),
	} {
		t.Setenv(name, value)
	}
	values, err := env.Load()
	if err != nil {
		t.Fatalf("loading environment: %v", err)
	}
	storageDir, err := resolveStorageDir(values.StorageDir)
	if err != nil {
		t.Fatalf("resolving storage dir: %v", err)
	}
	gatewayStore, err := store.Open(storageDir)
	if err != nil {
		t.Fatalf("opening store: %v", err)
	}
	writes := &registryWriteTransport{broadcasts: make(chan []byte, 1)}
	sessions := sessionsReading(gatewayStore, storageDir, chainWithoutADial{})
	sources := func(endpoints config.Chain, heightSettings config.HeightSync, routePrefix string, adoption *transport.PeerRPCAdoption, steps user.StepTimer) (chainSources, error) {
		dialed, err := sessions(endpoints, heightSettings, routePrefix, adoption, steps)
		dialed.Transport = writes
		dialed.Registry = countingRegistry{reads: new(int)}
		return dialed, err
	}
	composed, err := compose(t.Context(), values, storageDir, gatewayStore, sources)
	if err != nil {
		gatewayStore.Close()
		t.Fatalf("compose(): %v", err)
	}
	t.Cleanup(func() {
		if err := composed.shutdown(shutdownGracePeriod); err != nil {
			t.Errorf("shutdown(): %v", err)
		}
	})

	composed.peers.ObserveChain([]string{signer.Address()}, registryWriteHeight)
	if err := composed.peers.Start(t.Context()); err != nil {
		t.Fatalf("starting the peer channel: %v", err)
	}

	select {
	case tx := <-writes.broadcasts:
		fee := decodedFee(t, tx)
		if got := fee.Amount[0]; got.Denom != sharedFeeDenom || got.Amount.Uint64() != registryWriteFeeAmount {
			t.Fatalf("registry write fee = %s, want %d%s", got, registryWriteFeeAmount, sharedFeeDenom)
		}
		if fee.GasLimit != registryWriteGasLimit {
			t.Fatalf("registry write gas limit = %d, want %d", fee.GasLimit, registryWriteGasLimit)
		}
	case <-time.After(registryWriteDeadline):
		t.Fatal("the peer channel broadcast no registry write")
	}
}
