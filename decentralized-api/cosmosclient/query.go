package cosmosclient

import (
	"context"
	"decentralized-api/logging"
	"decentralized-api/observability"
	"fmt"

	rpcclient "github.com/cometbft/cometbft/rpc/client"
	"github.com/cometbft/cometbft/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	"github.com/productscience/inference/x/inference/types"
)

// QueryByKeyWithOptions Query any stored value by key, e.g.:
// storeKey: "inference",
// dataKey: "ActiveParticipants/value/"
func QueryByKeyWithOptions(rpcClient *http.HTTP, storeKey string, dataKey []byte, blockHeight int64, withProof bool) (result *coretypes.ResultABCIQuery, err error) {
	logging.Info("Querying store", types.System, "storeKey", storeKey, "dataKey", dataKey)

	path := fmt.Sprintf("store/%s/key", storeKey)
	queryCtx, queryOp := observability.Chain.StartStoreQuery(context.Background(), storeKey, withProof, blockHeight)
	defer queryOp.FinishErr(&err)

	result, err = rpcClient.ABCIQueryWithOptions(queryCtx, path, dataKey, rpcclient.ABCIQueryOptions{Height: blockHeight, Prove: withProof})
	return result, err
}

func QueryByKey(rpcClient *http.HTTP, storeKey string, dataKey []byte) (result *coretypes.ResultABCIQuery, err error) {
	logging.Info("Querying store", types.System, "storeKey", storeKey, "dataKey", dataKey)

	path := fmt.Sprintf("store/%s/key", storeKey)
	queryCtx, queryOp := observability.Chain.StartStoreQuery(context.Background(), storeKey, false, 0)
	defer queryOp.FinishErr(&err)

	result, err = rpcClient.ABCIQuery(queryCtx, path, dataKey)
	return result, err
}
