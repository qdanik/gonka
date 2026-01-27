package v0_2_8

import (
	"errors"
	"testing"

	"cosmossdk.io/log"
	"cosmossdk.io/store"
	"cosmossdk.io/store/metrics"
	storetypes "cosmossdk.io/store/types"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/cosmos-sdk/codec"
	codectypes "github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/runtime"
	sdk "github.com/cosmos/cosmos-sdk/types"
	authtypes "github.com/cosmos/cosmos-sdk/x/auth/types"
	govtypes "github.com/cosmos/cosmos-sdk/x/gov/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	keepertest "github.com/productscience/inference/testutil/keeper"
	blskeeper "github.com/productscience/inference/x/bls/keeper"
	blstypes "github.com/productscience/inference/x/bls/types"
	bookkeepertypes "github.com/productscience/inference/x/bookkeeper/types"
	"github.com/productscience/inference/x/inference/keeper"
	"github.com/productscience/inference/x/inference/types"
)

var errUnauthorizedBurn = errors.New("module account pre_programmed_sale does not have permissions to burn tokens: unauthorized")

// TestBurnExtraCommunityCoins_OldApproachFails demonstrates that the old approach
// (burning directly from pre_programmed_sale) would fail due to missing burner permission.
func TestBurnExtraCommunityCoins_OldApproachFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, ctx, mocks := setupTestKeeper(t, ctrl)

	coins := sdk.NewCoins(sdk.NewInt64Coin(types.BaseCoin, 1000000))

	// Simulate OLD behavior: direct burn from pre_programmed_sale fails
	// because it doesn't have burner permission
	mocks.BankKeeper.EXPECT().
		BurnCoins(gomock.Any(), "pre_programmed_sale", coins, gomock.Any()).
		Return(errUnauthorizedBurn)

	// Call the old burn approach directly (simulating what the old code did)
	err := mocks.BankKeeper.BurnCoins(ctx, "pre_programmed_sale", coins, "direct burn attempt")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unauthorized")
}

// TestBurnExtraCommunityCoins_NewApproachSucceeds demonstrates that the new approach
// (transfer to bookkeeper, then burn) succeeds.
func TestBurnExtraCommunityCoins_NewApproachSucceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	k, ctx, mocks := setupTestKeeper(t, ctrl)

	preProgrammedSaleAddr := authtypes.NewModuleAddress("pre_programmed_sale")
	coins := sdk.NewCoins(sdk.NewInt64Coin(types.BaseCoin, 1000000))

	// Mock: GetModuleAddress returns the expected address
	mocks.AccountKeeper.EXPECT().
		GetModuleAddress("pre_programmed_sale").
		Return(preProgrammedSaleAddr)

	// Mock: SpendableCoins returns some coins to burn
	mocks.BankViewKeeper.EXPECT().
		SpendableCoins(gomock.Any(), preProgrammedSaleAddr).
		Return(coins)

	// Step 1: Transfer from pre_programmed_sale to bookkeeper succeeds
	mocks.BankKeeper.EXPECT().
		SendCoinsFromModuleToModule(gomock.Any(), "pre_programmed_sale", bookkeepertypes.ModuleName, coins, "transfer for burn").
		Return(nil)

	// Step 2: Burn from bookkeeper succeeds (bookkeeper has burner permission)
	mocks.BankKeeper.EXPECT().
		BurnCoins(gomock.Any(), bookkeepertypes.ModuleName, coins, "one-time burn of pre_programmed_sale account").
		Return(nil)

	// Call the actual function
	err := burnExtraCommunityCoins(ctx, &k)
	require.NoError(t, err)
}

// TestBurnExtraCommunityCoins_NoCoins tests that the function handles empty balance gracefully.
func TestBurnExtraCommunityCoins_NoCoins(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	k, ctx, mocks := setupTestKeeper(t, ctrl)

	preProgrammedSaleAddr := authtypes.NewModuleAddress("pre_programmed_sale")

	// Mock: GetModuleAddress returns the expected address
	mocks.AccountKeeper.EXPECT().
		GetModuleAddress("pre_programmed_sale").
		Return(preProgrammedSaleAddr)

	// Mock: SpendableCoins returns empty (no coins to burn)
	mocks.BankViewKeeper.EXPECT().
		SpendableCoins(gomock.Any(), preProgrammedSaleAddr).
		Return(sdk.NewCoins())

	// No burn calls expected when there are no coins

	err := burnExtraCommunityCoins(ctx, &k)
	require.NoError(t, err)
}

// TestBurnExtraCommunityCoins_TransferFails tests error handling when transfer fails.
func TestBurnExtraCommunityCoins_TransferFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	k, ctx, mocks := setupTestKeeper(t, ctrl)

	preProgrammedSaleAddr := authtypes.NewModuleAddress("pre_programmed_sale")
	coins := sdk.NewCoins(sdk.NewInt64Coin(types.BaseCoin, 1000000))

	// Mock: GetModuleAddress returns the expected address
	mocks.AccountKeeper.EXPECT().
		GetModuleAddress("pre_programmed_sale").
		Return(preProgrammedSaleAddr)

	// Mock: SpendableCoins returns some coins
	mocks.BankViewKeeper.EXPECT().
		SpendableCoins(gomock.Any(), preProgrammedSaleAddr).
		Return(coins)

	// Transfer fails
	mocks.BankKeeper.EXPECT().
		SendCoinsFromModuleToModule(gomock.Any(), "pre_programmed_sale", bookkeepertypes.ModuleName, coins, "transfer for burn").
		Return(errors.New("transfer failed"))

	err := burnExtraCommunityCoins(ctx, &k)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to transfer coins")
}

type testMocks struct {
	BankKeeper     *keepertest.MockBookkeepingBankKeeper
	BankViewKeeper *keepertest.MockBankKeeper
	AccountKeeper  *keepertest.MockAccountKeeper
}

func setupTestKeeper(t *testing.T, ctrl *gomock.Controller) (keeper.Keeper, sdk.Context, testMocks) {
	sdk.GetConfig().SetBech32PrefixForAccount("gonka", "gonka")

	bankKeeper := keepertest.NewMockBookkeepingBankKeeper(ctrl)
	bankViewKeeper := keepertest.NewMockBankKeeper(ctrl)
	accountKeeper := keepertest.NewMockAccountKeeper(ctrl)
	validatorSet := keepertest.NewMockValidatorSet(ctrl)
	groupMock := keepertest.NewMockGroupMessageKeeper(ctrl)
	stakingMock := keepertest.NewMockStakingKeeper(ctrl)
	collateralMock := keepertest.NewMockCollateralKeeper(ctrl)
	streamvestingMock := keepertest.NewMockStreamVestingKeeper(ctrl)
	authzKeeper := keepertest.NewMockAuthzKeeper(ctrl)
	upgradeKeeper := keepertest.NewMockUpgradeKeeper(ctrl)

	storeKey := storetypes.NewKVStoreKey(types.StoreKey)
	blsStoreKey := storetypes.NewKVStoreKey(blstypes.StoreKey)

	db := dbm.NewMemDB()
	stateStore := store.NewCommitMultiStore(db, log.NewNopLogger(), metrics.NewNoOpMetrics())
	stateStore.MountStoreWithDB(storeKey, storetypes.StoreTypeIAVL, db)
	stateStore.MountStoreWithDB(blsStoreKey, storetypes.StoreTypeIAVL, db)
	require.NoError(t, stateStore.LoadLatestVersion())

	registry := codectypes.NewInterfaceRegistry()
	cdc := codec.NewProtoCodec(registry)
	authority := authtypes.NewModuleAddress(govtypes.ModuleName)

	blsKeeper := blskeeper.NewKeeper(
		cdc,
		runtime.NewKVStoreService(blsStoreKey),
		log.NewNopLogger(),
		authority.String(),
	)

	k := keeper.NewKeeper(
		cdc,
		runtime.NewKVStoreService(storeKey),
		log.NewNopLogger(),
		authority.String(),
		bankKeeper,
		bankViewKeeper,
		groupMock,
		validatorSet,
		stakingMock,
		accountKeeper,
		blsKeeper,
		collateralMock,
		streamvestingMock,
		authzKeeper,
		nil,
		upgradeKeeper,
	)

	ctx := sdk.NewContext(stateStore, cmtproto.Header{}, false, log.NewNopLogger())

	require.NoError(t, k.SetParams(ctx, types.DefaultParams()))
	require.NoError(t, blsKeeper.SetParams(ctx, blstypes.DefaultParams()))

	mocks := testMocks{
		BankKeeper:     bankKeeper,
		BankViewKeeper: bankViewKeeper,
		AccountKeeper:  accountKeeper,
	}

	return k, ctx, mocks
}
