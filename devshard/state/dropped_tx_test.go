package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"devshard/internal/testutil"
	"devshard/logging"
	"devshard/signing"
	"devshard/types"
)

// captureLogger keeps what a run logged, so a silent drop can be told from a reported one.
type captureLogger struct {
	warnings []string
	debugs   []string
}

func (c *captureLogger) Info(string, ...any)        {}
func (c *captureLogger) Error(string, ...any)       {}
func (c *captureLogger) Warn(msg string, _ ...any)  { c.warnings = append(c.warnings, msg) }
func (c *captureLogger) Debug(msg string, _ ...any) { c.debugs = append(c.debugs, msg) }

func captureLogs(t *testing.T) *captureLogger {
	t.Helper()
	capture := &captureLogger{}
	logging.SetLogger(capture)
	t.Cleanup(func() { logging.SetLogger(&slogRestore{}) })
	return capture
}

type slogRestore struct{}

func (slogRestore) Info(string, ...any)  {}
func (slogRestore) Error(string, ...any) {}
func (slogRestore) Warn(string, ...any)  {}
func (slogRestore) Debug(string, ...any) {}

// A confirm-start is queued once per inference. Dropped without a word, it leaves the record pending
// for good, and the timeout round that follows is one every verifier rejects — with nothing anywhere
// saying why.
func TestADroppedConfirmStartIsReported(t *testing.T) {
	hosts := []*signing.Secp256k1Signer{testutil.MustGenerateKey(t), testutil.MustGenerateKey(t)}
	sm, _ := newTestSM(t, hosts, 10000)
	capture := captureLogs(t)

	// The inference was never started, so its confirmation cannot apply.
	_, applied, err := sm.ApplyLocalBestEffort(1, []*types.DevshardTx{
		txStart(&types.MsgStartInference{
			InferenceId: 1, PromptHash: []byte("prompt"), Model: "llama",
			InputLength: 100, MaxTokens: testutil.TestMaxTokens, StartedAt: 1000,
		}),
		{Tx: &types.DevshardTx_ConfirmStart{ConfirmStart: &types.MsgConfirmStart{
			InferenceId: 99, ExecutorSig: []byte("not-a-signature"), ConfirmedAt: 1000,
		}}},
	})

	require.NoError(t, err, "a confirmation is not mandatory, so the diff still composes")
	require.Len(t, applied, 1, "only the start applied")
	require.Contains(t, capture.warnings, "dropped confirm start",
		"a dropped confirmation must leave a trace: without one the pending record has no explanation")
}
