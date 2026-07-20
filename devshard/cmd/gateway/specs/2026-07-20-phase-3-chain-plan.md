# Gateway Phase 3: chain — TxClient + PhaseObserver — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** The `cmd/gateway/chain` package — two independent components: **TxClient** (build/sign/broadcast/query gonka escrow transactions) and **PhaseObserver** (poll epoch/participant/version state, publish an immutable snapshot, subscribers register instead of being push-poked).

**Architecture:** Phase 3 of 9 (design spec §10). Two tracks with disjoint files, runnable in parallel:
- **Track A — TxClient:** hand-rolled Cosmos protobuf (no generated types exist for the `inference.inference` escrow Msgs), unordered-tx with 9-min TTL, precompute-hash → intent callback → broadcast, three-way tx-query semantics across fallback endpoints. Byte-exact encoding is load-bearing (a wrong field number = the chain rejects the tx), so the encoders are pinned by goldens generated from the old package.
- **Track B — PhaseObserver:** polls public-API read endpoints, derives an immutable `PhaseSnapshot`, publishes via an atomic holder + subscribe. The old code pushed into 11 downstream sinks from inside `refresh()`; this inverts to: the snapshot **carries the raw inputs** (phase, per-host + per-model weight views, preserved sets, versions capability, block-derivation), and subscribers derive what they need.

**Tech Stack:** Go 1.24.2, module `devshard`. Consumes `config.Config.{Chain,Tx}` (Phase 1) and `devshard/signing` (`SignerFromHex`, `Secp256k1Signer`). No other `devshard/*` import (NOT bridge, NOT proto, NOT types-for-encoding). std `net/http`, `crypto/sha256`, `encoding/base64`.

## Global Constraints

- All prior Global Constraints hold: naming, error style (`fmt.Errorf("verb: %w", err)`, lowercase, sentinels), `-race -count=1`, gofmt/vet clean before each checkpoint, no `os.Getenv` outside `env/`, no mutable package-level state, terse technical comments (no task/phase/old-file references — the §G sweep applies at phase end).
- **Config-sourced, not env-sourced:** TxClient and Observer receive a `config.Config` snapshot (or the sub-structs); they never read env. Fee denom + poll cadence are config constants; only `GATEWAY_TX_FEE_AMOUNT`/`GATEWAY_TX_GAS_LIMIT` came from env (already in Phase 1 config).
- **Byte-exact tx encoding (Track A):** the protobuf encoders must reproduce the old encoders byte-for-byte, pinned by goldens generated from the old package via a build-tagged generator (same oracle pattern as Phase 2). A wrong byte = a rejected/invalid tx on a live chain we cannot test against, so goldens are the merge bar for Track A encoding.
- **No live node:** every test is hermetic — `httptest.Server` stubs for Cosmos REST (broadcast/query) and public-API (epochs/participants/preserved/versions); injectable `Clock`/`now func() time.Time` for TTL and unordered-tx timeout; no real network, no real time.
- **Observer stays pure (design decision — flag for veto):** PhaseObserver observes and publishes; it does NOT compute capacity scale factor or speculative-attempt policy (those were downstream in the old push fan-out and belong to limits/engine in later phases). The snapshot carries raw weight views + preserved sets + phase + versions capability; subscribers (limits/scheduler/engine, Phases 5/7/8) derive scale, admission, and policy. This keeps the observer a single-responsibility component.
- **Commits are Daniil's:** executor skips commit steps; single user commit at phase end.

## File Structure

```
cmd/gateway/chain/
  doc.go             package doc
  protoencode.go     hand-rolled Cosmos protobuf field encoders (pure funcs)
  tx_build.go        buildCreateEscrowTx / buildSettleEscrowTx / signDoc / txHashFromBytes
  txclient.go        TxClient: CreateEscrow / SettleEscrow / GetTxEscrowID + HTTP (broadcast, tx-query, fetchChainID, fetchAccount)
  settlement.go      SettlementInput DTO (creator-supplied settle payload → encoder inputs)
  snapshot.go        PhaseSnapshot type + phase enums + rawPoCBlockingState + Admission predicate
  observer.go        PhaseObserver: poll loop, fetch+parse (epoch/participants/preserved), derive snapshot, atomic publish + Subscribe
  versions.go        VersionsCache: per-node PoC-validation-inference capability, fail-closed, TTL
  *_test.go          per-file unit tests
  testdata/txgold/*  committed golden tx-encoding bytes (base64), generated from the old package
cmd/devshardctl/chain_txgold_test.go   build tag `txgold`; emits the encoding goldens (the ONLY old-package change)
```

Dependency-injection shape (so tests are hermetic): both components take a `*http.Client` and a `now func() time.Time`; the poll loop takes a `context.Context`; no globals.

---

## TRACK A — TxClient

### Task A0: encoding golden generator + package skeleton

**Files:** create `cmd/devshardctl/chain_txgold_test.go` (build tag `txgold`); `cmd/gateway/chain/doc.go`; `cmd/gateway/chain/testdata/txgold/` (generated).

**Interfaces produced:** the golden corpus contract for tx-encoding. Each golden = one fixed input → base64 of the exact bytes the OLD encoder emits.

The generator drives the old encoders with FIXED inputs (a fixed secp256k1 key hex, fixed chainID/account/amount/modelID, a fixed settlement payload, a fixed TTL timestamp injected — the old code uses `time.Now()`, so the generator must call the lower-level `encodeUnorderedTxBody(msgAny, fixedTime)` directly rather than the top-level builder for the TTL-bearing cases, and separately capture the top-level `buildCreateDevshardEscrowTx` output with a monkeypatched/observed time where feasible; where time can't be injected, golden ONLY the deterministic sub-encoders: `encodeMsgCreateDevshardEscrow`, `encodeMsgSettleDevshardEscrow`, `encodeAny`, `encodeAuthInfo`, `encodeSignDoc`, `encodeFee`, `encodeSecp256k1PubKey`, and `txHashFromBytes` over a fixed byte input).

- [ ] Step 1: write the generator that, for a curated set of fixed inputs, calls each old deterministic encoder and writes `testdata/txgold/<name>.b64` (base64 of bytes) under the NEW package's testdata dir. Cover: create-msg (creator/amount/modelID), settle-msg (full: stateRoot/nonce/restHash/hostStats×2/slotSig×2/fees/version), each with representative field values incl. an empty-optional case; `encodeAny` wrapping; `encodeAuthInfo`/`encodeSignerInfo`/`encodeFee` (note fee amount is a STRING field); `encodeSignDoc`; `txHashFromBytes` of a fixed 10-byte input.
- [ ] Step 2: run `go test ./cmd/devshardctl/ -tags txgold -run TestGenerateTxGoldens -count=1`; confirm files written; spot-check one against a manual field-decode.
- [ ] Step 3: write `doc.go` package comment. Confirm old package otherwise untouched (`git status` shows one new build-tagged file).
- [ ] Step 4: checkpoint `test(gateway): chain tx-encoding golden generator + skeleton`.

### Task A1: protobuf field encoders (byte-exact)

**Files:** `chain/protoencode.go`, `chain/protoencode_test.go`.

**Interfaces produced:** the pure field encoders, same field-number→wire mapping as old (`chain_tx_rest.go:607-772`):
```go
func appendVarintField(dst []byte, field int, value uint64) []byte
func appendBytesField(dst []byte, field int, value []byte) []byte
func encodeAny(typeURL string, value []byte) []byte
func encodeSecp256k1PubKey(compressed []byte) []byte
func encodeFee(denom string, amount uint64, gasLimit uint64) []byte   // amount is a STRING sub-field
func encodeSignerInfo(pubKeyAny []byte, sequence uint64) []byte       // modeInfo→single→SIGN_MODE_DIRECT(1)
func encodeAuthInfo(pubKeyAny []byte, sequence uint64, denom string, amount, gasLimit uint64) []byte
func encodeSignDoc(bodyBytes, authInfoBytes []byte, chainID string, accountNumber uint64) []byte
func encodeTxRaw(bodyBytes, authInfoBytes, signature []byte) []byte
func encodeUnorderedTxBody(msgAny []byte, timeout time.Time) []byte   // field1=msg, field4=varint 1, field5=timestamp
func encodeTimestamp(ts time.Time) []byte                            // field1=Unix seconds, field2=nanos if nonzero
func txHashFromBytes(txBytes []byte) string                          // upper(hex(sha256))
```
Type-URL consts: `createEscrowMsgTypeURL="/inference.inference.MsgCreateDevshardEscrow"`, `settleEscrowMsgTypeURL="/inference.inference.MsgSettleDevshardEscrow"`, `secp256k1PubKeyTypeURL="/cosmos.crypto.secp256k1.PubKey"`.

- [ ] Step 1: tests assert each encoder's output == the committed golden for the matching fixed input (load `testdata/txgold/*.b64`, decode, compare bytes). Plus a self-consistency test: decode `encodeMsgCreateDevshardEscrow` output with a minimal hand-rolled field reader and assert field 1=creator, 2=amount, 3=modelID (proves field numbers, not just match-old).
- [ ] Step 2: RED (undefined).
- [ ] Step 3: implement `protoencode.go` porting the old wire logic exactly.
- [ ] Step 4: green; encoders match goldens byte-for-byte.
- [ ] Step 5: checkpoint `feat(gateway): chain protobuf encoders (golden-matched)`.

### Task A2: tx builders + settlement DTO

**Files:** `chain/tx_build.go`, `chain/settlement.go`, tests.

**Interfaces produced:**
```go
type SettlementInput struct {
    EscrowID  uint64
    StateRoot []byte   // already-decoded (caller base64-decodes)
    Nonce     uint64
    RestHash  []byte
    HostStats []SettlementHostStat
    SlotSigs  []SettlementSlotSig
    Fees      uint64
    Version   string
}
type SettlementHostStat struct{ SlotID, Missed, Invalid, Cost, RequiredValidations, CompletedValidations uint64 }
type SettlementSlotSig struct{ SlotID uint64; Signature []byte }
func encodeMsgCreateDevshardEscrow(creator string, amount uint64, modelID string) []byte
func encodeMsgSettleDevshardEscrow(settler string, input SettlementInput) []byte
func buildCreateEscrowTx(signer *signing.Secp256k1Signer, chainID string, accountNumber uint64, feeDenom string, feeAmount, gasLimit, amount uint64, modelID string, ttl time.Time) ([]byte, error)
func buildSettleEscrowTx(signer *signing.Secp256k1Signer, chainID string, accountNumber uint64, feeDenom string, feeAmount, gasLimit uint64, settler string, input SettlementInput, ttl time.Time) ([]byte, error)
```
Build steps (both, per `chain_tx_rest.go:502-533`): msg → `encodeAny(typeURL,msg)` → `encodeUnorderedTxBody(any, ttl)` → pubKeyAny → `encodeAuthInfo(pubKeyAny, 0, denom, feeAmount, gasLimit)` (**sequence hardcoded 0**) → `encodeSignDoc(body, authInfo, chainID, accountNumber)` → `sig=signer.Sign(signDoc)` (require `len>=64`, else `fmt.Errorf("invalid signature length %d", len)`) → `encodeTxRaw(body, authInfo, sig[:64])` (**truncate to 64, drop recovery byte**). Note `ttl` is a parameter (injected — the caller passes `now().Add(unorderedTxTTL)`), NOT `time.Now()` inside.

- [ ] Step 1: tests — the full create-tx and settle-tx built with a FIXED signer key + fixed ttl match a committed golden (add these two whole-tx goldens to A0's generator by calling the old top-level builder with the same fixed key and an injected fixed time — if the old builder can't take injected time, golden the assembled result by composing the sub-encoder goldens and assert structural equality field-by-field instead, documenting which). Settlement DTO round-trips through the encoder producing golden bytes. `sig[:64]` truncation asserted (feed a 65-byte signer, assert 64 consumed). Signature-length error path.
- [ ] Step 2: RED. Step 3: implement. Step 4: green.
- [ ] Step 5: checkpoint `feat(gateway): chain tx builders + settlement encoding`.

### Task A3: TxClient — HTTP broadcast, query three-way, lifecycle

**Files:** `chain/txclient.go`, `chain/txclient_test.go`.

**Interfaces produced:**
```go
type TxClient struct{ /* baseURL, txQueryURLs []string, chainID, feeDenom, feeAmount, gasLimit, pollInterval, pollTimeout, client, now */ }
type Config struct {
    RESTBaseURL string; TxQueryFallbackURLs []string; ChainID string
    FeeDenom string; FeeAmount, GasLimit uint64
    PollInterval, PollTimeout time.Duration
    HTTPClient *http.Client; Now func() time.Time
}
func NewTxClient(cfg Config) (*TxClient, error)                         // errors if RESTBaseURL empty; applies config-const defaults
func (c *TxClient) CreateEscrow(ctx, signer *signing.Secp256k1Signer, amount uint64, modelID string, onPrepared func(txHash string) error) (CreateEscrowResult, error)
func (c *TxClient) SettleEscrow(ctx, signer *signing.Secp256k1Signer, input SettlementInput) (SettleEscrowResult, error)
func (c *TxClient) GetTxEscrowID(ctx, txHash string) (escrowID uint64, found bool, err error)
type CreateEscrowResult struct{ EscrowID uint64; TxHash, Creator string }
type SettleEscrowResult struct{ EscrowID uint64; TxHash, Settler string }
var ErrTxNotFound = errors.New("tx not found on chain")
```
Semantics to preserve exactly:
- **CreateEscrow** (`:160`): resolve chainID (config or `fetchChainID` node_info), `fetchAccount(creator)` for accountNumber, `buildCreateEscrowTx(..., ttl=c.now().Add(unorderedTxTTL))`, `txHash=txHashFromBytes(txBytes)`, **`onPrepared(txHash)` BEFORE broadcast** (wrap failure `"record escrow create intent before broadcast: %w"`, abort), `broadcastTx` (mode SYNC), verify `EqualFold(nodeHash, txHash)` else mismatch error, `waitForCreatedEscrowID(txHash)` parsing the `devshard_escrow_created`/`escrow_id` event.
- **SettleEscrow** (`:217`): build+broadcast, return node hash directly — **no onPrepared, no confirmation wait** (preserve the asymmetry).
- **GetTxEscrowID three-way** (`:131`): iterate `txQueryURLs`; 404 → set `sawNotFound`, continue; other error → record `lastErr`, continue; committed `Code!=0` → `(0,false,nil)` (committed-but-failed = no escrow); found event → `(id,true,nil)`. After loop: `lastErr==nil && sawNotFound` → `(0,false,ErrTxNotFound)`; else `(0,false,lastErr)`.
- `broadcastTx`: POST `/cosmos/tx/v1beta1/txs` `{"tx_bytes":<b64>,"mode":"BROADCAST_MODE_SYNC"}`; `Code!=0` → `"broadcast tx failed code=%d codespace=%s raw_log=%s"`.
- `waitForCreatedEscrowID`: poll GET `/cosmos/tx/v1beta1/txs/{escaped}` across query URLs until `pollTimeout`, `select` on `ctx.Done()`/`time.After(pollInterval)`; parse event in `.Events` then `.Logs[].Events`.
- `fetchChainID`: GET `/cosmos/base/tendermint/v1beta1/node_info` → `default_node_info.network`. `fetchAccount`: GET `/cosmos/auth/v1beta1/accounts/{addr}` → account_number/sequence.
- `txQueryURLs`: `[RESTBaseURL] ∪ TxQueryFallbackURLs` deduped (RESTBaseURL first).

- [ ] Step 1: tests against `httptest.Server` stubs. CreateEscrow happy path: stub node_info+account+broadcast+tx-query(with event) → assert EscrowID + onPrepared-called-before-broadcast (record call order via a channel/flag the stub sets on broadcast). onPrepared-error aborts before broadcast (stub asserts broadcast never hit). Hash-mismatch path. GetTxEscrowID: table over {all-404→ErrTxNotFound, code!=0→(0,false,nil), found→(id,true,nil), one-404-one-transient→lastErr}. Broadcast Code!=0 error text. Injected `now` + short pollTimeout for the wait-timeout path (no real sleeps — small durations + fake clock).
- [ ] Step 2: RED. Step 3: implement. Step 4: green under `-race`.
- [ ] Step 5: checkpoint `feat(gateway): chain TxClient — broadcast, three-way tx query, intent-before-broadcast`.

---

## TRACK B — PhaseObserver

### Task B1: PhaseSnapshot + phase derivation

**Files:** `chain/snapshot.go`, `chain/snapshot_test.go`.

**Interfaces produced:**
```go
type PhaseSnapshot struct {
    BlockHeight          int64
    EpochIndex           uint64
    EpochPhase           string
    ConfirmationPoCPhase string
    RequestsBlocked      bool
    BlockReason          string   // "" | "poc" | "confirmation_poc"
    // raw inputs folded in from the old push fan-out:
    CurrentWeights       map[string]float64            // participant addr → weight (validation-merged during PoC)
    FullWeights          map[string]float64            // steady-state baseline
    CurrentWeightsByModel map[string]map[string]float64
    FullWeightsByModel    map[string]map[string]float64
    Preserved            []string                      // PoC-preserved participant addrs; nil = "not loaded, treat all preserved"
    PreservedByModel     map[string][]string
    InferenceURLs        map[string]string             // participant addr → dapi base url
    LastUpdatedAt        time.Time
    LastError            string
}
const ( EpochPhaseInference="Inference"; EpochPhasePoCGenerate="PoCGenerate"; EpochPhasePoCGenerateWindDown="PoCGenerateWindDown"; EpochPhasePoCValidate="PoCValidate"; EpochPhasePoCValidateWindDown="PoCValidateWindDown" )
const ( ConfirmationPoCInactive="CONFIRMATION_POC_INACTIVE"; ConfirmationPoCGracePeriod="CONFIRMATION_POC_GRACE_PERIOD"; ConfirmationPoCGeneration="CONFIRMATION_POC_GENERATION"; ConfirmationPoCValidation="CONFIRMATION_POC_VALIDATION"; ConfirmationPoCCompleted="CONFIRMATION_POC_COMPLETED" )
func rawPoCBlockingState(epochPhase, confirmationPhase string) (blocked bool, reason string)
func (s PhaseSnapshot) Admission() error   // nil when !RequestsBlocked; else a *RequestBlockedError{Reason, Message}
type RequestBlockedError struct{ Reason, Message string }
```
`rawPoCBlockingState` truth table (from `:956`): epochPhase in {PoCGenerate, PoCGenerateWindDown, PoCValidate, PoCValidateWindDown} → (true,"poc"); else confirmationPhase in {GracePeriod, Generation, Validation} → (true,"confirmation_poc"); else (false,""). Inference/Inactive/Completed are non-blocking. Note: `RequestsBlocked` in the snapshot = `rawBlocked` (relaxed-mode override is an api-phase concern now, not the observer's — document this; the old `deriveChainPhaseSnapshot` folded relaxed-mode in, but relaxed-mode is an operational toggle that belongs where admission is decided).

- [ ] Step 1: table-driven test over ALL epoch×confirmation enum combinations (5×5=25) asserting (blocked, reason); Admission() returns typed error iff blocked, with the humanized message; the "poc" vs "confirmation_poc" reason precedence.
- [ ] Step 2: RED. Step 3: implement. Step 4: green.
- [ ] Step 5: checkpoint `feat(gateway): chain phase snapshot + block derivation`.

### Task B2: fetch + parse → derive snapshot

**Files:** `chain/observer_fetch.go` (fetch/parse helpers), tests. (Kept separate from the loop for testability; merged into observer.go is fine if it stays small.)

**Interfaces produced (unexported, exercised via the loop in B4 but unit-tested directly here):**
```go
func parseEpochInfo(body []byte) (epochInfo, error)          // /v1/epochs/latest
func parseParticipants(body []byte, pocActive bool) (participantsState, error) // /v1/epochs/current/participants
func parsePreservedSnapshot(body []byte, expectedAnchor int64) (preservedSnapshotState, preservedSnapshotStatus, error)
```
Parse the JSON shapes documented in the porting map: epoch `{block_height, phase, latest_epoch{index, poc_start_block_height}, is_confirmation_poc_active, active_confirmation_poc_event{phase, trigger_height}, epoch_stages{...}}`; participants `active_participants.participants[]{index, inference_url, weight, models[], ml_nodes[]{ml_nodes[]{node_id, timeslot_allocation[], poc_weight}}}` — **key everything by participant address (`index`), never URL**; weights from ML-node `poc_weight` (during PoC only preserved nodes contribute). Flexible scalar decoders `jsonInt64`/`jsonUint64`/`confirmationPoCPhaseValue` (accept string or int) ported. `nil` preserved = not-loaded sentinel.

- [ ] Step 1: tests feed canned JSON bodies (captured shapes as testdata strings) → assert parsed fields; the flexible-scalar cases (confirmation phase as int 0-4 vs string); the weight derivation (poc_weight summed per participant); empty/absent arrays → empty maps not nil-panics.
- [ ] Step 2: RED. Step 3: implement. Step 4: green.
- [ ] Step 5: checkpoint `feat(gateway): chain epoch/participant/preserved parsers`.

### Task B3: VersionsCache

**Files:** `chain/versions.go`, `chain/versions_test.go`.

**Interfaces produced:**
```go
type VersionsCache struct{ /* client, ttl, mu, candidates map[string]string, entries map[string]versionsEntry, now func()time.Time */ }
func NewVersionsCache(client *http.Client, ttl time.Duration, now func() time.Time) *VersionsCache
func (v *VersionsCache) SetCandidates(minerToBaseURL map[string]string)   // replace set, drop stale entries
func (v *VersionsCache) Poll(ctx context.Context)                          // one pass, fetchOne per candidate
func (v *VersionsCache) IsNodeValidationCapable(miner, nodeID string) bool // fail-closed: false if unknown/nil/stale(now-fetchedAt>ttl)
func (v *VersionsCache) Run(ctx context.Context, interval time.Duration)   // ticker loop until ctx done
```
`fetchOne`: GET `base+"/v1/versions"` → `{mlnodes:[{node_id, poc_validation_inference}]}`; **nil map on any error/non-200** (fail-closed). `IsNodeValidationCapable` false when miner unknown, entry nil, or `now-fetchedAt > ttl`.

- [ ] Step 1: tests against httptest stub: capable node → true within ttl; unknown miner → false; stub returns 500 → false (fail-closed); staleness via injected `now` advanced past ttl → false; SetCandidates drops entries for removed miners. Run loop stops on ctx cancel (goleak-style: assert goroutine exits).
- [ ] Step 2: RED. Step 3: implement. Step 4: green under `-race`.
- [ ] Step 5: checkpoint `feat(gateway): chain versions cache (fail-closed)`.

### Task B4: PhaseObserver — poll loop, publish, subscribe

**Files:** `chain/observer.go`, `chain/observer_test.go`.

**Interfaces produced:**
```go
type PhaseObserver struct{ /* publicAPIBaseURL, chainRESTBaseURL, client, pollInterval, now, versions, current atomic.Pointer[PhaseSnapshot], subscribers, mu, stopCh, doneCh */ }
type ObserverConfig struct {
    PublicAPIBaseURL string; ChainRESTBaseURL string
    PollInterval time.Duration; HTTPClient *http.Client; Now func() time.Time
}
func NewPhaseObserver(cfg ObserverConfig) (*PhaseObserver, error)   // errors if PublicAPIBaseURL empty
func (o *PhaseObserver) Start(ctx context.Context)                  // spawns versions.Run + poll loop
func (o *PhaseObserver) Stop()                                      // close stopCh, wait doneCh
func (o *PhaseObserver) Snapshot() PhaseSnapshot                    // atomic load
func (o *PhaseObserver) Subscribe(cb func(PhaseSnapshot)) (cancel func())  // fires on each publish; same holder semantics as config.Holder
func (o *PhaseObserver) Versions() *VersionsCache
```
`refresh()` (one poll): parseEpochInfo → derive base snapshot (B1) → if not blocked-phase OR capacity needed, fetchParticipants → fold weights/preserved/inferenceURLs into the snapshot → feed `versions.SetCandidates(inferenceURLs)` (observer-internal) → publish (atomic store + notify subscribers). Subscribe/publish reuses the exact pattern from `config.Holder` (store-before-notify, synchronous callbacks, cancel via map delete). The loop: immediate refresh, then `time.NewTicker(pollInterval)`; `select` on ctx/stopCh; `defer close(doneCh)`.

**Design note in code (1 line):** the observer publishes raw inputs; scale-factor and speculative-attempt policy are derived by subscribers (limits/engine), not here.

- [ ] Step 1: tests wire a stub public-API server (epoch + participants endpoints) → Start → poll once → Snapshot() has the derived fields; Subscribe fires with the new snapshot on change; a second poll with changed phase notifies again; Stop() terminates the loop and the versions loop (goleak clean). Blocked-phase snapshot sets RequestsBlocked + reason. Injected `now`/tiny pollInterval — no real sleeps.
- [ ] Step 2: RED. Step 3: implement. Step 4: green under `-race` with `goleak` (the repo uses `go.uber.org/goleak`).
- [ ] Step 5: checkpoint `feat(gateway): chain phase observer — poll, publish, subscribe`.

---

## Phase 3 Definition of Done

- `go test ./cmd/gateway/chain/ -race -count=1` fully green; tx-encoding goldens matched byte-for-byte; observer/versions loops goleak-clean.
- `go test ./cmd/gateway/... -race -count=1` green; `go build ./...`; gofmt/vet clean; no `os.Getenv` in chain.
- Old package diff = exactly one build-tagged file (`chain_txgold_test.go`); prod build of devshardctl unaffected.
- `chain` imports only `config`, `devshard/signing`, stdlib (verify: no bridge/proto/types-for-encoding).
- Comment sweep (§G) over the new files as the final step.

## What later phases consume

- **escrow (Phase 6):** `TxClient.CreateEscrow/SettleEscrow/GetTxEscrowID` + `ErrTxNotFound` + the `onPrepared` intent hook (intent-commitment crash recovery).
- **limits (Phase 5) / scheduler (Phase 7) / engine (Phase 8):** `PhaseObserver.Subscribe` + `PhaseSnapshot` (weights → scale, preserved → host filtering, phase → speculative policy, `Versions().IsNodeValidationCapable`).
- **api (Phase 9):** `PhaseSnapshot.Admission()` as the pre-queue admission predicate.
