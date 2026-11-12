package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"devshard/testenv/config"
	"devshard/testenv/sessionload"
)

const (
	adminKey       = "testenv-citest-admin"
	keyringSecret  = "testenv1"
	ledgerSettle   = 12 * time.Second
	readyLimit     = 90 * time.Second
	stopGrace      = 8 * time.Second
	escrowKeyEnv   = "GATEWAY_ESCROW_KEY"
	maxNonceForRun = 5_000_000
	escrowAmount   = 2_000_000_000_000_000
	tokenPrice     = 10
	feePerNonce    = 1000
)

type runSpec struct {
	BinDir   string
	OutDir   string
	Gateway  gatewayConfig
	Scenario scenario
	Seed     uint64
	Load     loadShape
}

// loadShape sizes the conversations; prompts grow by userBytes every turn.
type loadShape struct {
	Sessions      int
	Turns         int
	CacheKeyShare float64
	SystemBytes   int
	UserBytes     int
}

var defaultLoad = loadShape{Sessions: 40, Turns: 8, CacheKeyShare: 0.5, SystemBytes: 2000, UserBytes: 2400}

// runOnce brings up a fresh stack, drives the load, writes <OutDir>/result.json and stops everything it started.
func runOnce(ctx context.Context, spec runSpec) (err error) {
	if err := os.MkdirAll(spec.OutDir, 0o755); err != nil {
		return err
	}
	if err := clearRunState(spec.OutDir); err != nil {
		return err
	}
	s := &stack{dir: spec.OutDir}
	defer s.stop(stopGrace)
	cfg, ports, err := prepareChain(ctx, spec, s)
	if err != nil {
		return err
	}
	if err := startStack(ctx, spec, s, cfg, ports); err != nil {
		return err
	}
	gatewayURL := "http://127.0.0.1:" + strconv.Itoa(ports.gateway)
	if err := warmUp(ctx, gatewayURL, cfg.Escrows[0].ModelID); err != nil {
		return err
	}
	sessions := spec.Load.Sessions
	if spec.Scenario.Sessions > 0 {
		sessions = spec.Scenario.Sessions
	}
	started := time.Now()
	turns := sessionload.Run(ctx, sessionload.Config{
		URL: gatewayURL, APIKey: adminKey, Model: cfg.Escrows[0].ModelID, Seed: spec.Seed, Sessions: sessions, Turns: spec.Load.Turns,
		Think: spec.Scenario.Think, CacheKeyShare: spec.Load.CacheKeyShare, SystemBytes: spec.Load.SystemBytes, UserBytes: spec.Load.UserBytes,
		MaxTokens: 64, Duration: spec.Scenario.Duration, RequestLimit: 60 * time.Second, MaxRetries: 2,
	})
	elapsed := time.Since(started)
	select {
	case <-time.After(ledgerSettle):
	case <-ctx.Done():
		return ctx.Err()
	}
	result, err := collect(spec, ports, turns, elapsed)
	if err != nil {
		return err
	}
	data, _ := json.MarshalIndent(result, "", "  ")
	return os.WriteFile(filepath.Join(spec.OutDir, "result.json"), data, 0o644)
}

// clearRunState removes what an earlier, failed run of the same directory left, so a fresh chain never meets old gateway or host databases.
func clearRunState(dir string) error {
	stale, err := filepath.Glob(filepath.Join(dir, "data-h*"))
	if err != nil {
		return err
	}
	for _, path := range append(stale, filepath.Join(dir, "gateway-data"), filepath.Join(dir, "keyring")) {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
}

type portSet struct {
	chainGRPC, chainRPC, chainAdmin       int
	dapiGRPC, dapiHTTP, ml, host, backend []int
	gateway, stats                        int
}

func allocPorts(hosts int) (portSet, error) {
	var (
		p   portSet
		err error
	)
	next := func() int {
		if err != nil {
			return 0
		}
		var port int
		port, err = freePort()
		return port
	}
	p.chainGRPC, p.chainRPC, p.chainAdmin, p.gateway, p.stats = next(), next(), next(), next(), next()
	for i := 0; i < hosts; i++ {
		p.dapiGRPC = append(p.dapiGRPC, next())
		p.dapiHTTP = append(p.dapiHTTP, next())
		p.ml = append(p.ml, next())
		p.host = append(p.host, next())
		p.backend = append(p.backend, next())
	}
	return p, err
}

// prepareChain runs gencompose for keys and the seed, then points every participant at its own loopback port and lays out the escrow groups.
func prepareChain(ctx context.Context, spec runSpec, s *stack) (*config.File, portSet, error) {
	ports, err := allocPorts(spec.Scenario.Hosts)
	if err != nil {
		return nil, ports, err
	}
	var hosts strings.Builder
	hosts.WriteString("hosts:\n")
	for i := 0; i < spec.Scenario.Hosts; i++ {
		fmt.Fprintf(&hosts, "  - {id: h%d, key_name: h%d}\n", i, i)
	}
	input := filepath.Join(s.dir, "seed.yaml")
	if err := os.WriteFile(input, []byte(hosts.String()), 0o644); err != nil {
		return nil, ports, err
	}
	if err := s.runToEnd(ctx, "gencompose", filepath.Join(spec.BinDir, "gencompose"), "-config", input, "-out", filepath.Join(s.dir, "compose.unused.yml")); err != nil {
		return nil, ports, err
	}
	cfg, err := config.Load(input)
	if err != nil {
		return nil, ports, fmt.Errorf("load generated chain config: %w", err)
	}
	cfg.Params.MaxNonce = maxNonceForRun
	for i := range cfg.Participants {
		cfg.Participants[i].InferenceURL = "http://127.0.0.1:" + strconv.Itoa(ports.host[i])
	}
	var slots []string
	for i, h := range cfg.Hosts {
		for k := 0; k < spec.Scenario.hostSlots(i); k++ {
			slots = append(slots, h.Address)
		}
	}
	template := cfg.Escrows[0]
	cfg.Escrows = nil
	for id := 1; id <= spec.Scenario.Escrows; id++ {
		escrow := template
		escrow.ID, escrow.Slots, escrow.Amount = uint64(id), slots, escrowAmount
		escrow.TokenPrice, escrow.FeePerNonce = tokenPrice, feePerNonce
		cfg.Escrows = append(cfg.Escrows, escrow)
	}
	if err := cfg.Save(filepath.Join(s.dir, "chain.yaml")); err != nil {
		return nil, ports, err
	}
	return cfg, ports, nil
}

func startStack(ctx context.Context, spec runSpec, s *stack, cfg *config.File, ports portSet) error {
	bin := func(name string) string { return filepath.Join(spec.BinDir, name) }
	local := func(port int) string { return "127.0.0.1:" + strconv.Itoa(port) }
	chainGRPC, chainRPC := local(ports.chainGRPC), "http://"+local(ports.chainRPC)
	chainAdmin := "http://" + local(ports.chainAdmin)
	if err := s.start("chain", bin("mockchain"), []string{
		"CONFIG_PATH=" + filepath.Join(s.dir, "chain.yaml"), "MOCK_CHAIN_GRPC_ADDR=" + chainGRPC,
		"MOCK_CHAIN_RPC_ADDR=" + local(ports.chainRPC), "MOCK_CHAIN_TESTENV_ADDR=" + local(ports.chainAdmin),
	}); err != nil {
		return err
	}
	if err := s.waitHTTP(ctx, "mock-chain", chainRPC+"/health", readyLimit); err != nil {
		return err
	}
	model := defaultHostModel
	cacheTokens := model.CacheTokens
	if spec.Scenario.CacheTokens > 0 {
		cacheTokens = spec.Scenario.CacheTokens
	}
	for i := 0; i < spec.Scenario.Hosts; i++ {
		mult := spec.Scenario.hostSlots(i)
		if err := s.start(fmt.Sprintf("ml%d", i), bin("mockopenai"), []string{
			"MOCK_OPENAI_ADDR=" + local(ports.ml[i]), "MOCK_OPENAI_TTFT=" + model.TTFT.String(),
			"MOCK_OPENAI_TOKEN_INTERVAL=" + model.TokenInterval.String(), "MOCK_OPENAI_WORKERS=" + strconv.Itoa(model.Workers*mult),
			"MOCK_OPENAI_QUEUE=1000", "MOCK_OPENAI_CACHE_TOKENS=" + strconv.Itoa(cacheTokens*mult),
			"MOCK_OPENAI_PREFILL_PER_TOKEN=" + model.PrefillPerToken.String(),
			"MOCK_OPENAI_CACHE_IGNORE_REPLAYS=" + strconv.FormatBool(spec.Scenario.IgnoreReplays),
			"MOCK_OPENAI_REQUEST_LOG=" + filepath.Join(s.dir, fmt.Sprintf("requests-h%d.jsonl", i)),
		}); err != nil {
			return err
		}
		mlURL := "http://" + local(ports.ml[i])
		if err := s.start(fmt.Sprintf("dapi%d", i), bin("mockdapi"), []string{
			"MOCK_DAPI_GRPC_ADDR=" + local(ports.dapiGRPC[i]), "MOCK_DAPI_HTTP_ADDR=" + local(ports.dapiHTTP[i]),
			"MOCK_CHAIN_GRPC_ADDR=" + chainGRPC, "MOCK_CHAIN_RPC_ADDR=" + chainRPC, "MOCK_CHAIN_TESTENV_URL=" + chainAdmin,
			"MOCK_ML_ENDPOINT=" + mlURL, fmt.Sprintf("MOCK_ML_NODES=ml%d=%s", i, mlURL), "CHAIN_ID=" + cfg.ChainID,
		}); err != nil {
			return err
		}
		dataDir := filepath.Join(s.dir, fmt.Sprintf("data-h%d", i))
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return err
		}
		if err := s.start(fmt.Sprintf("host%d", i), bin("devshardd"), []string{
			"NODE_GRPC_URL=" + chainGRPC, "NODE_RPC_URL=" + chainRPC, "NODE_HOST=127.0.0.1", "NODE_MANAGER_ADDR=" + local(ports.dapiGRPC[i]),
			"CHAIN_ID=" + cfg.ChainID, "KEYRING_BACKEND=file", "KEYRING_DIR=" + filepath.Join(s.dir, "keyring"), "KEYRING_PASSWORD=" + keyringSecret,
			"KEY_NAME=" + fmt.Sprintf("h%d", i), "DEVSHARD_ALLOW_PRIVATE_ADDRESSES=true", "LOG_FORMAT=json",
		}, "--port", strconv.Itoa(ports.backend[i]), "--data-dir", dataDir); err != nil {
			return err
		}
		if err := s.servePrefixProxy(ports.host[i], ports.backend[i]); err != nil {
			return err
		}
	}
	for i := 0; i < spec.Scenario.Hosts; i++ {
		if err := s.waitHTTP(ctx, fmt.Sprintf("host %d", i), "http://"+local(ports.backend[i])+"/healthz", readyLimit); err != nil {
			return err
		}
	}
	return startGateway(ctx, spec, s, cfg, ports)
}

func startGateway(ctx context.Context, spec runSpec, s *stack, cfg *config.File, ports portSet) error {
	var devshards []map[string]string
	for _, escrow := range cfg.Escrows {
		devshards = append(devshards, map[string]string{"id": strconv.FormatUint(escrow.ID, 10), "model": escrow.ModelID, "private_key_env": escrowKeyEnv})
	}
	encoded, _ := json.Marshal(devshards)
	storage := filepath.Join(s.dir, "gateway-data")
	if err := os.MkdirAll(storage, 0o755); err != nil {
		return err
	}
	env := append([]string{
		"DEVSHARD_PORT=" + strconv.Itoa(ports.gateway), "DEVSHARD_STATS_ENABLED=true", "DEVSHARD_STATS_PORT=" + strconv.Itoa(ports.stats),
		"DEVSHARD_CHAIN_GRPC=127.0.0.1:" + strconv.Itoa(ports.chainGRPC), "DEVSHARD_CHAIN_RPC=http://127.0.0.1:" + strconv.Itoa(ports.chainRPC),
		"DEVSHARD_CHAIN_ID=" + cfg.ChainID, "DEVSHARD_PUBLIC_API=http://127.0.0.1:" + strconv.Itoa(ports.dapiHTTP[0]),
		"DEVSHARDS_JSON=" + string(encoded), escrowKeyEnv + "=" + cfg.User.PrivateKeyHex, "DEVSHARD_PRIVATE_KEY=" + cfg.User.PrivateKeyHex,
		"DEVSHARD_ADMIN_API_KEY=" + adminKey, "DEVSHARD_STORAGE_DIR=" + storage, "DEVSHARD_ESCROW_ROTATION_ENABLED=false", "DEVSHARD_ALLOW_PRIVATE_ADDRESSES=true", "DEVSHARD_LOG_FORMAT=json",
	}, spec.Gateway.env()...)
	if err := s.start("gateway", filepath.Join(spec.BinDir, spec.Gateway.Binary), env); err != nil {
		return err
	}
	return s.waitHTTP(ctx, "gateway", "http://127.0.0.1:"+strconv.Itoa(ports.gateway)+"/healthz", readyLimit)
}

// warmUp sends one request until it is served, so every escrow has been taught to its group before the clock starts.
func warmUp(ctx context.Context, url, model string) error {
	body := fmt.Sprintf(`{"model":%q,"max_tokens":8,"messages":[{"role":"user","content":"warm up"}]}`, model)
	deadline := time.Now().Add(readyLimit)
	last := "no answer"
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url+"/v1/chat/completions", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminKey)
		if resp, err := probeClient.Do(req); err == nil {
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
			last = fmt.Sprintf("status %d: %s", resp.StatusCode, snippet)
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("gateway served no warm-up request within %s; last answer: %s", readyLimit, last)
}
