package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"devshard/testenv/mockopenai"
	"devshard/testenv/sessionload"
)

// result is everything one run measured; the aggregate step reads only this file.
type result struct {
	Gateway  gatewayConfig `json:"gateway"`
	Scenario string        `json:"scenario"`
	Seed     uint64        `json:"seed"`
	Elapsed  float64       `json:"elapsed_s"`

	Turns    int     `json:"turns"`
	Errors   int     `json:"errors"`
	Retries  int     `json:"client_retries"`
	TTFTP50  float64 `json:"ttft_p50_ms"`
	TTFTP95  float64 `json:"ttft_p95_ms"`
	LatP50   float64 `json:"latency_p50_ms"`
	LatP95   float64 `json:"latency_p95_ms"`
	Requests float64 `json:"gateway_requests_ok"`

	PromptTokens    uint64  `json:"host_prompt_tokens"`
	CachedTokens    uint64  `json:"host_cached_tokens"`
	HitRate         float64 `json:"hit_rate"`
	Executions      []int   `json:"host_executions"`
	ShareMaxOverMin float64 `json:"host_share_max_over_min"`
	FollowUps       int     `json:"follow_ups"`
	SameHost        int     `json:"follow_ups_same_host"`
	SameHostShare   float64 `json:"same_host_share"`
	DuplicateExecs  int     `json:"duplicate_executions"`
	ReplaysAtHosts  uint64  `json:"validation_replays_at_hosts"`
	Keyed           split   `json:"keyed_conversations"`
	Unkeyed         split   `json:"unkeyed_conversations"`

	Attempts       float64            `json:"gateway_attempts"`
	AffinityHits   float64            `json:"affinity_hits"`
	Exceptions     map[string]float64 `json:"affinity_exceptions"`
	AffinityBurns  float64            `json:"affinity_burns"`
	GhostBurns     float64            `json:"ghost_burns"`
	BurnsPerReq    float64            `json:"burns_per_request"`
	AssignedNonces float64            `json:"ledger_assigned_nonces"`
	LedgerCost     float64            `json:"ledger_chain_cost"`
	LedgerReserved float64            `json:"ledger_reserved_cost"`
	SpendPerReq    float64            `json:"spend_per_request"`
	NoncesPerReq   float64            `json:"nonces_per_request"`
	Epochs         json.RawMessage    `json:"ledger_epochs,omitempty"`
}

func collect(spec runSpec, ports portSet, turns []sessionload.Turn, elapsed time.Duration) (*result, error) {
	r := &result{Gateway: spec.Gateway, Scenario: spec.Scenario.Name, Seed: spec.Seed, Elapsed: elapsed.Seconds()}
	summarizeTurns(r, turns)
	raw, _ := json.Marshal(turns)
	if err := os.WriteFile(filepath.Join(spec.OutDir, "turns.json"), raw, 0o644); err != nil {
		return nil, err
	}
	if err := collectHosts(r, spec, ports, keyedByFingerprint(turns)); err != nil {
		return nil, err
	}
	if err := collectGateway(r, ports); err != nil {
		return nil, err
	}
	return r, nil
}

func summarizeTurns(r *result, turns []sessionload.Turn) {
	var ttft, latency []float64
	for _, t := range turns {
		if t.Error == sessionload.CancelledAtRunEnd {
			continue
		}
		r.Turns++
		r.Retries += t.Retries
		if t.Error != "" {
			r.Errors++
			continue
		}
		ttft = append(ttft, float64(t.TTFT)/1e6)
		latency = append(latency, float64(t.Latency)/1e6)
	}
	r.TTFTP50, r.TTFTP95 = percentile(ttft, 50), percentile(ttft, 95)
	r.LatP50, r.LatP95 = percentile(latency, 50), percentile(latency, 95)
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	return sorted[max(0, int(math.Ceil(float64(len(sorted))*p/100))-1)]
}

// split is the host-side result for one kind of conversation: those that send prompt_cache_key or those keyed by the gateway.
type split struct {
	Prompt    uint64  `json:"prompt_tokens"`
	Cached    uint64  `json:"cached_tokens"`
	HitRate   float64 `json:"hit_rate"`
	FollowUps int     `json:"follow_ups"`
	SameHost  int     `json:"same_host"`
	SameShare float64 `json:"same_host_share"`
}

func (s *split) finish() {
	if s.Prompt > 0 {
		s.HitRate = float64(s.Cached) / float64(s.Prompt)
	}
	if s.FollowUps > 0 {
		s.SameShare = float64(s.SameHost) / float64(s.FollowUps)
	}
}

type execution struct {
	host, msgs, prompt, cached int
	at                         int64
	fp                         string
}

// collectHosts reads each mock host's own counters and request log: the hit rate and the landing share come from the hosts, not the gateway.
func collectHosts(r *result, spec runSpec, ports portSet, keyed map[string]bool) error {
	var all []execution
	for i := 0; i < spec.Scenario.Hosts; i++ {
		var stats mockopenai.Stats
		if err := getJSON("http://127.0.0.1:"+strconv.Itoa(ports.ml[i])+"/testenv/stats", &stats); err != nil {
			return err
		}
		r.PromptTokens += stats.PromptTokens
		r.CachedTokens += stats.CachedPromptTokens
		r.ReplaysAtHosts += stats.ValidationReplays
		logged, err := readExecutions(filepath.Join(spec.OutDir, fmt.Sprintf("requests-h%d.jsonl", i)), i)
		if err != nil {
			return err
		}
		r.Executions = append(r.Executions, len(logged))
		all = append(all, logged...)
	}
	if r.PromptTokens > 0 {
		r.HitRate = float64(r.CachedTokens) / float64(r.PromptTokens)
	}
	r.ShareMaxOverMin = maxOverMin(r.Executions)
	r.FollowUps, r.SameHost, r.DuplicateExecs = landing(all)
	r.Keyed, r.Unkeyed = splitByKeyed(all, keyed)
	if r.FollowUps > 0 {
		r.SameHostShare = float64(r.SameHost) / float64(r.FollowUps)
	}
	return nil
}

func maxOverMin(counts []int) float64 {
	if len(counts) == 0 || slices.Min(counts) == 0 {
		return 0
	}
	return float64(slices.Max(counts)) / float64(slices.Min(counts))
}

func readExecutions(path string, host int) ([]execution, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var out []execution
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var line struct {
			T      int64  `json:"t"`
			FP     string `json:"fp"`
			Msgs   int    `json:"msgs"`
			Prompt int    `json:"prompt"`
			Cached int    `json:"cached"`
		}
		if json.Unmarshal(scanner.Bytes(), &line) == nil {
			out = append(out, execution{host: host, msgs: line.Msgs, prompt: line.Prompt, cached: line.Cached, at: line.T, fp: line.FP})
		}
	}
	return out, scanner.Err()
}

func keyedByFingerprint(turns []sessionload.Turn) map[string]bool {
	out := map[string]bool{}
	for _, t := range turns {
		out[t.Fingerprint] = t.Keyed
	}
	return out
}

func splitByKeyed(all []execution, keyed map[string]bool) (with, without split) {
	var a, b []execution
	for _, e := range all {
		isKeyed, known := keyed[e.fp]
		if !known {
			continue
		}
		part, sum := &b, &without
		if isKeyed {
			part, sum = &a, &with
		}
		*part = append(*part, e)
		sum.Prompt += uint64(e.prompt)
		sum.Cached += uint64(e.cached)
	}
	with.FollowUps, with.SameHost, _ = landing(a)
	without.FollowUps, without.SameHost, _ = landing(b)
	with.finish()
	without.finish()
	return with, without
}

type turnKey struct {
	fp   string
	msgs int
}

// landing compares, per conversation, the host of each turn with the host of the turn before it; a turn executed twice counts once, at its first execution.
func landing(all []execution) (followUps, sameHost, duplicates int) {
	first := map[turnKey]execution{}
	for _, e := range all {
		key := turnKey{e.fp, e.msgs}
		if prior, seen := first[key]; seen {
			duplicates++
			if e.at >= prior.at {
				continue
			}
		}
		first[key] = e
	}
	for key, e := range first {
		prev, ok := first[turnKey{key.fp, key.msgs - 2}]
		if !ok {
			continue
		}
		followUps++
		if prev.host == e.host {
			sameHost++
		}
	}
	return followUps, sameHost, duplicates
}

func collectGateway(r *result, ports portSet) error {
	text, err := getText("http://127.0.0.1:" + strconv.Itoa(ports.gateway) + "/metrics")
	if err != nil {
		return err
	}
	samples := parseMetrics(text)
	r.Requests = samples.sum("devshard_gateway_requests_total", "outcome", "success")
	r.Attempts = samples.sum("devshard_gateway_attempts_started_total", "", "")
	r.AffinityHits = samples.sum("devshard_gateway_affinity_hits_total", "", "")
	r.AffinityBurns = samples.sum("devshard_gateway_affinity_burns_total", "", "")
	r.GhostBurns = samples.sum("devshard_gateway_ghost_nonces_burned_total", "", "")
	r.Exceptions = samples.byLabel("devshard_gateway_affinity_exceptions_total", "reason")
	if r.Requests > 0 {
		r.BurnsPerReq = r.GhostBurns / r.Requests
	}
	raw, err := getText("http://127.0.0.1:" + strconv.Itoa(ports.stats) + "/api/v1/epochs")
	if err != nil {
		return err
	}
	r.Epochs = json.RawMessage(raw)
	r.LedgerCost, r.LedgerReserved, r.AssignedNonces = ledgerTotals(r.Epochs)
	if r.Requests > 0 {
		r.SpendPerReq = (r.LedgerCost + r.AssignedNonces*feePerNonce) / r.Requests
		r.NoncesPerReq = r.AssignedNonces / r.Requests
	}
	return nil
}

// ledgerTotals sums chain_cost, reserved_cost and assigned_nonces over the epoch summaries the ledger serves.
func ledgerTotals(raw json.RawMessage) (cost, reserved, nonces float64) {
	var doc any
	if json.Unmarshal(raw, &doc) != nil {
		return 0, 0, 0
	}
	var walk func(v any)
	walk = func(v any) {
		switch node := v.(type) {
		case map[string]any:
			if _, summary := node["chain_cost"]; summary {
				cost += number(node["chain_cost"])
				reserved += number(node["reserved_cost"])
				nonces += number(node["assigned_nonces"])
				return
			}
			for _, child := range node {
				walk(child)
			}
		case []any:
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(doc)
	return cost, reserved, nonces
}

func number(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return 0
}

type sample struct {
	labels map[string]string
	value  float64
}

type metricSet map[string][]sample

func parseMetrics(text string) metricSet {
	set := metricSet{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		cut := strings.LastIndexByte(line, ' ')
		if cut < 0 {
			continue
		}
		value, err := strconv.ParseFloat(line[cut+1:], 64)
		if err != nil {
			continue
		}
		name, labels := line[:cut], map[string]string{}
		if open := strings.IndexByte(name, '{'); open >= 0 {
			for _, pair := range strings.Split(strings.TrimSuffix(name[open+1:], "}"), `",`) {
				if k, v, ok := strings.Cut(pair, `="`); ok {
					labels[k] = strings.Trim(v, `"`)
				}
			}
			name = name[:open]
		}
		set[name] = append(set[name], sample{labels, value})
	}
	return set
}

func (m metricSet) sum(name, label, want string) float64 {
	total := 0.0
	for _, s := range m[name] {
		if label == "" || s.labels[label] == want {
			total += s.value
		}
	}
	return total
}

func (m metricSet) byLabel(name, label string) map[string]float64 {
	out := map[string]float64{}
	for _, s := range m[name] {
		out[s.labels[label]] += s.value
	}
	return out
}

func getText(url string) (string, error) {
	resp, err := probeClient.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return string(body), nil
}

func getJSON(url string, into any) error {
	text, err := getText(url)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(text), into)
}
