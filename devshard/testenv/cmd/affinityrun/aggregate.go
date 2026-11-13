package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"devshard/testenv/sessionload"
)

// aggregate prints one table per scenario: a row per gateway config, each cell the mean over the seeds with the min and max beside it.
func aggregate(dir string, out io.Writer) error {
	paths, err := filepath.Glob(filepath.Join(dir, "*", "*", "*", "result.json"))
	if err != nil {
		return err
	}
	groups := map[string]map[string][]*result{}
	for _, path := range paths {
		var r result
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &r); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if err := recompute(filepath.Dir(path), &r); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if groups[r.Scenario] == nil {
			groups[r.Scenario] = map[string][]*result{}
		}
		groups[r.Scenario][r.Gateway.Name] = append(groups[r.Scenario][r.Gateway.Name], &r)
	}
	for _, scenarioName := range sortedKeys(groups) {
		fmt.Fprintf(out, "\n### Scenario %s\n\n", scenarioName)
		writeTable(out, groups[scenarioName])
	}
	return nil
}

// recompute rebuilds the fields that come from a run's saved turns and request logs with the current code, so runs made by an earlier build read alike.
func recompute(dir string, r *result) error {
	data, err := os.ReadFile(filepath.Join(dir, "turns.json"))
	if err != nil {
		return err
	}
	var turns []sessionload.Turn
	if err := json.Unmarshal(data, &turns); err != nil {
		return err
	}
	r.Turns, r.Errors, r.Retries = 0, 0, 0
	summarizeTurns(r, turns)
	logs, err := filepath.Glob(filepath.Join(dir, "requests-h*.jsonl"))
	if err != nil {
		return err
	}
	var all []execution
	for i := range logs {
		logged, err := readExecutions(filepath.Join(dir, fmt.Sprintf("requests-h%d.jsonl", i)), i)
		if err != nil {
			return err
		}
		all = append(all, logged...)
	}
	r.Keyed, r.Unkeyed = splitByKeyed(all, keyedByFingerprint(turns))
	return nil
}

type column struct {
	title string
	value func(*result) float64
	unit  string
}

var columns = []column{
	{"hit rate at hosts", func(r *result) float64 { return r.HitRate * 100 }, "%"},
	{"follow-ups on previous host", func(r *result) float64 { return r.SameHostShare * 100 }, "%"},
	{"TTFT p50 (ms)", func(r *result) float64 { return r.TTFTP50 }, ""},
	{"TTFT p95 (ms)", func(r *result) float64 { return r.TTFTP95 }, ""},
	{"latency p50 (ms)", func(r *result) float64 { return r.LatP50 }, ""},
	{"latency p95 (ms)", func(r *result) float64 { return r.LatP95 }, ""},
	{"hit rate, keyed conversations", func(r *result) float64 { return r.Keyed.HitRate * 100 }, "%"},
	{"hit rate, gateway-keyed conversations", func(r *result) float64 { return r.Unkeyed.HitRate * 100 }, "%"},
	{"same host, keyed conversations", func(r *result) float64 { return r.Keyed.SameShare * 100 }, "%"},
	{"same host, gateway-keyed conversations", func(r *result) float64 { return r.Unkeyed.SameShare * 100 }, "%"},
	{"burned nonces / request (all ghosts)", func(r *result) float64 { return r.BurnsPerReq }, ""},
	{"affinity burns / request", func(r *result) float64 { return safeDiv(r.AffinityBurns, r.Requests) }, ""},
	{"nonces assigned / request", func(r *result) float64 { return r.NoncesPerReq }, ""},
	{"escrow spend / request", func(r *result) float64 { return r.SpendPerReq }, ""},
	{"duplicate executions", func(r *result) float64 { return float64(r.DuplicateExecs) }, ""},
	{"validation replays at hosts", func(r *result) float64 { return float64(r.ReplaysAtHosts) }, ""},
	{"host share max/min", func(r *result) float64 { return r.ShareMaxOverMin }, ""},
	{"errors", func(r *result) float64 { return float64(r.Errors) }, ""},
	{"client retries", func(r *result) float64 { return float64(r.Retries) }, ""},
	{"requests", func(r *result) float64 { return r.Requests }, ""},
}

func writeTable(out io.Writer, byConfig map[string][]*result) {
	names := sortedKeys(byConfig)
	fmt.Fprintf(out, "| metric |")
	for _, name := range names {
		fmt.Fprintf(out, " %s (n=%d) |", name, len(byConfig[name]))
	}
	fmt.Fprintf(out, "\n|---|%s\n", strings.Repeat("---|", len(names)))
	for _, col := range columns {
		fmt.Fprintf(out, "| %s |", col.title)
		for _, name := range names {
			fmt.Fprintf(out, " %s |", spread(byConfig[name], col))
		}
		fmt.Fprintln(out)
	}
	reasons := map[string]bool{}
	for _, rs := range byConfig {
		for _, r := range rs {
			for reason := range r.Exceptions {
				reasons[reason] = true
			}
		}
	}
	for _, reason := range sortedKeys(reasons) {
		fmt.Fprintf(out, "| exception %s (total) |", reason)
		for _, name := range names {
			fmt.Fprintf(out, " %s |", spread(byConfig[name], column{value: func(r *result) float64 { return r.Exceptions[reason] }}))
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintf(out, "| affinity hits (total) |")
	for _, name := range names {
		fmt.Fprintf(out, " %s |", spread(byConfig[name], column{value: func(r *result) float64 { return r.AffinityHits }}))
	}
	fmt.Fprintln(out)
}

func spread(runs []*result, col column) string {
	if len(runs) == 0 {
		return "-"
	}
	values := make([]float64, 0, len(runs))
	for _, r := range runs {
		values = append(values, col.value(r))
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return fmt.Sprintf("%s%s [%s–%s]", format(sum/float64(len(values))), col.unit, format(slices.Min(values)), format(slices.Max(values)))
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func format(v float64) string {
	switch {
	case v >= 1000:
		return fmt.Sprintf("%.0f", v)
	case v >= 10:
		return fmt.Sprintf("%.1f", v)
	default:
		return fmt.Sprintf("%.3g", v)
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
