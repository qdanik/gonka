// Command affinityrun starts a GPU-free local stack (mock chain, five mock hosts with prefix caches, the gateway) as native processes and measures session affinity under multi-turn load.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

func main() {
	var (
		binDir   = flag.String("bin", "", "directory with the built binaries")
		outDir   = flag.String("out", "", "directory for this run's logs and result.json")
		cfgName  = flag.String("config", "C", "gateway config: A off, B bounds at their largest, B0 PR 34 binary, C defaults, D max_burns 2, E share_cap 1, P short pin age")
		scenName = flag.String("scenario", "equal", "equal, unequal, busy, noreplay or pinage")
		seed     = flag.Uint64("seed", 1, "load seed")
		duration = flag.Duration("duration", 0, "override the scenario duration")
		agg      = flag.String("aggregate", "", "directory of runs to turn into tables instead of running")
	)
	flag.Parse()
	if *agg != "" {
		if err := aggregate(*agg, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "aggregate:", err)
			os.Exit(1)
		}
		return
	}
	gw, gwOK := gatewayConfigs[*cfgName]
	sc, scOK := scenarios[*scenName]
	if !gwOK || !scOK || *binDir == "" || *outDir == "" {
		fmt.Fprintln(os.Stderr, "need -bin, -out and a known -config and -scenario")
		os.Exit(2)
	}
	if *duration > 0 {
		sc.Duration = *duration
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	abs, err := filepath.Abs(*binDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if gw.Binary == "" {
		gw.Binary = "gateway"
	}
	if _, err := os.Stat(filepath.Join(abs, gw.Binary)); err != nil {
		fmt.Fprintf(os.Stderr, "gateway binary %s is missing: %v (B0 needs a gateway built from d6c01d74 as %s)\n", gw.Binary, err, "gateway-pr34")
		os.Exit(2)
	}
	spec := runSpec{BinDir: abs, OutDir: *outDir, Gateway: gw, Scenario: sc, Seed: *seed, Load: defaultLoad}
	if err := runOnce(ctx, spec); err != nil {
		fmt.Fprintln(os.Stderr, "run failed:", err)
		os.Exit(1)
	}
}
