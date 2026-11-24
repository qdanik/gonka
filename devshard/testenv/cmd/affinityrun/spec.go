package main

import (
	"strconv"
	"time"
)

// gatewayConfig is one affinity setting of the gateway under test; the limit fields are the three added bounds.
type gatewayConfig struct {
	Name       string `json:"name"`
	Affinity   bool   `json:"affinity"`
	MaxPinAge  int64  `json:"max_pin_age_ms"`
	ShareCap   int64  `json:"share_cap"`
	MaxBurns   int64  `json:"max_burns"`
	TTLMS      int64  `json:"ttl_ms"`
	Describe   string `json:"describe"`
	Binary     string `json:"binary"`
	ExtraNotes string `json:"-"`
}

// The three bounds at their largest accepted values are the closest this build gets to PR 34, which has no bounds.
const (
	boundsOffPinAgeMS = 604_800_000
	boundsOffShareCap = 10_000
	boundsOffBurns    = 1000
)

// affinityOn builds one affinity-on setting; only the burns and the share cap vary between most of them.
func affinityOn(name string, burns, shareCap int64, describe string) gatewayConfig {
	return gatewayConfig{Name: name, Affinity: true, MaxPinAge: 3_600_000, ShareCap: shareCap, MaxBurns: burns, TTLMS: 600_000, Describe: describe}
}

var gatewayConfigs = map[string]gatewayConfig{
	"A":    {Name: "A", Affinity: false, MaxPinAge: 3_600_000, ShareCap: 125, MaxBurns: 2, TTLMS: 600_000, Describe: "affinity off"},
	"B":    {Name: "B", Affinity: true, MaxPinAge: boundsOffPinAgeMS, ShareCap: boundsOffShareCap, MaxBurns: boundsOffBurns, TTLMS: 600_000, Describe: "affinity on, the three bounds at their largest values (PR 34 behaviour)"},
	"B0":   {Name: "B0", Affinity: true, MaxPinAge: 3_600_000, ShareCap: 125, MaxBurns: 2, TTLMS: 600_000, Describe: "gateway built from d6c01d74 (PR 34 tip), which has no bounds", Binary: "gateway-pr34"},
	"C":    affinityOn("C", 1, 125, "max_burns 1, share cap 125 percent"),
	"D":    affinityOn("D", 2, 125, "final defaults: max_burns 2, share cap 125 percent, pin age 1 h"),
	"F":    affinityOn("F", 2, boundsOffShareCap, "D without the share cap"),
	"G100": affinityOn("G100", 2, 100, "D with share cap 100 percent"),
	"G110": affinityOn("G110", 2, 110, "D with share cap 110 percent"),
	"G150": affinityOn("G150", 2, 150, "D with share cap 150 percent"),
	"P":    {Name: "P", Affinity: true, MaxPinAge: 4_000, ShareCap: 125, MaxBurns: 2, TTLMS: 600_000, Describe: "D with max_pin_age 4 s, the pin-age check"},
}

func (g gatewayConfig) env() []string {
	return []string{
		"GATEWAY_AFFINITY_ENABLED=" + strconv.FormatBool(g.Affinity),
		"GATEWAY_AFFINITY_TTL_MS=" + strconv.FormatInt(g.TTLMS, 10),
		"GATEWAY_AFFINITY_MAX_PIN_AGE_MS=" + strconv.FormatInt(g.MaxPinAge, 10),
		"GATEWAY_AFFINITY_SHARE_CAP=" + strconv.FormatInt(g.ShareCap, 10),
		"GATEWAY_AFFINITY_MAX_BURNS=" + strconv.FormatInt(g.MaxBurns, 10),
	}
}

// scenario is the hosts and the load a gateway config is measured under; the same seed gives the same prompts.
type scenario struct {
	Name     string `json:"name"`
	Hosts    int    `json:"hosts"`
	Escrows  int    `json:"escrows"`
	BigHost  bool   `json:"big_host"`
	Duration time.Duration
	Think    time.Duration
	// Sessions and CacheTokens of 0 keep the defaults; IgnoreReplays stops validation replays from warming the host caches.
	Sessions      int
	CacheTokens   int
	IgnoreReplays bool
}

// hostSlots is how many slots of each escrow group host i holds; the big host holds four and has four times the workers and cache.
func (s scenario) hostSlots(i int) int {
	if s.BigHost && i == 0 {
		return 4
	}
	return 1
}

var scenarios = map[string]scenario{
	"equal":    {Name: "equal", Hosts: 5, Escrows: 2, Duration: 90 * time.Second, Think: 3 * time.Second},
	"unequal":  {Name: "unequal", Hosts: 5, Escrows: 2, BigHost: true, Duration: 90 * time.Second, Think: 3 * time.Second},
	"busy":     {Name: "busy", Hosts: 5, Escrows: 2, Duration: 60 * time.Second, Think: 1500 * time.Millisecond, Sessions: 90, CacheTokens: 96_000},
	"noreplay": {Name: "noreplay", Hosts: 5, Escrows: 2, Duration: 90 * time.Second, Think: 3 * time.Second, IgnoreReplays: true},
	"pinage":   {Name: "pinage", Hosts: 5, Escrows: 2, Duration: 45 * time.Second, Think: 2 * time.Second},
}

// hostModel is the mock-openai cost model of every host.
type hostModel struct {
	TTFT            time.Duration
	TokenInterval   time.Duration
	PrefillPerToken time.Duration
	CacheTokens     int
	Workers         int
}

var defaultHostModel = hostModel{
	TTFT:            60 * time.Millisecond,
	TokenInterval:   5 * time.Millisecond,
	PrefillPerToken: 150 * time.Microsecond,
	CacheTokens:     48_000,
	Workers:         8,
}
