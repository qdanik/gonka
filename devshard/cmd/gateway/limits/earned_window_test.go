package limits

import (
	"testing"

	"devshard/cmd/gateway/config"
)

const earnedModel = "model-a"

var earnedEpoch = testEpoch

// The arithmetic is pinned here rather than taken from the defaults, so a retuned fleet figure does not
// silently restate what these tests claim.
func weightedConfig() ParticipantConfig {
	settings := ParticipantConfigFromLimits(config.Defaults().Limits)
	settings.Pricing.ConcurrencyPer10000Weight = 8
	settings.Pricing.ContextTokensByModel = map[string]int64{earnedModel: 40_000}
	settings.Pricing.OutputTokensByModel = map[string]int64{earnedModel: 4_096}
	return settings
}

func concurrentRequests(t *testing.T, limiter *ParticipantLimiter, participant string) int {
	t.Helper()
	cost := TokenCost{Input: 40_000, Output: 4_096}
	admitted := 0
	for range 256 {
		if _, admission := limiter.Acquire(participant, earnedModel, cost); admission != AdmissionOpen {
			break
		}
		admitted++
	}
	return admitted
}

// A window is what the chain's weight buys: the host that earned four times the weight takes four times
// the work, instead of both being handed the same flat window and the weaker one drowning in it.
func TestAHostsWindowIsSizedByTheWeightItEarned(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))
	limiter.ObserveWeights(map[string]map[string]float64{earnedModel: {
		"strong": 40_000,
		"weak":   10_000,
	}})

	strong, weak := concurrentRequests(t, limiter, "strong"), concurrentRequests(t, limiter, "weak")

	if weak != 8 {
		t.Fatalf("the 10000-weight host took %d requests, want the 8 its weight buys", weak)
	}
	if strong != 32 {
		t.Fatalf("the 40000-weight host took %d requests, want the 32 its weight buys", strong)
	}
}

// Nothing in the chain's view is guaranteed: a host with no weight observed keeps the configured window.
func TestAHostWithNoWeightKeepsTheConfiguredWindow(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))

	if admitted := concurrentRequests(t, limiter, "unweighed"); admitted != 32 {
		t.Fatalf("an unweighed host took %d requests, want the configured 32", admitted)
	}
}

// The chain moves weight every epoch, and PoC takes most of it away. A window already open has to follow it
// down, or the host keeps a window its weight no longer pays for.
func TestAWindowFollowsTheWeightDown(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))
	limiter.ObserveWeights(map[string]map[string]float64{earnedModel: {"host": 40_000}})
	if admitted := concurrentRequests(t, limiter, "host"); admitted != 32 {
		t.Fatalf("the host took %d requests at full weight, want 32", admitted)
	}

	limiter.ObserveWeights(map[string]map[string]float64{earnedModel: {"host": 10_000}})

	window, tracked := limiter.WindowFor("host", earnedModel)
	if !tracked {
		t.Fatal("the host stopped being tracked when its weight moved")
	}
	if window.OutputWindowTokens != 8*4_096 {
		t.Fatalf("output window = %v tokens, want the 8 requests the smaller weight buys", window.OutputWindowTokens)
	}
}

// The two windows are priced differently on purpose. A request reserves its whole output budget, so the
// output window is exactly the concurrency the weight bought. A request almost never fills the context, so
// the input window opens at twice what that concurrency would reserve and only floors there.
func TestTheTwoWindowsArePricedOnWhatARequestCosts(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))
	limiter.ObserveWeights(map[string]map[string]float64{earnedModel: {"host": 10_000}})

	limiter.admitOne("host", earnedModel)
	bounds := limiter.states[key{participant: "host", model: earnedModel}].bounds

	const concurrency = 8
	if bounds.Output.Min != concurrency*4_096 || bounds.Output.Initial != concurrency*4_096 {
		t.Fatalf("output window = %+v, want floor and start both at the %d requests 10000 weight buys",
			bounds.Output, concurrency)
	}
	if bounds.Input.Min != concurrency*40_000 || bounds.Input.Initial != 2*concurrency*40_000 {
		t.Fatalf("input window = %+v, want a floor of %d contexts and a start of twice that",
			bounds.Input, concurrency)
	}
	if bounds.Input.Step != 40_000 || bounds.Output.Step != 4_096 {
		t.Fatalf("steps = (%d, %d), want one context and one output budget: an answer buys a whole request",
			bounds.Input.Step, bounds.Output.Step)
	}
}

// A host with a weight too small to buy a whole request still gets one, or it can never earn its way up.
func TestTheSmallestWeightStillBuysOneRequest(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))
	limiter.ObserveWeights(map[string]map[string]float64{earnedModel: {"tiny": 100}})

	if admitted := concurrentRequests(t, limiter, "tiny"); admitted != 1 {
		t.Fatalf("a 100-weight host took %d requests, want exactly 1", admitted)
	}
}

func TestWindowForAnswersOnlyForATrackedPair(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))

	if _, tracked := limiter.WindowFor("never-seen", earnedModel); tracked {
		t.Fatal("WindowFor invented a window for a host the limiter has never admitted")
	}
	limiter.admitOne("host", earnedModel)
	if _, tracked := limiter.WindowFor("host", earnedModel); !tracked {
		t.Fatal("WindowFor lost a host the limiter is tracking")
	}
}

// The fleet's front door is what keeps a request from being admitted only to burn a nonce at a full host,
// so it has to follow what the hosts have earned: a window a host grew into is capacity the gateway has.
func TestTheFrontDoorFollowsWhatTheHostsEarned(t *testing.T) {
	t.Parallel()
	weightDerived := ModelCapacity{CurrentWeight: 10_000, BaselineWeight: 10_000, MaxConcurrentPer10000Weight: 5}

	cold, limited := effectiveConcurrencyLimit(2048, weightDerived)
	if !limited || cold != 5 {
		t.Fatalf("cold fleet limit = %d (limited %v), want the 5 its weight buys", cold, limited)
	}

	grown := weightDerived
	grown.HostWindowRequests = 40
	if limit, _ := effectiveConcurrencyLimit(2048, grown); limit != 40 {
		t.Fatalf("fleet limit = %d once the hosts grew to 40, want 40: the door must not shut out what they earned", limit)
	}
}

// With no weight to price it from, the hosts' own windows are still an answer.
func TestTheFrontDoorTakesTheHostWindowsWithoutAWeight(t *testing.T) {
	t.Parallel()

	limit, limited := effectiveConcurrencyLimit(2048, ModelCapacity{HostWindowRequests: 24})

	if !limited || limit != 24 {
		t.Fatalf("fleet limit = %d (limited %v), want the 24 the hosts can take", limit, limited)
	}
}

// One model's door is one model's hosts, and a host whose cut-off is open can take nothing at all.
func TestTheFleetsRoomIsTheSumOfItsHostsWindows(t *testing.T) {
	t.Parallel()
	limiter := newTestLimiter(weightedConfig(), fixedNow(earnedEpoch))
	limiter.ObserveWeights(map[string]map[string]float64{earnedModel: {"a": 10_000, "b": 20_000}})
	limiter.admitOne("a", earnedModel)
	limiter.admitOne("b", earnedModel)
	limiter.admitOne("c", "other-model")

	if room := limiter.ModelConcurrency(earnedModel); room != 8+16 {
		t.Fatalf("room for %s = %d, want the 8 and 16 its two hosts buy, and nothing from another model", earnedModel, room)
	}
	if room := limiter.ModelConcurrency("never-served"); room != 0 {
		t.Fatalf("room for a model no host serves = %d, want 0", room)
	}
}

// Windows grow without a ceiling, so the configured maximum has to stay one over the door they lift.
func TestTheConfiguredMaximumStillCapsADoorTheHostsLifted(t *testing.T) {
	t.Parallel()
	grown := ModelCapacity{CurrentWeight: 10_000, BaselineWeight: 10_000, MaxConcurrentPer10000Weight: 5, HostWindowRequests: 5_000}

	if limit, _ := effectiveConcurrencyLimit(2048, grown); limit != 2048 {
		t.Fatalf("fleet limit = %d, want the configured 2048: a window that grew past the process's own ceiling is not admission", limit)
	}
	if limit, _ := effectiveConcurrencyLimit(0, grown); limit != 5_000 {
		t.Fatalf("fleet limit = %d with no configured maximum, want the 5000 the hosts can take", limit)
	}
}
