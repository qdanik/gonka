package scenarios

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"devshard/cmd/gateway/api"
)

const (
	effectLabelName    = "effect"
	effectLabelDropped = "dropped"

	repeatedRequestBody = `{"model":"scenario-model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"the same question every time"}]}`
)

type cacheProbe struct{ hits []bool }

func cacheDrops(harness *gatewayHarness) (float64, error) {
	families, err := harness.currentGateway().Telemetry().Registry().Gather()
	if err != nil {
		return 0, err
	}
	var removed float64
	for _, family := range families {
		if family.GetName() != "devshard_gateway_cache_invalidations_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == effectLabelName && label.GetValue() == effectLabelDropped {
					removed += metric.GetCounter().GetValue()
				}
			}
		}
	}
	return removed, nil
}

type sendRepeatedRequest struct{ probe *cacheProbe }

func (step sendRepeatedRequest) apply(harness *gatewayHarness) {
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(repeatedRequestBody))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	harness.currentGateway().Handler().ServeHTTP(recorder, request)
	step.probe.hits = append(step.probe.hits, recorder.Header().Get(api.CacheStatusHeader) == "hit")
}

// Test flow:
//  1. Make validators 1, 2 and 3 vote every validation invalid, then send one request twice.
//  2. Assert the second is served from the cache, so the first was stored.
//  3. Send rounds of other requests so the invalid votes reach the session, then send the first request a third time.
//  4. Assert the third is not served from the cache: the invalid vote removed the entry and the request ran a race again.
func TestAReplyVotedInvalidIsNotServedFromTheCacheAgain(t *testing.T) {
	spec := defaultSpec()
	spec.validationRate = everyValidatorChecks
	spec.behaviours = map[int]participantBehaviour{
		1: {votesInvalid: true},
		2: {votesInvalid: true},
		3: {votesInvalid: true},
	}
	probe := &cacheProbe{}
	spec.steps = append([]harnessStep{sendRepeatedRequest{probe}, sendRepeatedRequest{probe}}, requestRounds(8, 4, 10*time.Second)...)
	spec.steps = append(spec.steps,
		sendRepeatedRequest{probe},
		expectThat{label: "the voted-invalid reply was raced again", verify: func(harness *gatewayHarness) error {
			if want := []bool{false, true, false}; !slices.Equal(probe.hits, want) {
				return fmt.Errorf("cache hits = %v, want %v", probe.hits, want)
			}
			removed, err := cacheDrops(harness)
			if err != nil {
				return err
			}
			if removed == 0 {
				return fmt.Errorf("cache entries dropped = %v, want at least one", removed)
			}
			return nil
		}},
	)
	runSteps(t, spec)
}
