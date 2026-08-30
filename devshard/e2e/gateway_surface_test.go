package e2e

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"devshard/e2e/testutil"
)

func gatewayGet(t *testing.T, client *http.Client, url, bearer string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("building GET %s: %v", url, err)
	}
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading GET %s: %v", url, err)
	}
	return response.StatusCode, string(body)
}

// The kill switch stops the serving, not the operating: an escrow still has to be inspectable and settleable.
func TestE2E_GatewayKillSwitchLeavesTheOperatorSurfaceUp(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{
		gatewayEnvOverrides: map[string]string{"GATEWAY_DISABLED": "true"},
	})

	if resp := testutil.SendCompletionRaw(t, client, env.clientURL, "while disabled", testutil.AdminAPIKey); resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("chat while disabled = %d %s, want 503", resp.StatusCode, resp.Body)
	}
	if status, _ := gatewayGet(t, client, env.clientURL+"/metrics", ""); status != http.StatusOK {
		t.Errorf("/metrics while disabled = %d, want 200: a gateway nobody can scrape cannot be diagnosed", status)
	}
	if status, _ := gatewayGet(t, client, env.clientURL+"/v1/admin/state", testutil.AdminAPIKey); status != http.StatusOK {
		t.Errorf("admin state while disabled = %d, want 200: the escrow still has to be settleable", status)
	}
}

// Without a configured key the admin surface does not exist at all, and says 404 rather than 401.
func TestE2E_GatewayWithoutAnAdminKeyHidesTheOperatorRoutes(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{
		gatewayEnvOverrides: map[string]string{"GATEWAY_ADMIN_API_KEY": ""},
	})

	if status, body := gatewayGet(t, client, env.clientURL+"/v1/admin/state", testutil.AdminAPIKey); status != http.StatusNotFound {
		t.Errorf("admin state with no key configured = %d %s, want 404", status, body)
	}
}

// A pin is a demand for one escrow, so an unknown one must fail rather than fall back to whatever is routable.
func TestE2E_GatewayServesAPinnedEscrowAndRefusesAnUnknownPin(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	pinned := testutil.PostJSONRaw(t, client,
		env.clientURL+"/devshard/"+defaultEscrowID+"/v1/chat/completions",
		testutil.ChatCompletionBody("pinned", false), testutil.AdminAPIKey)
	testutil.RequireOpenAINonStreamingCompletion(t, pinned)

	unknown := testutil.PostJSONRaw(t, client,
		env.clientURL+"/devshard/999999/v1/chat/completions",
		testutil.ChatCompletionBody("unknown pin", false), testutil.AdminAPIKey)
	if unknown.StatusCode == http.StatusOK {
		t.Errorf("a pin to an escrow that does not exist was served anyway: %s", unknown.Body)
	}
}

// Route labels stay patterns: an id in a label grows the series with the escrow set, one more per probe.
func TestE2E_GatewayScrapeCarriesTemplatedRouteLabels(t *testing.T) {
	env, client := startGatewayEnv(t, e2eEnvOptions{})

	testutil.RequireOpenAINonStreamingCompletion(t,
		testutil.SendCompletionRaw(t, client, env.clientURL, "labelled", testutil.AdminAPIKey))
	testutil.RequireOpenAINonStreamingCompletion(t, testutil.PostJSONRaw(t, client,
		env.clientURL+"/devshard/"+defaultEscrowID+"/v1/chat/completions",
		testutil.ChatCompletionBody("labelled pin", false), testutil.AdminAPIKey))
	gatewayGet(t, client, env.clientURL+"/v1/../v1/models", "")

	status, scrape := gatewayGet(t, client, env.clientURL+"/metrics", "")
	if status != http.StatusOK {
		t.Fatalf("/metrics = %d", status)
	}
	if !strings.Contains(scrape, `path="/v1/chat/completions"`) {
		t.Error("the scrape carries no route label for the chat route")
	}
	if !strings.Contains(scrape, `path="/devshard/{id}/v1/chat/completions"`) {
		t.Error("the pinned chat route raised no series, so the templating below is unchecked")
	}
	for line := range strings.SplitSeq(scrape, "\n") {
		if strings.Contains(line, `path="/devshard/`) && !strings.Contains(line, "{id}") {
			t.Errorf("a route label carries a concrete escrow id, which grows the series with the escrow set: %s", line)
		}
	}
}
