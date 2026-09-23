package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"devshard/cmd/gateway/registry"
)

const collectSignaturesPath = "/devshard/" + liveEscrowID + "/v1/debug/signatures/collect"

func harnessWithLiveEscrow(t *testing.T) *harness {
	t.Helper()
	live := newHarness(t)
	session, machine := newLiveSession(t)
	live.escrows.sessions[liveEscrowID] = registry.NewSessionHandle(session, machine)
	return live
}

func TestCollectingSignaturesReportsTheQuorumAtTheNonce(t *testing.T) {
	live := harnessWithLiveEscrow(t)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest(http.MethodPost, collectSignaturesPath+"?nonce=0", nil).WithContext(cancelled)
	request.Header.Set("Authorization", "Bearer "+adminKey)
	recorder := httptest.NewRecorder()

	live.server.Handler().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("collect: got %d %s, want 200", recorder.Code, recorder.Body.String())
	}
	var answer struct {
		EscrowID        string `json:"escrow_id"`
		Nonce           uint64 `json:"nonce"`
		SignatureWeight uint32 `json:"sig_weight"`
		QuorumThreshold uint32 `json:"quorum_threshold"`
		TotalSlots      uint32 `json:"total_slots"`
		HasQuorum       bool   `json:"has_quorum"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil {
		t.Fatalf("decoding %s: %v", recorder.Body.String(), err)
	}
	if answer.EscrowID != liveEscrowID || answer.TotalSlots == 0 || answer.QuorumThreshold == 0 || answer.HasQuorum {
		t.Fatalf("answer = %+v, want this escrow's slots and threshold with no quorum yet", answer)
	}
}

func TestCollectingSignaturesRefusesAMalformedRequest(t *testing.T) {
	testCases := []struct {
		name   string
		method string
		target string
		want   int
		reason string
	}{
		{name: "missing nonce", method: http.MethodPost, target: collectSignaturesPath, want: http.StatusBadRequest, reason: "nonce"},
		{name: "unparseable nonce", method: http.MethodPost, target: collectSignaturesPath + "?nonce=seven", want: http.StatusBadRequest, reason: "nonce"},
		{name: "nonce ahead of the session", method: http.MethodPost, target: collectSignaturesPath + "?nonce=999", want: http.StatusBadRequest, reason: "ahead"},
		{name: "unknown escrow", method: http.MethodPost, target: "/devshard/404/v1/debug/signatures/collect?nonce=0", want: http.StatusNotFound, reason: "404"},
		{name: "read method", method: http.MethodGet, target: collectSignaturesPath + "?nonce=0", want: http.StatusMethodNotAllowed},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			live := harnessWithLiveEscrow(t)

			recorder := live.request(t, testCase.method, testCase.target, "", adminHeaders())

			if recorder.Code != testCase.want {
				t.Fatalf("got %d %s, want %d", recorder.Code, recorder.Body.String(), testCase.want)
			}
			if !strings.Contains(recorder.Body.String(), testCase.reason) {
				t.Fatalf("body %s does not name %q", recorder.Body.String(), testCase.reason)
			}
		})
	}
}

func TestCollectingSignaturesNeedsTheAdminKey(t *testing.T) {
	live := harnessWithLiveEscrow(t)

	recorder := live.request(t, http.MethodPost, collectSignaturesPath+"?nonce=0", "", nil)

	if recorder.Code != http.StatusUnauthorized && recorder.Code != http.StatusForbidden {
		t.Fatalf("collect without the admin key: got %d, want it refused", recorder.Code)
	}
}
