package chain

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetEscrowMultiEndpointAgreement(t *testing.T) {
	const escrowID = "77"
	notFoundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	notFoundBodyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{"found": false})
	})
	transientErrorHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})
	foundHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONResponse(t, w, map[string]any{
			"found":  true,
			"escrow": map[string]any{"amount": "12345"},
		})
	})

	tests := []struct {
		name        string
		primary     http.HandlerFunc
		fallback    http.HandlerFunc
		wantBalance uint64
		wantFound   bool
		checkErr    func(t *testing.T, err error)
	}{
		{
			name:        "found on primary",
			primary:     foundHandler.ServeHTTP,
			fallback:    notFoundHandler.ServeHTTP,
			wantBalance: 12345,
			wantFound:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			// Proves iteration continues past a not-found endpoint to a later success.
			name:        "found on fallback after primary 404",
			primary:     notFoundHandler.ServeHTTP,
			fallback:    foundHandler.ServeHTTP,
			wantBalance: 12345,
			wantFound:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name:     "all endpoints 404 -> not found, no error",
			primary:  notFoundHandler.ServeHTTP,
			fallback: notFoundHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name:     "all endpoints report found:false -> not found, no error",
			primary:  notFoundBodyHandler.ServeHTTP,
			fallback: notFoundBodyHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
			},
		},
		{
			name:     "one 404 one transient error -> ambiguous, non-nil error",
			primary:  notFoundHandler.ServeHTTP,
			fallback: transientErrorHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("want non-nil error, got nil")
				}
			},
		},
		{
			name:     "one found:false one transient error -> ambiguous, non-nil error",
			primary:  notFoundBodyHandler.ServeHTTP,
			fallback: transientErrorHandler.ServeHTTP,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("want non-nil error, got nil")
				}
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			primaryServer := httptest.NewServer(testCase.primary)
			defer primaryServer.Close()
			fallbackServer := httptest.NewServer(testCase.fallback)
			defer fallbackServer.Close()

			client, err := NewTxClient(Config{
				RESTBaseURL:         primaryServer.URL,
				TxQueryFallbackURLs: []string{fallbackServer.URL},
			})
			if err != nil {
				t.Fatalf("NewTxClient: %v", err)
			}

			info, found, err := client.GetEscrow(t.Context(), escrowID)
			testCase.checkErr(t, err)
			if found != testCase.wantFound {
				t.Errorf("found = %v, want %v", found, testCase.wantFound)
			}
			if found && info.Balance != testCase.wantBalance {
				t.Errorf("Balance = %d, want %d", info.Balance, testCase.wantBalance)
			}
			if found && info.EscrowID != escrowID {
				t.Errorf("EscrowID = %q, want %q", info.EscrowID, escrowID)
			}
		})
	}
}
