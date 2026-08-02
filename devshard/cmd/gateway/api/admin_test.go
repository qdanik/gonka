package api

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"devshard/cmd/gateway/store"

	"devshard/cmd/gateway/internal/logcapture"
)

// guardedTree lays out a base storage dir and a sibling directory nothing may reach.
func guardedTree(t *testing.T) (base, guarded string) {
	t.Helper()
	root := t.TempDir()
	base = filepath.Join(root, "gateway")
	guarded = filepath.Join(root, "victim")
	for _, dir := range []string{filepath.Join(base, "escrow-7"), guarded} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatalf("preparing %s: %v", dir, err)
		}
	}
	return base, guarded
}

func TestRemoveDevshardStorageRefusesAPathOutsideItsBase(t *testing.T) {
	testCases := []struct {
		name       string
		target     func(base, guarded string) string
		wantRemove bool
	}{
		{
			name:       "a path inside the base is removed",
			target:     func(base, _ string) string { return filepath.Join(base, "escrow-7") },
			wantRemove: true,
		},
		{
			name:   "parent traversal",
			target: func(base, _ string) string { return base + "/../victim" },
		},
		{
			name:   "absolute path",
			target: func(_, guarded string) string { return guarded },
		},
		{
			name:   "url encoded traversal",
			target: func(base, _ string) string { return base + "/" + mustUnescape(t, "%2e%2e%2fvictim") },
		},
		{
			name:   "a sibling sharing the base's prefix",
			target: func(base, _ string) string { return base + "-other" },
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			base, guarded := guardedTree(t)
			err := removeDevshardStorage(testCase.target(base, guarded), base)

			if testCase.wantRemove {
				if err != nil {
					t.Fatalf("removing an in-base path: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("removeDevshardStorage accepted a path outside %q", base)
			}
			if _, statErr := os.Stat(guarded); statErr != nil {
				t.Fatalf("the guarded directory was deleted: %v", statErr)
			}
		})
	}
}

func TestAnEscrowIdCannotEscapeTheBaseStorageDir(t *testing.T) {
	testCases := []struct {
		name     string
		escrowID string
	}{
		{name: "parent segments after a real one", escrowID: "7/../../victim"},
		{name: "repeated parent segments", escrowID: "a/../../.."},
		{name: "url encoded parent segments", escrowID: mustUnescape(t, "7%2f%2e%2e%2f%2e%2e%2fvictim")},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			base, guarded := guardedTree(t)
			err := removeDevshardStorage(DevshardStoragePath(base, testCase.escrowID), base)
			if err == nil {
				t.Fatalf("escrow id %q reached %q", testCase.escrowID, filepath.Clean(DevshardStoragePath(base, testCase.escrowID)))
			}
			if _, statErr := os.Stat(guarded); statErr != nil {
				t.Fatalf("the guarded directory was deleted: %v", statErr)
			}
		})
	}
}

func TestRemoveDevshardStorageRefusesTheBaseDirectoryItself(t *testing.T) {
	base := t.TempDir()
	if err := removeDevshardStorage(base, base); err == nil {
		t.Fatal("removeDevshardStorage deleted its own base directory")
	}
	if _, err := os.Stat(base); err != nil {
		t.Fatalf("the base directory was deleted: %v", err)
	}
}

func TestRemoveDevshardStorageTreatsAStateFileAsItsDirectory(t *testing.T) {
	base := t.TempDir()
	escrowDir := filepath.Join(base, "escrow-7")
	if err := os.MkdirAll(escrowDir, 0o700); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	if err := removeDevshardStorage(filepath.Join(escrowDir, "state.db"), base); err != nil {
		t.Fatalf("removing: %v", err)
	}
	if _, err := os.Stat(escrowDir); !os.IsNotExist(err) {
		t.Fatalf("the escrow directory survived: %v", err)
	}
}

func TestRemoveDevshardStorageIgnoresAnEmptyPath(t *testing.T) {
	if err := removeDevshardStorage("  ", t.TempDir()); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}

func TestDeletingADevshardRemovesItsRowAndItsStorage(t *testing.T) {
	live := newHarness(t)
	escrowDir := filepath.Join(live.storageDir, "escrow-7")
	if err := os.MkdirAll(escrowDir, 0o700); err != nil {
		t.Fatalf("preparing: %v", err)
	}
	recorder := live.request(t, http.MethodDelete, "/v1/admin/devshards/7", "", adminHeaders())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if len(live.control.deleted) != 1 || live.control.deleted[0] != "7" {
		t.Fatalf("deleted rows: %v", live.control.deleted)
	}
	if _, err := os.Stat(escrowDir); !os.IsNotExist(err) {
		t.Fatalf("the escrow directory survived: %v", err)
	}
}

func TestDeletingABusyDevshardIsRefused(t *testing.T) {
	live := newHarness(t)
	live.escrows.busy["7"] = true
	recorder := live.request(t, http.MethodDelete, "/v1/admin/devshards/7", "", adminHeaders())
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status: got %d, want 409", recorder.Code)
	}
	if len(live.control.deleted) != 0 {
		t.Fatalf("a busy devshard's row was deleted: %v", live.control.deleted)
	}
}

func TestAStoreFailureOnTheOperatorRoutesIsA500(t *testing.T) {
	live := newHarness(t)
	live.control.listErr = errStoreUnavailable
	recorder := live.request(t, http.MethodGet, "/v1/admin/devshards", "", adminHeaders())
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", recorder.Code)
	}
}

// A settle claims funds against nonces requests are still spending, so no request body may buy its
// way past the busy check.
func TestSettlingABusyDevshardIsRefusedAndCannotBeForced(t *testing.T) {
	live := newHarness(t)
	live.escrows.busy["7"] = true
	for _, body := range []string{"", `{"force":true}`} {
		refused := live.request(t, http.MethodPost, "/v1/admin/devshards/7/settle", body, adminHeaders())
		if refused.Code != http.StatusConflict {
			t.Fatalf("settle with body %q: got %d (%s), want 409", body, refused.Code, refused.Body.String())
		}
	}
	if slices.Contains(live.operations.calls, "settle") {
		t.Fatalf("a busy devshard reached the settle path: %v", live.operations.calls)
	}
}

func TestAnAdminListReflectsTheStore(t *testing.T) {
	live := newHarness(t)
	live.control.devshards = []store.DevshardRecord{{EscrowID: "42", Model: "kimi", Active: true}}
	recorder := live.request(t, http.MethodGet, "/v1/admin/devshards", "", adminHeaders())
	if recorder.Code != http.StatusOK {
		t.Fatalf("status: got %d", recorder.Code)
	}
	if got := recorder.Body.String(); !strings.Contains(got, `"EscrowID":"42"`) {
		t.Fatalf("body: %s", got)
	}
}

// The failure log is the contract here: an operator action that returns 5xx is otherwise invisible,
// since auditAdmin only records the successful path.
func TestAFailedAdminOperationIsLogged(t *testing.T) {
	entries := logcapture.Install(t)
	live := newHarness(t)
	live.control.listErr = errStoreUnavailable

	live.request(t, http.MethodGet, "/v1/admin/devshards", "", adminHeaders())

	entry, found := entries.Find("admin request failed")
	if !found {
		t.Fatalf("a 500 on an admin route left no log line: %+v", entries.All())
	}
	if entry.Level != "error" {
		t.Fatalf("level = %q, want error", entry.Level)
	}
	if got := logcapture.Field(entry, "status"); got != http.StatusInternalServerError {
		t.Fatalf("status field = %v, want 500", got)
	}
	if got := logcapture.Field(entry, "route"); got != "/v1/admin/devshards" {
		t.Fatalf("route field = %v", got)
	}
	if got, _ := logcapture.Field(entry, "error").(string); !strings.Contains(got, errStoreUnavailable.Error()) {
		t.Fatalf("error field = %q, want the store failure", got)
	}
}

// An unkeyed call on an admin route is the shape an intrusion attempt takes, so the refusal is
// recorded even though nothing failed.
func TestAnUnkeyedAdminCallIsLogged(t *testing.T) {
	entries := logcapture.Install(t)
	live := newHarness(t)

	live.request(t, http.MethodGet, "/v1/admin/devshards", "", nil)

	entry, found := entries.Find("admin request refused")
	if !found {
		t.Fatalf("an unkeyed admin call left no log line: %+v", entries.All())
	}
	if entry.Level != "warn" {
		t.Fatalf("level = %q, want warn", entry.Level)
	}
	if got := logcapture.Field(entry, "status"); got != http.StatusUnauthorized {
		t.Fatalf("status field = %v, want 401", got)
	}
}

func TestASuccessfulAdminOperationIsNotLoggedAsAFailure(t *testing.T) {
	entries := logcapture.Install(t)
	live := newHarness(t)

	live.request(t, http.MethodGet, "/v1/admin/devshards", "", adminHeaders())

	if _, found := entries.Find("admin request failed"); found {
		t.Fatalf("a 200 was logged as a failure: %+v", entries.All())
	}
	if _, found := entries.Find("admin request refused"); found {
		t.Fatalf("a 200 was logged as a refusal: %+v", entries.All())
	}
}

func mustUnescape(t *testing.T, encoded string) string {
	t.Helper()
	decoded, err := url.PathUnescape(encoded)
	if err != nil {
		t.Fatalf("unescaping %q: %v", encoded, err)
	}
	return decoded
}

var errStoreUnavailable = errors.New("store unavailable")
