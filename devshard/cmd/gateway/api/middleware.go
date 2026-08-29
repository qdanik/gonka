package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	json "github.com/goccy/go-json"

	"devshard/cmd/gateway/filters"
)

const (
	// chatIngestLimit stops a body at the socket that filters would reject after buffering it.
	chatIngestLimit = filters.MaxBodyBytes

	// adminIngestLimit bounds every operator body. The largest of them is a settings patch.
	adminIngestLimit = 64 << 10

	// bodyReadTimeout is armed per request, not as http.Server.ReadTimeout, which would expire mid-response.
	bodyReadTimeout = 30 * time.Second
)

// credentials is one request's resolved identity, computed only where an answer is used.
type credentials struct {
	admin  bool
	apiKey bool
}

// keyGate compares a bearer against configured keys. See README.md, "Authentication and the kill switch".
type keyGate struct {
	digests    [][sha256.Size]byte
	configured bool
}

func newKeyGate(keys ...string) keyGate {
	gate := keyGate{}
	for _, key := range keys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		gate.digests = append(gate.digests, sha256.Sum256([]byte(trimmed)))
		gate.configured = true
	}
	return gate
}

// authenticate compares every configured key, so the time taken does not depend on which one matched.
func (g keyGate) authenticate(authorization string) bool {
	presented, hasBearer := strings.CutPrefix(authorization, "Bearer ")
	if !g.configured || !hasBearer {
		return false
	}
	digest := sha256.Sum256([]byte(strings.TrimSpace(presented)))
	matched := 0
	for _, configured := range g.digests {
		matched |= subtle.ConstantTimeCompare(digest[:], configured[:])
	}
	return matched == 1
}

// requireAdmin gates the operator routes. See README.md, "Authentication and the kill switch".
func (s *Server) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.adminEnabled() {
			writeError(w, http.StatusNotFound, "not found")
			return
		}
		if !s.authenticateAdmin(r) {
			writeJSON(w, http.StatusUnauthorized, errorEnvelope{Error: errorDetail{
				Message: "Invalid admin API key.",
				Type:    "invalid_request_error",
				Code:    "invalid_api_key",
			}})
			return
		}
		next(w, r)
	}
}

func (s *Server) adminEnabled() bool {
	return s.config.Load().Server.AdminEnabled()
}

func (s *Server) authenticateAdmin(r *http.Request) bool {
	return s.resolveCredentials(r).admin
}

// resolveCredentials answers only when a credential was presented, so an unauthenticated request compares nothing.
func (s *Server) resolveCredentials(r *http.Request) credentials {
	authorization := r.Header.Get("Authorization")
	if authorization == "" {
		return credentials{}
	}
	return s.verify(authorization)
}

func (s *Server) compareKeys(authorization string) credentials {
	server := s.config.Load().Server
	adminKeys := keyGate{}
	if server.AdminEnabled() {
		adminKeys = newKeyGate(server.AdminAPIKey)
	}
	return credentials{
		admin:  adminKeys.authenticate(authorization),
		apiKey: newKeyGate(server.APIKeys...).authenticate(authorization),
	}
}

// disabled is the operator kill switch; alwaysOn routes stay reachable. See operations.md, "The kill switch".
func (s *Server) disabled(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		modes := s.config.Load().Modes
		if !modes.Disabled {
			next(w, r)
			return
		}
		if redirect := strings.TrimSpace(modes.DisabledRedirectURL); redirect != "" {
			w.Header().Set("Location", redirect)
			writeJSON(w, http.StatusPermanentRedirect, map[string]any{
				"status":  http.StatusPermanentRedirect,
				"message": modes.DisabledMessage,
				"new_url": redirect,
			})
			return
		}
		message := strings.TrimSpace(modes.DisabledMessage)
		if message == "" {
			message = "gateway is disabled"
		}
		writeError(w, http.StatusServiceUnavailable, message)
	}
}

// readBody bounds ingest with MaxBytesReader, not LimitReader. See README.md, "Reading a body".
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	deadlines := http.NewResponseController(w)
	_ = deadlines.SetReadDeadline(time.Now().Add(bodyReadTimeout))
	r.Body = http.MaxBytesReader(baseWriter(w), r.Body, limit)
	body, err := io.ReadAll(r.Body)
	_ = deadlines.SetReadDeadline(time.Time{})
	return body, err
}

// baseWriter walks the Unwrap chain: MaxBytesReader marks a connection by type-asserting the writer it is handed.
func baseWriter(w http.ResponseWriter) http.ResponseWriter {
	for {
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		w = unwrapper.Unwrap()
	}
}

func decodeAdminBody(w http.ResponseWriter, r *http.Request, target any) error {
	body, err := readBody(w, r, adminIngestLimit)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	return json.Unmarshal(body, target)
}

func allowMethods(w http.ResponseWriter, r *http.Request, methods ...string) bool {
	if slices.Contains(methods, r.Method) {
		return true
	}
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}
