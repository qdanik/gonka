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

	// bodyReadTimeout bounds how long a caller may take to deliver a body it has already announced. It
	// is armed per request rather than as http.Server.ReadTimeout, which stays armed for the whole
	// exchange: there it would expire mid-response and cancel the request a long stream is still
	// writing for.
	bodyReadTimeout = 30 * time.Second
)

// credentials is one request's resolved identity. It is computed only where an answer is used, so a
// route that needs neither never compares a key at all.
type credentials struct {
	admin  bool
	apiKey bool
}

// keyGate compares a presented bearer against configured keys, hashing both sides first. See
// gateway-request-lifecycle.md, "3. Authorisation and routability".
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

// authenticate reports whether authorization carries one of the configured keys. Every configured
// key is compared, so the time taken does not depend on which one matched.
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

// requireAdmin gates the operator routes, and is where the admin key comparison lives rather than in a
// blanket middleware. See gateway-request-lifecycle.md, "3. Authorisation and routability".
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

// resolveCredentials answers only when a credential was presented, so an unauthenticated request
// never reaches a key comparison at all.
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

// disabled is the operator kill switch; /metrics and the operator routes stay reachable. See
// gateway-operations.md, "The kill switch".
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

// readBody bounds ingest with MaxBytesReader rather than LimitReader: it returns a typed error the
// status mapper recognises, and it marks the connection so the server stops reading a hostile body
// instead of draining it. The deadline is cleared again before the response begins, so only the read
// is bounded. A writer that neither carries a deadline nor unwraps to one reports ErrNotSupported and
// keeps the size bound alone.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	deadlines := http.NewResponseController(w)
	_ = deadlines.SetReadDeadline(time.Now().Add(bodyReadTimeout))
	r.Body = http.MaxBytesReader(baseWriter(w), r.Body, limit)
	body, err := io.ReadAll(r.Body)
	_ = deadlines.SetReadDeadline(time.Time{})
	return body, err
}

// baseWriter walks the Unwrap chain to the writer the server owns. MaxBytesReader marks a connection
// by type-asserting its writer, so handed a wrapper it cannot see through it leaves the server
// draining a hostile body it has already refused.
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
