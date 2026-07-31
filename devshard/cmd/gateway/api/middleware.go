package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"devshard/cmd/gateway/filters"
)

const (
	// chatIngestLimit stops a body at the socket that filters would reject after buffering it.
	chatIngestLimit = filters.MaxBodyBytes
	// adminIngestLimit bounds every operator body. The largest of them is a settings patch.
	adminIngestLimit = 64 << 10
)

// credentials is one request's resolved identity. It is computed only where an answer is used, so a
// route that needs neither never compares a key at all.
type credentials struct {
	admin  bool
	apiKey bool
}

// keyGate compares a presented bearer against configured keys. Both sides are hashed first, so the
// comparison reveals neither the configured key's length nor any prefix of its bytes.
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

// requireAdmin gates the operator routes. The key comparison lives here rather than in a blanket
// middleware so an unauthenticated request to a cheap public route never reaches it.
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

// disabled is the operator kill switch. /metrics and the operator routes stay reachable: monitoring
// and remediation are what a turned-off gateway still needs.
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
// instead of draining it.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	return io.ReadAll(r.Body)
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
	for _, method := range methods {
		if r.Method == method {
			return true
		}
	}
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}
