package proxy

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"

	"versioned/internal/observability"
)

// Handler returns an http.Handler that routes requests by version prefix.
// First path segment is the version name, stripped before forwarding.
// Example: /v0.2.11/chat/completions -> localhost:9001/chat/completions
func Handler(routes *atomic.Value) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) == 0 || parts[0] == "" {
			_, requestLogging := startRequestLogging(r, "", "")
			proxyErr := fmt.Errorf("version prefix required")
			defer requestLogging.finish(&proxyErr)
			requestLogging.setHTTPStatus(http.StatusBadRequest)
			http.Error(w, "version prefix required", http.StatusBadRequest)
			return
		}

		version := parts[0]
		rest := "/"
		if len(parts) == 2 {
			rest = "/" + parts[1]
		}

		routeMap := routes.Load().(map[string]string)
		target, ok := routeMap[version]
		if !ok {
			_, requestLogging := startRequestLogging(r, version, "")
			proxyErr := fmt.Errorf("version %q not found", version)
			defer requestLogging.finish(&proxyErr)
			requestLogging.setHTTPStatus(http.StatusNotFound)
			http.Error(w, fmt.Sprintf("version %q not found", version), http.StatusNotFound)
			return
		}

		requestContext, requestLogging := startRequestLogging(r, version, target)
		var proxyErr error
		defer requestLogging.finish(&proxyErr)

		targetURL, err := url.Parse("http://" + target)
		if err != nil {
			proxyErr = err
			requestLogging.setHTTPStatus(http.StatusInternalServerError)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		recorder := &statusRecorder{ResponseWriter: w}

		p := &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetXForwarded()
				pr.Out = pr.Out.WithContext(requestContext)
				pr.Out.URL.Scheme = targetURL.Scheme
				pr.Out.URL.Host = targetURL.Host
				pr.Out.Host = targetURL.Host
				pr.Out.URL.Path = rest
				pr.Out.URL.RawPath = ""
				observability.Proxy.InjectRequestContext(requestContext, pr.Out.Header)
			},
			ModifyResponse: func(resp *http.Response) error {
				requestLogging.setHTTPStatus(resp.StatusCode)
				return nil
			},
			ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
				proxyErr = err
				requestLogging.setHTTPStatus(http.StatusBadGateway)
				http.Error(rw, "bad gateway", http.StatusBadGateway)
			},
			FlushInterval: -1, // flush immediately for SSE
		}

		p.ServeHTTP(recorder, r.WithContext(requestContext))
		statusCode := recorder.statusCode()
		requestLogging.setHTTPStatus(statusCode)
		if statusCode >= http.StatusInternalServerError {
			proxyErr = fmt.Errorf("proxy returned status %d", statusCode)
		}
	})
}
