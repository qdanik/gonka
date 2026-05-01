package proxy

import (
	"context"
	"log/slog"
	"net/http"
)

type requestLogging struct {
	request   *http.Request
	version   string
	target    string
	status    int
}

func newRequestLogging(r *http.Request, version string, target string) *requestLogging {
	return &requestLogging{request: r, version: version, target: target}
}

func startRequestLogging(r *http.Request, version string, target string) (context.Context, *requestLogging) {
	requestContext := r.Context()
	requestLogging := newRequestLogging(r, version, target)
	requestLogging.logReceived()
	return requestContext, requestLogging
}

func (logging *requestLogging) logReceived() {
	slog.Info(
		"versiond.proxy.request",
		"service", "versiond",
		"method", logging.request.Method,
		"path", logging.request.URL.Path,
		"version", logging.version,
		"target", logging.target,
	)
}

func (logging *requestLogging) setHTTPStatus(statusCode int) {
	if logging == nil || statusCode == 0 {
		return
	}
	logging.status = statusCode
}

func (logging *requestLogging) finish(requestErr *error) {
	statusCode := logging.status
	if statusCode == 0 {
		if requestErr != nil && *requestErr != nil {
			statusCode = http.StatusInternalServerError
		} else {
			statusCode = http.StatusOK
		}
		logging.setHTTPStatus(statusCode)
	}

	attrs := []any{
		"service", "versiond",
		"method", logging.request.Method,
		"path", logging.request.URL.Path,
		"version", logging.version,
		"target", logging.target,
		"status_code", statusCode,
	}
	if requestErr != nil && *requestErr != nil {
		attrs = append(attrs, "error", *requestErr)
		slog.Error("versiond.proxy.request_failed", attrs...)
		return
	}
	slog.Info("versiond.proxy.request_completed", attrs...)
}