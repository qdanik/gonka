package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// servePrefixProxy stands in for versiond's route: the gateway calls /devshard/<version>/..., a bare devshardd serves the same routes without that prefix.
func (s *stack) servePrefixProxy(listenPort, backendPort int) error {
	target := &url.URL{Scheme: "http", Host: "127.0.0.1:" + strconv.Itoa(backendPort)}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.FlushInterval = -1
	director := proxy.Director
	proxy.Director = func(r *http.Request) {
		director(r)
		r.URL.Path = stripVersionPrefix(r.URL.Path)
		r.URL.RawPath = ""
	}
	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(listenPort))
	if err != nil {
		return err
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = server.Serve(listener) }()
	s.closers = append(s.closers, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		_ = server.Close()
	})
	return nil
}

func stripVersionPrefix(path string) string {
	rest, ok := strings.CutPrefix(path, "/devshard/")
	if !ok {
		return path
	}
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		return rest[i:]
	}
	return "/"
}
