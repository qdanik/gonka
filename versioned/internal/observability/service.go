package observability

import (
	"context"
	"net/http"
)

var Default = NewService()
var Proxy = Default.Proxy()

type Service struct {
	proxy *ProxyService
}

type ProxyService struct {
	trace ProxyTraceService
}

func NewService() *Service {
	return &Service{
		proxy: &ProxyService{
			trace: NewProxyTraceService(),
		},
	}
}

func (s *Service) Proxy() *ProxyService {
	if s == nil {
		return NewService().Proxy()
	}
	return s.proxy
}

func (s *ProxyService) Trace() ProxyTraceService {
	if s == nil {
		return NewProxyTraceService()
	}
	return s.trace
}

func (s *ProxyService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return s.Trace().ExtractRequestContext(ctx, headers)
}

func (s *ProxyService) InjectRequestContext(ctx context.Context, headers http.Header) {
	s.Trace().InjectRequestContext(ctx, headers)
}

func (s *ProxyService) StartRequest(ctx context.Context, method string, path string, version string, target string) (context.Context, *Operation) {
	return s.Trace().StartRequest(ctx, method, path, version, target)
}

func (s *ProxyService) SetHTTPStatus(op *Operation, statusCode int) {
	s.Trace().SetHTTPStatus(op, statusCode)
}