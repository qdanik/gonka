package observability

import (
	"context"
	"net/http"
)

var Default = NewService()
var Request = Default.Request()

type Service struct {
	request *RequestService
}

type RequestService struct {
	trace RequestTraceService
}

func NewService() *Service {
	return &Service{
		request: &RequestService{
			trace: NewRequestTraceService(),
		},
	}
}

func (s *Service) Request() *RequestService {
	if s == nil {
		return NewService().Request()
	}
	return s.request
}

func (s *RequestService) Trace() RequestTraceService {
	if s == nil {
		return NewRequestTraceService()
	}
	return s.trace
}

func (s *RequestService) ExtractRequestContext(ctx context.Context, headers http.Header) context.Context {
	return s.Trace().ExtractRequestContext(ctx, headers)
}

func (s *RequestService) InjectRequestContext(ctx context.Context, headers http.Header) {
	s.Trace().InjectRequestContext(ctx, headers)
}

func (s *RequestService) StartRequest(ctx context.Context, method string, route string) (context.Context, *Operation) {
	return s.Trace().StartRequest(ctx, method, route)
}

func (s *RequestService) SetEscrowID(op *Operation, escrowID string) {
	s.Trace().SetEscrowID(op, escrowID)
}

func (s *RequestService) SetSessionID(op *Operation, sessionID string) {
	s.Trace().SetSessionID(op, sessionID)
}

func (s *RequestService) SetSender(op *Operation, sender string) {
	s.Trace().SetSender(op, sender)
}

func (s *RequestService) SetHTTPStatus(op *Operation, statusCode int) {
	s.Trace().SetHTTPStatus(op, statusCode)
}