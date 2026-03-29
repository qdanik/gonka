package telemetry

import "testing"

func TestParseHeaders(t *testing.T) {
	headers := parseHeaders("authorization=Bearer token, x-tenant = alpha ,broken,no-value=,=missing-key")

	if len(headers) != 2 {
		t.Fatalf("expected 2 parsed headers, got %d", len(headers))
	}

	if headers["authorization"] != "Bearer token" {
		t.Fatalf("unexpected authorization header: %q", headers["authorization"])
	}

	if headers["x-tenant"] != "alpha" {
		t.Fatalf("unexpected x-tenant header: %q", headers["x-tenant"])
	}
}

func TestParseHeadersEmpty(t *testing.T) {
	if headers := parseHeaders(""); headers != nil {
		t.Fatalf("expected nil headers, got %#v", headers)
	}
}
