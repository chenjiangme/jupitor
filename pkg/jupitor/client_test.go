package jupitor

import (
	"testing"
)

func TestNewClient(t *testing.T) {
	baseURL := "http://localhost:8080"
	c := NewClient(baseURL)

	if c == nil {
		t.Fatal("expected non-nil client")
	}

	if c.baseURL != baseURL {
		t.Errorf("expected baseURL %q, got %q", baseURL, c.baseURL)
	}

	if c.httpClient == nil {
		t.Fatal("expected non-nil httpClient")
	}
}
