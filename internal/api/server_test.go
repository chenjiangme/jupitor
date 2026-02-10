package api

import (
	"testing"

	"jupitor/internal/config"
)

func TestNewServer(t *testing.T) {
	cfg := &config.Config{}
	s := NewServer(cfg)
	if s == nil {
		t.Fatal("NewServer returned nil")
	}
}
