package us

import "testing"

func TestDailyBarGathererName(t *testing.T) {
	g := NewDailyBarGatherer("key", "secret", "https://data.alpaca.markets",
		nil, nil, 5000, 10, 50, 16,
		"2016-01-01", "", "https://api.alpaca.markets")
	if got := g.Name(); got != "us-alpaca-data" {
		t.Errorf("DailyBarGatherer.Name() = %q, want %q", got, "us-alpaca-data")
	}
}

func TestStreamGathererName(t *testing.T) {
	g := NewStreamGatherer("key", "secret", "wss://stream.data.alpaca.markets", nil)
	if got := g.Name(); got != "us-stream" {
		t.Errorf("StreamGatherer.Name() = %q, want %q", got, "us-stream")
	}
}
