package us

import "testing"

func TestDailyBarGathererName(t *testing.T) {
	g := NewDailyBarGatherer("key", "secret", "https://data.alpaca.markets", nil, 5000, 10, "2016-01-01", "", "https://api.alpaca.markets")
	if got := g.Name(); got != "us-daily" {
		t.Errorf("DailyBarGatherer.Name() = %q, want %q", got, "us-daily")
	}
}

func TestTradeGathererName(t *testing.T) {
	g := NewTradeGatherer("key", "secret", "https://data.alpaca.markets", nil, 100, 200, "2020-01-01")
	if got := g.Name(); got != "us-trade" {
		t.Errorf("TradeGatherer.Name() = %q, want %q", got, "us-trade")
	}
}

func TestStreamGathererName(t *testing.T) {
	g := NewStreamGatherer("key", "secret", "wss://stream.data.alpaca.markets", nil)
	if got := g.Name(); got != "us-stream" {
		t.Errorf("StreamGatherer.Name() = %q, want %q", got, "us-stream")
	}
}
