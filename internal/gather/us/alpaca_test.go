package us

import "testing"

func TestDailyBarGathererName(t *testing.T) {
	g := NewDailyBarGatherer("key", "secret", "https://data.alpaca.markets",
		nil, nil, 5000, 10, 16,
		"2016-01-01", "", "https://api.alpaca.markets", "")
	if got := g.Name(); got != "us-alpaca-data" {
		t.Errorf("DailyBarGatherer.Name() = %q, want %q", got, "us-alpaca-data")
	}
}

func TestBuildTradeBatches(t *testing.T) {
	symbols := []string{"SPY", "AAPL", "MSFT", "GOOGL", "TSLA", "AMZN"}
	counts := map[string]int64{
		"SPY":   500_000, // alone in batch (hits 500K target)
		"AAPL":  80_000,  // grouped with remaining (240K total < 500K)
		"MSFT":  30_000,
		"GOOGL": 60_000,
		"TSLA":  50_000,
		"AMZN":  20_000,
	}

	batches := buildTradeBatches(symbols, counts)

	// SPY (500K) → own batch (adding AAPL would exceed 500K)
	// AAPL+MSFT+GOOGL+TSLA+AMZN (240K) → single batch
	if len(batches) != 2 {
		t.Fatalf("buildTradeBatches returned %d batches, want 2; batches: %v", len(batches), batches)
	}

	if len(batches[0]) != 1 || batches[0][0] != "SPY" {
		t.Errorf("batch[0] = %v, want [SPY]", batches[0])
	}
	if len(batches[1]) != 5 {
		t.Errorf("batch[1] has %d symbols, want 5: %v", len(batches[1]), batches[1])
	}
}

func TestBuildTradeBatchesAllSmall(t *testing.T) {
	symbols := []string{"A", "B", "C", "D", "E"}
	counts := map[string]int64{
		"A": 10_000,
		"B": 10_000,
		"C": 10_000,
		"D": 10_000,
		"E": 10_000,
	}

	batches := buildTradeBatches(symbols, counts)

	// All fit in one batch (50K < 100K target)
	if len(batches) != 1 {
		t.Fatalf("buildTradeBatches returned %d batches, want 1", len(batches))
	}
	if len(batches[0]) != 5 {
		t.Errorf("batch[0] has %d symbols, want 5", len(batches[0]))
	}
}

func TestBuildTradeBatchesMinOneSymbol(t *testing.T) {
	symbols := []string{"MEGA"}
	counts := map[string]int64{"MEGA": 1_000_000}

	batches := buildTradeBatches(symbols, counts)

	if len(batches) != 1 {
		t.Fatalf("buildTradeBatches returned %d batches, want 1", len(batches))
	}
	if batches[0][0] != "MEGA" {
		t.Errorf("batch[0][0] = %q, want MEGA", batches[0][0])
	}
}

func TestStreamGathererName(t *testing.T) {
	g := NewStreamGatherer("key", "secret", "https://api.alpaca.markets", "/tmp", "", "")
	if got := g.Name(); got != "us-stream" {
		t.Errorf("StreamGatherer.Name() = %q, want %q", got, "us-stream")
	}
}
