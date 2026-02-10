package us

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jupitor/internal/domain"
)

func TestUniverseWriterAddBars(t *testing.T) {
	dir := t.TempDir()
	uw := newUniverseWriter(dir)

	bars := []domain.Bar{
		{Symbol: "AAPL", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
		{Symbol: "MSFT", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
		{Symbol: "AAPL", Timestamp: time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC)},
		{Symbol: "GOOGL", Timestamp: time.Date(2025, 1, 7, 0, 0, 0, 0, time.UTC)},
	}

	uw.AddBars(bars)
	if err := uw.Flush(); err != nil {
		t.Fatal(err)
	}

	// Check 2025-01-06.txt
	data, err := os.ReadFile(filepath.Join(dir, "2025-01-06.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Errorf("2025-01-06.txt has %d lines, want 2", len(lines))
	}

	// Check 2025-01-07.txt
	data, err = os.ReadFile(filepath.Join(dir, "2025-01-07.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines = strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Errorf("2025-01-07.txt has %d lines, want 2", len(lines))
	}
}

func TestUniverseWriterFinalize(t *testing.T) {
	dir := t.TempDir()
	uw := newUniverseWriter(dir)

	// Write bars in two batches with duplicates.
	bars1 := []domain.Bar{
		{Symbol: "MSFT", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
		{Symbol: "AAPL", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
		{Symbol: "GOOGL", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
	}
	uw.AddBars(bars1)
	if err := uw.Flush(); err != nil {
		t.Fatal(err)
	}

	// Second batch with some duplicates.
	bars2 := []domain.Bar{
		{Symbol: "AAPL", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
		{Symbol: "TSLA", Timestamp: time.Date(2025, 1, 6, 0, 0, 0, 0, time.UTC)},
	}
	uw.AddBars(bars2)
	if err := uw.Flush(); err != nil {
		t.Fatal(err)
	}

	// Before finalize: file may have duplicates.
	if err := uw.Finalize(); err != nil {
		t.Fatal(err)
	}

	// After finalize: sorted, deduped.
	data, err := os.ReadFile(filepath.Join(dir, "2025-01-06.txt"))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")

	want := []string{"AAPL", "GOOGL", "MSFT", "TSLA"}
	if len(lines) != len(want) {
		t.Fatalf("finalized file has %d lines, want %d: %v", len(lines), len(want), lines)
	}
	for i, line := range lines {
		if line != want[i] {
			t.Errorf("line %d = %q, want %q", i, line, want[i])
		}
	}
}
