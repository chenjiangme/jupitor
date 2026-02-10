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

func TestReadUniverseFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "2025-01-06.txt")
	if err := os.WriteFile(path, []byte("AAPL\nGOOGL\nMSFT\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	symbols, err := ReadUniverseFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"AAPL", "GOOGL", "MSFT"}
	if len(symbols) != len(want) {
		t.Fatalf("ReadUniverseFile returned %d symbols, want %d", len(symbols), len(want))
	}
	for i, s := range symbols {
		if s != want[i] {
			t.Errorf("symbol[%d] = %q, want %q", i, s, want[i])
		}
	}
}

func TestReadUniverseFileEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(path, []byte("\n\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	symbols, err := ReadUniverseFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(symbols) != 0 {
		t.Errorf("expected 0 symbols, got %d", len(symbols))
	}
}

func TestListUniverseDates(t *testing.T) {
	dir := t.TempDir()

	// Create universe files.
	for _, date := range []string{"2025-01-03", "2025-01-06", "2025-01-02"} {
		path := filepath.Join(dir, date+".txt")
		if err := os.WriteFile(path, []byte("AAPL\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	dates, err := ListUniverseDates(dir)
	if err != nil {
		t.Fatal(err)
	}

	want := []string{"2025-01-06", "2025-01-03", "2025-01-02"}
	if len(dates) != len(want) {
		t.Fatalf("ListUniverseDates returned %d dates, want %d", len(dates), len(want))
	}
	for i, d := range dates {
		if d != want[i] {
			t.Errorf("date[%d] = %q, want %q", i, d, want[i])
		}
	}
}

func TestListUniverseDatesEmpty(t *testing.T) {
	dir := t.TempDir()

	dates, err := ListUniverseDates(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(dates) != 0 {
		t.Errorf("expected 0 dates, got %d", len(dates))
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
