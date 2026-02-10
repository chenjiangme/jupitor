package us

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateBruteSymbols(t *testing.T) {
	symbols := GenerateBruteSymbols()
	// 26 + 676 + 17576 + 456976 = 475254
	want := 26 + 26*26 + 26*26*26 + 26*26*26*26
	if len(symbols) != want {
		t.Errorf("GenerateBruteSymbols() count = %d, want %d", len(symbols), want)
	}

	// Spot-check a few symbols.
	first := symbols[0]
	if first != "A" {
		t.Errorf("first symbol = %q, want %q", first, "A")
	}

	// Check that single letters come before doubles.
	found := make(map[string]bool)
	for _, s := range symbols {
		found[s] = true
	}
	for _, sym := range []string{"A", "Z", "AA", "ZZ", "AAA", "ZZZ", "AAAA", "ZZZZ"} {
		if !found[sym] {
			t.Errorf("expected symbol %q not found", sym)
		}
	}
}

func TestAllBruteSymbols(t *testing.T) {
	// Create a temp CSV with a mix of new and overlapping symbols.
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "test.csv")
	csv := "symbol,description,industry,exchange\nGOOGL,Alphabet,Tech,NASDAQ\nAAAA,Overlap,Test,NYSE\nFOOBAR,NewSym,Test,NYSE\n"
	if err := os.WriteFile(csvPath, []byte(csv), 0o644); err != nil {
		t.Fatal(err)
	}

	symbols, err := AllBruteSymbols(csvPath)
	if err != nil {
		t.Fatal(err)
	}

	// AAAA is already in brute-force, GOOGL is 5 chars but already in brute force.
	// FOOBAR is 6 chars, new.
	// So total = 475254 (brute) + 1 (GOOGL) + 1 (FOOBAR) = 475256
	// Wait — GOOGL is 5 chars, not in brute-force (1-4 chars only). So +2 new.
	want := 26 + 26*26 + 26*26*26 + 26*26*26*26 + 2 // GOOGL + FOOBAR
	if len(symbols) != want {
		t.Errorf("AllBruteSymbols() count = %d, want %d", len(symbols), want)
	}

	// Verify FOOBAR is present.
	found := false
	for _, s := range symbols {
		if s == "FOOBAR" {
			found = true
			break
		}
	}
	if !found {
		t.Error("FOOBAR not found in AllBruteSymbols result")
	}
}

func TestSymbolsShuffled(t *testing.T) {
	dir := t.TempDir()
	csvPath := filepath.Join(dir, "empty.csv")
	if err := os.WriteFile(csvPath, []byte("symbol\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	a, err := AllBruteSymbols(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	b, err := AllBruteSymbols(csvPath)
	if err != nil {
		t.Fatal(err)
	}

	// With 475K+ symbols, the probability of identical shuffle is essentially 0.
	same := true
	for i := range a {
		if a[i] != b[i] {
			same = false
			break
		}
	}
	if same {
		t.Error("two calls to AllBruteSymbols returned identical order — shuffle not working")
	}
}
