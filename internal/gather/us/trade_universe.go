package us

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// GenerateTradeUniverse scans all universe dates and writes trade-universe CSVs
// for dates where all symbols have trade files. Used by the standalone command.
// Returns (true, nil) if any CSVs were written.
func GenerateTradeUniverse(ctx context.Context, dataDir string, ref *ReferenceData, log *slog.Logger) (bool, error) {
	universeDir := filepath.Join(dataDir, "us", "universe")
	dates, err := ListUniverseDates(universeDir)
	if err != nil {
		return false, fmt.Errorf("listing universe dates: %w", err)
	}

	tradesDir := filepath.Join(dataDir, "us", "trades")

	wrote := 0
	for _, date := range dates {
		if ctx.Err() != nil {
			return wrote > 0, ctx.Err()
		}

		outPath := tradeUniversePath(dataDir, date)
		if _, err := os.Stat(outPath); err == nil {
			continue
		}

		symbols, err := ReadUniverseFile(filepath.Join(universeDir, date+".txt"))
		if err != nil {
			log.Error("reading universe file", "date", date, "error", err)
			continue
		}

		allComplete := true
		for _, sym := range symbols {
			tradePath := filepath.Join(tradesDir, strings.ToUpper(sym), date+".parquet")
			if _, err := os.Stat(tradePath); os.IsNotExist(err) {
				allComplete = false
				break
			}
		}

		if !allComplete {
			continue
		}

		if err := generateTradeUniverseForDate(dataDir, date, symbols, ref, log); err != nil {
			continue
		}
		wrote++
	}

	return wrote > 0, nil
}

// generateTradeUniverseForDate writes a single trade-universe CSV for the given
// date. Called from the daemon after a trade day completes, and from the
// standalone batch command.
func generateTradeUniverseForDate(dataDir, date string, symbols []string, ref *ReferenceData, log *slog.Logger) error {
	outPath := tradeUniversePath(dataDir, date)
	spxDir := filepath.Join(dataDir, "us", "index", "spx")
	ndxDir := filepath.Join(dataDir, "us", "index", "ndx")

	if err := writeTradeUniverseCSV(outPath, symbols, ref, spxDir, ndxDir, date); err != nil {
		log.Error("writing trade universe CSV", "date", date, "error", err)
		return err
	}

	log.Info("trade universe CSV written", "date", date, "symbols", len(symbols))
	return nil
}

// tradeUniversePath returns the path for a trade-universe CSV.
func tradeUniversePath(dataDir, date string) string {
	return filepath.Join(dataDir, "us", "trade-universe", date+".csv")
}

// writeTradeUniverseCSV generates a trade-universe CSV for a single date.
func writeTradeUniverseCSV(path string, symbols []string, ref *ReferenceData, spxDir, ndxDir, date string) error {
	// Load SPX/NDX members for this date.
	spxMembers := readIndexSet(filepath.Join(spxDir, date+".txt"))
	ndxMembers := readIndexSet(filepath.Join(ndxDir, date+".txt"))

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating trade-universe dir: %w", err)
	}

	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating CSV %s: %w", path, err)
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	w.WriteString("symbol,type,spx,ndx\n")

	sorted := make([]string, len(symbols))
	copy(sorted, symbols)
	sort.Strings(sorted)

	for _, sym := range sorted {
		symType := "STOCK"
		if ref != nil {
			symType = ref.SymbolType(sym)
		}
		inSPX := "false"
		if spxMembers[sym] {
			inSPX = "true"
		}
		inNDX := "false"
		if ndxMembers[sym] {
			inNDX = "true"
		}
		fmt.Fprintf(w, "%s,%s,%s,%s\n", sym, symType, inSPX, inNDX)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("flushing CSV: %w", err)
	}
	return nil
}

// readIndexSet reads an index constituent file and returns a set of symbols.
// Returns an empty map if the file doesn't exist.
func readIndexSet(path string) map[string]bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return make(map[string]bool)
	}

	set := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		sym := strings.TrimSpace(line)
		if sym != "" {
			set[sym] = true
		}
	}
	return set
}
