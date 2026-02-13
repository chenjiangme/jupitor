package dashboard

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// LoadTierMap reads the latest trade-universe CSV and returns symbol→tier
// for ex-index stocks (non-empty tier field).
func LoadTierMap(dataDir string) (map[string]string, error) {
	dir := filepath.Join(dataDir, "us", "trade-universe")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading trade-universe dir: %w", err)
	}

	// Find latest CSV by name (lexicographic = chronological for YYYY-MM-DD).
	var latest string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".csv") {
			continue
		}
		if e.Name() > latest {
			latest = e.Name()
		}
	}
	if latest == "" {
		return nil, fmt.Errorf("no trade-universe CSV files found in %s", dir)
	}

	path := filepath.Join(dir, latest)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	tierMap := make(map[string]string)
	scanner := bufio.NewScanner(f)
	first := true
	for scanner.Scan() {
		if first {
			first = false
			continue // skip header
		}
		fields := strings.Split(scanner.Text(), ",")
		if len(fields) < 5 {
			continue
		}
		tier := strings.TrimSpace(fields[4])
		if tier != "" {
			tierMap[fields[0]] = tier
		}
	}

	slog.Info("loaded trade-universe CSV", "file", latest, "exIndexSymbols", len(tierMap))
	return tierMap, scanner.Err()
}
