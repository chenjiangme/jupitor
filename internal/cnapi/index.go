package cnapi

import (
	"bufio"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// IndexEntry holds the name and index membership for a constituent stock.
type IndexEntry struct {
	Name  string
	Index string // "csi300" or "csi500"
}

// LoadIndexConstituents reads csi300 and csi500 index files for the given date
// and returns a map of symbol → IndexEntry. CSI 300 takes priority if a symbol
// appears in both indices.
func LoadIndexConstituents(dataDir, date string) (map[string]IndexEntry, error) {
	result := make(map[string]IndexEntry, 800)

	// Load csi500 first so csi300 overwrites on conflict.
	for _, idx := range []string{"csi500", "csi300"} {
		path := filepath.Join(dataDir, "cn", "index", idx, date+".txt")
		entries, err := readIndexFile(path, idx)
		if err != nil {
			return nil, err
		}
		for sym, entry := range entries {
			result[sym] = entry
		}
	}

	return result, nil
}

// readIndexFile reads a single index file. Each line is "symbol,name".
func readIndexFile(path, index string) (map[string]IndexEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	entries := make(map[string]IndexEntry)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, ",", 2)
		if len(parts) != 2 {
			continue
		}
		entries[parts[0]] = IndexEntry{
			Name:  parts[1],
			Index: index,
		}
	}
	return entries, scanner.Err()
}

// ListCNDates returns sorted dates where both csi300 and csi500 index files exist.
func ListCNDates(dataDir string) ([]string, error) {
	csi300Dates, err := listDatesInDir(filepath.Join(dataDir, "cn", "index", "csi300"))
	if err != nil {
		return nil, err
	}
	csi500Dates, err := listDatesInDir(filepath.Join(dataDir, "cn", "index", "csi500"))
	if err != nil {
		return nil, err
	}

	// Intersect.
	set := make(map[string]bool, len(csi300Dates))
	for _, d := range csi300Dates {
		set[d] = true
	}
	var dates []string
	for _, d := range csi500Dates {
		if set[d] {
			dates = append(dates, d)
		}
	}
	sort.Strings(dates)
	return dates, nil
}

// listDatesInDir reads *.txt files from a directory and extracts date stems.
func listDatesInDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dates []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, ".txt") {
			dates = append(dates, strings.TrimSuffix(name, ".txt"))
		}
	}
	return dates, nil
}
