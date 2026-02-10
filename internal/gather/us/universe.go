package us

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"jupitor/internal/domain"
)

// universeWriter manages daily universe files (universe/YYYY-MM-DD.txt),
// buffering symbol writes per date and flushing them in batches.
type universeWriter struct {
	mu      sync.Mutex
	dataDir string              // <DataDir>/us/universe
	buffers map[string][]string // date → symbols (batch buffer)
	touched map[string]bool     // files written this run (for final sort+dedup)
}

// newUniverseWriter creates a universe writer rooted at the given directory.
func newUniverseWriter(dataDir string) *universeWriter {
	return &universeWriter{
		dataDir: dataDir,
		buffers: make(map[string][]string),
		touched: make(map[string]bool),
	}
}

// AddBars extracts unique (date, symbol) pairs from bars and buffers them.
func (u *universeWriter) AddBars(bars []domain.Bar) {
	u.mu.Lock()
	defer u.mu.Unlock()

	for _, b := range bars {
		date := b.Timestamp.Format("2006-01-02")
		u.buffers[date] = append(u.buffers[date], b.Symbol)
	}
}

// Flush appends buffered symbols to their respective date files and clears
// the buffer. Thread-safe.
func (u *universeWriter) Flush() error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if err := os.MkdirAll(u.dataDir, 0o755); err != nil {
		return fmt.Errorf("creating universe dir: %w", err)
	}

	for date, symbols := range u.buffers {
		path := filepath.Join(u.dataDir, date+".txt")
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("opening universe file %s: %w", path, err)
		}

		w := bufio.NewWriter(f)
		for _, sym := range symbols {
			w.WriteString(sym + "\n")
		}
		w.Flush()
		f.Close()

		u.touched[date] = true
	}

	u.buffers = make(map[string][]string)
	return nil
}

// Finalize sorts and deduplicates each universe file that was touched during
// this run.
func (u *universeWriter) Finalize() error {
	u.mu.Lock()
	dates := make([]string, 0, len(u.touched))
	for date := range u.touched {
		dates = append(dates, date)
	}
	u.mu.Unlock()

	for _, date := range dates {
		path := filepath.Join(u.dataDir, date+".txt")
		if err := sortDedup(path); err != nil {
			return fmt.Errorf("finalizing universe file %s: %w", date, err)
		}
	}
	return nil
}

// ReadUniverseFile reads a universe file and returns the list of symbols.
func ReadUniverseFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	lines := strings.Split(string(data), "\n")
	var symbols []string
	for _, line := range lines {
		sym := strings.TrimSpace(line)
		if sym != "" {
			symbols = append(symbols, sym)
		}
	}
	return symbols, nil
}

// ListUniverseDates returns all dates that have universe files in the given
// directory, sorted in descending order (latest first).
func ListUniverseDates(dir string) ([]string, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.txt"))
	if err != nil {
		return nil, fmt.Errorf("globbing universe files: %w", err)
	}

	var dates []string
	for _, m := range matches {
		base := filepath.Base(m)
		date := strings.TrimSuffix(base, ".txt")
		// Validate date format.
		if len(date) == 10 && date[4] == '-' && date[7] == '-' {
			dates = append(dates, date)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dates)))
	return dates, nil
}

// sortDedup reads lines from the file, sorts them, removes duplicates, and
// writes them back.
func sortDedup(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		return nil
	}

	sort.Strings(lines)

	deduped := make([]string, 0, len(lines))
	prev := ""
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && line != prev {
			deduped = append(deduped, line)
			prev = line
		}
	}

	return os.WriteFile(path, []byte(strings.Join(deduped, "\n")+"\n"), 0o644)
}
