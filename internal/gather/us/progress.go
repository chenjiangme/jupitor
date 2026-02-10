package us

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// progressTracker manages the .tried-empty and .last-completed files for
// crash recovery and idempotency.
type progressTracker struct {
	mu         sync.Mutex
	triedEmpty map[string]struct{}
	writer     *bufio.Writer
	file       *os.File
	dailyDir   string // <DataDir>/us/daily
}

// newProgressTracker creates a tracker rooted at the given daily directory
// and loads any existing .tried-empty entries.
func newProgressTracker(dailyDir string) (*progressTracker, error) {
	if err := os.MkdirAll(dailyDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating daily dir: %w", err)
	}

	pt := &progressTracker{
		triedEmpty: make(map[string]struct{}),
		dailyDir:   dailyDir,
	}

	// Load existing .tried-empty if present.
	path := filepath.Join(dailyDir, ".tried-empty")
	data, err := os.ReadFile(path)
	if err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			sym := strings.TrimSpace(line)
			if sym != "" {
				pt.triedEmpty[sym] = struct{}{}
			}
		}
	}

	// Open for appending.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening .tried-empty: %w", err)
	}
	pt.file = f
	pt.writer = bufio.NewWriter(f)

	return pt, nil
}

// IsTriedEmpty returns true if the symbol was already tried and returned no data.
func (p *progressTracker) IsTriedEmpty(symbol string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.triedEmpty[symbol]
	return ok
}

// MarkEmpty records a batch of symbols as tried-empty.
func (p *progressTracker) MarkEmpty(symbols []string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, sym := range symbols {
		if _, ok := p.triedEmpty[sym]; ok {
			continue
		}
		p.triedEmpty[sym] = struct{}{}
		if _, err := p.writer.WriteString(sym + "\n"); err != nil {
			return fmt.Errorf("writing to .tried-empty: %w", err)
		}
	}
	return p.writer.Flush()
}

// MarkCompleted writes the given date to .last-completed.
func (p *progressTracker) MarkCompleted(date string) error {
	path := filepath.Join(p.dailyDir, ".last-completed")
	return os.WriteFile(path, []byte(date), 0o644)
}

// IsCompleted returns true if .last-completed matches the given date.
func (p *progressTracker) IsCompleted(date string) bool {
	path := filepath.Join(p.dailyDir, ".last-completed")
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(data)) == date
}

// LastCompleted returns the date string from .last-completed, or empty string.
func (p *progressTracker) LastCompleted() string {
	path := filepath.Join(p.dailyDir, ".last-completed")
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// Reset deletes the .tried-empty file and clears the in-memory set.
func (p *progressTracker) Reset() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file != nil {
		p.file.Close()
	}

	p.triedEmpty = make(map[string]struct{})

	path := filepath.Join(p.dailyDir, ".tried-empty")
	os.Remove(path)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("reopening .tried-empty: %w", err)
	}
	p.file = f
	p.writer = bufio.NewWriter(f)
	return nil
}

// Close flushes and closes the .tried-empty file.
func (p *progressTracker) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.writer != nil {
		p.writer.Flush()
	}
	if p.file != nil {
		return p.file.Close()
	}
	return nil
}
